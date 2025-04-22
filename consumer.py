import asyncio
import json
import time
import logging
from dataclasses import dataclass
from typing import Optional, Set
from aio_pika import IncomingMessage
import networkx as nx
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.utils.graph import Dag
from src.models.tx import TxStore
from src.utils.data import hash_hex, ordered_sum
from src.utils.balance import get_balance
import sys
from multiprocessing import Process, Event
import signal
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TransactionConfig:
    """Конфигурация параметров транзакций"""
    DEV_ADDRESS: str = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy"

class TransactionProcessor:
    """Класс, отвечающий за обработку транзакций и управление графом"""
    def __init__(self, db: MongoDB, rabbitmq: RabbitMQ, redis_client: Redis, dag: Dag):
        self.db = db
        self.rabbitmq = rabbitmq
        self.redis = redis_client
        self.dag = dag
        self.config = TransactionConfig()

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает входящее сообщение из RabbitMQ"""
        try:
            tx = self.parse_message(message)
            await self.process_transaction(tx, message)
        except json.JSONDecodeError:
            logger.error("Failed to parse message: Invalid JSON format")
            await message.nack(requeue=False)
        except asyncio.TimeoutError:
            logger.error("Timeout during processing")
            await message.nack(requeue=True)
        except Exception as e:
            logger.error(f"Unexpected processing error: {e}", exc_info=True)
            await message.nack(requeue=False)

    def parse_message(self, message: IncomingMessage) -> TxStore:
        """Преобразует сырые данные сообщения в объект TxStore"""
        data = json.loads(message.body.decode("utf-8"))
        content = data["content"]
        content["sign"] = data["sign"]
        return TxStore(**content)

    async def process_transaction(self, tx: TxStore, message: IncomingMessage) -> None:
        tx_hash = hash_hex(ordered_sum(dict(tx)))
        #async with self.redis.client.lock(f"lock:{tx_hash}"):
        await self.dag.load(self.redis)
        """Основная логика обработки одной транзакции"""
        

        # Проверка лимита неподтверждённых транзакций
        

        # Проверка дубликатов
        if await self.check_duplicate(tx):
            return await message.nack(requeue=False)

        # Проверка баланса
        if not await self.verify_balance(tx):
            return await message.nack(requeue=False)

        # Проверка наличия родителей
        if not await self.verify_parents(tx):
            return await message.nack(requeue=False)

        # Проверка на циклы и добавление в граф
        if not await self.add_to_graph(tx_hash, tx):
            return await message.nack(requeue=False)

        # Подтверждение транзакции
        if await self.confirm_transaction(tx, tx_hash):
            await message.ack()
        else:
            await message.nack(requeue=False)

    async def check_duplicate(self, tx: TxStore) -> bool:
        """Проверяет, существует ли уже такая транзакция в базе"""
        exists = await self.db.find_one({"credit": tx.credit, "uniq": tx.uniq})
        if exists:
            logger.info(f"Duplicate detected: {tx.uniq}")
            return True
        return False

    async def verify_balance(self, tx: TxStore) -> bool:
        """Проверяет, достаточно ли средств для выполнения транзакции"""
        balance = await self.get_balance(tx.credit)
        if balance < tx.amount and tx.credit != self.config.DEV_ADDRESS:
            logger.error(f"Insufficient funds: {balance} < {tx.amount} for {tx.credit}")
            return False
        return True

    async def get_balance(self, credit: str) -> int:
        """Получает баланс из Redis или MongoDB"""
        balance_key = f"balance:{credit}"
        try:
            balance = await asyncio.wait_for(self.redis.client.get(balance_key), timeout=0.5)
            if balance is None:
                balance = await asyncio.wait_for(get_balance(credit, self.db), timeout=0.5) or 0
                await self.redis.client.set(balance_key, balance)
            else:
                balance = int(balance)
            return balance
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching balance for {credit}")
            return 0

    async def verify_parents(self, tx: TxStore) -> bool:
        """Проверяет наличие всех родительских транзакций"""
        # Check if parents exist in the graph
        if not all(parent in self.dag.G.nodes for parent in tx.parents):
            logger.warning(f"One or more parents not found for transaction {tx.uniq}")
            return False
        return True

    async def add_to_graph(self, tx_hash: str, tx: TxStore) -> bool:
        # Check for cycles
        G_temp = self.dag.G.copy()
        G_temp.add_node(tx_hash)
        for parent in tx.parents:
            G_temp.add_edge(parent, tx_hash)
        if not nx.is_directed_acyclic_graph(G_temp):
            logger.error(f"Cycle detected for {tx_hash}")
            return False

        # Add transaction to graph
        now = int(time.time_ns() / 1000)
        tx_data = dict(tx, time=now)
        self.dag.G.add_node(tx_hash, weight=0.0, data=tx_data)
        for parent in tx.parents:
            self.dag.G.add_edge(parent, tx_hash)
        await self.dag.save(self.redis, self.dag.G)
        return True

    async def confirm_transaction(self, tx: TxStore, tx_hash: str) -> bool:
        """Подтверждает транзакцию: обновляет балансы"""  
        balance_debit_key = f"balance:{tx.debit}"
        balance_credit_key = f"balance:{tx.credit}"
        async with self.redis.client.pipeline() as pipe:
            await pipe.incrby(balance_debit_key, tx.amount)
            await pipe.decrby(balance_credit_key, tx.amount)
            try:
                await pipe.execute()
            except Exception as e:
                logger.error(f"Pipeline failed: {e}")
                return False

        logger.info(f"Transaction confirmed: {tx_hash}")
        return True

class TransactionService:
    def __init__(self):
        self.db = MongoDB()
        self.rabbitmq = RabbitMQ(host='localhost', queue_name='tx')
        self.redis = Redis()
        self.dag = Dag()
        self.processor = TransactionProcessor(self.db, self.rabbitmq, self.redis, self.dag)
        self.running = False

    async def run(self) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Consumer process {os.getpid()} started")
            await self.rabbitmq.consume_messages(self.processor.process_message)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.rabbitmq.connect()
        await self.db.connect()
        await self.dag.load(self.redis)

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        if self.rabbitmq.connection and not self.rabbitmq.connection.is_closed:
            await self.rabbitmq.close()
            logger.info("RabbitMQ connection closed")
        await self.db.close()
        logger.info("All connections closed")

def run_consumer_process(stop_event):
    async def async_run():
        service = TransactionService()
        try:
            await service.run()
        except Exception as e:
            logger.error(f"Consumer process failed: {e}", exc_info=True)
        finally:
            await service.shutdown()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(async_run())
    except KeyboardInterrupt:
        logger.info("Consumer process interrupted")
    finally:
        loop.close()

def main():
    NUM_PROCESSES = 1
    processes = []
    stop_events = []

    for _ in range(NUM_PROCESSES):
        stop_event = Event()
        process = Process(target=run_consumer_process, args=(stop_event,))
        processes.append(process)
        stop_events.append(stop_event)
        process.start()

    logger.info(f"Started {NUM_PROCESSES} consumer processes")

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping processes...")
        for stop_event in stop_events:
            stop_event.set()
        for process in processes:
            process.terminate()
            process.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        for process in processes:
            process.join()
    except Exception as e:
        logger.error(f"Main process error: {e}")
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()

if __name__ == "__main__":
    main()