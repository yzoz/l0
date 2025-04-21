import asyncio
import json
import time
import logging
from dataclasses import dataclass
from typing import Optional, Set
from aio_pika import IncomingMessage
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.models.tx import TxStore
from src.utils.data import hash_hex, ordered_sum
from src.utils.balance import get_balance
import redis.asyncio as redis
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

class RedisClient:
    def __init__(self):
        self.pool: Optional[redis.ConnectionPool] = None
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self.pool = redis.ConnectionPool(host='localhost', db=0, decode_responses=True)
        self.client = redis.Redis(connection_pool=self.pool)

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()
        if self.pool:
            self.pool.disconnect()

class TransactionProcessor:
    """Класс, отвечающий за обработку транзакций и управление графом"""
    def __init__(self, db: MongoDB, rabbitmq: RabbitMQ, redis_client: RedisClient):
        self.db = db
        self.rabbitmq = rabbitmq
        self.redis = redis_client
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
        """Основная логика обработки одной транзакции"""
        tx_hash = hash_hex(ordered_sum(dict(tx)))

        # Проверка лимита неподтверждённых транзакций
        if await self.redis.client.zcard("unconfirmed") > 1000:
            logger.info("Unconfirmed transactions limit exceeded")
            return await message.nack(requeue=True)

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
        if not await self.add_to_graph(tx_hash, tx.parents):
            return await message.nack(requeue=False)

        # Подтверждение транзакции
        if await self.confirm_transaction(tx, tx_hash):
            await message.ack()
        else:
            await self.remove_from_graph(tx_hash, tx.parents)
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
        async with self.redis.client.pipeline() as pipe:
            for parent_hash in tx.parents:
                pipe.sismember("graph:nodes", parent_hash)
            results = await pipe.execute()
        if not all(results):
            logger.warning(f"One or more parents not found for transaction {tx.uniq}")
            return False
        return True

    async def add_to_graph(self, tx_hash: str, parents: list) -> bool:
        #async with self.redis.client.lock(f"lock:graph:{tx_hash}"):
        """Добавляет транзакцию в граф и проверяет на циклы"""
        adj_key = f"graph:adj:{tx_hash}"
        async with self.redis.client.pipeline() as pipe:
            await pipe.sadd("graph:nodes", tx_hash)
            for parent_hash in parents:
                await pipe.sadd(adj_key, parent_hash)
            try:
                await pipe.execute()
            except Exception as e:
                logger.error(f"Failed to add {tx_hash} to graph: {e}")
                return False

        # Проверка на циклы
        if await self.has_cycle(tx_hash):
            logger.error(f"Cycle detected for {tx_hash}")
            await self.remove_from_graph(tx_hash, parents)
            return False

        return True

    async def remove_from_graph(self, tx_hash: str, parents: list) -> None:
        """Удаляет транзакцию из графа"""
        adj_key = f"graph:adj:{tx_hash}"
        async with self.redis.client.pipeline() as pipe:
            await pipe.srem("graph:nodes", tx_hash)
            await pipe.delete(adj_key)
            await pipe.execute()

    async def has_cycle(self, start_node: str) -> bool:
        """Проверка на наличие цикла с использованием DFS"""
        visited = set()
        rec_stack = set()

        async def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            adj_key = f"graph:adj:{node}"
            neighbors = await self.redis.client.smembers(adj_key) or set()
            for neighbor in neighbors:
                if neighbor not in visited:
                    if await dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            rec_stack.remove(node)
            return False

        return await dfs(start_node)

    async def confirm_transaction(self, tx: TxStore, tx_hash: str) -> bool:
        """Подтверждает транзакцию: обновляет балансы, счётчики"""
        now = int(time.time_ns() / 1000)
        pool_key = f"pool:{tx_hash}"
        time_key = f"time:{tx_hash}"
        balance_debit_key = f"balance:{tx.debit}"
        balance_credit_key = f"balance:{tx.credit}"
        unconfirmed_key = "unconfirmed"

        if await self.redis.client.exists(pool_key) and tx.credit != self.config.DEV_ADDRESS:
            logger.info(f"Transaction here: {tx_hash}")
            return False

        tx_data = dict(tx, time=now)

        async with self.redis.client.pipeline() as pipe:
            await pipe.set(pool_key, json.dumps(tx_data))
            await pipe.zadd(unconfirmed_key, {tx_hash: 0})
            await pipe.set(time_key, now)
            await pipe.incrby(balance_debit_key, tx.amount)
            await pipe.decrby(balance_credit_key, tx.amount)
            try:
                await pipe.execute()
            except Exception as e:
                logger.error(f"Pipeline failed: {e}")
                return False
        
        """Обновляет счётчики ссылок для родителей"""
        async with self.redis.client.pipeline() as pipe:
            for parent_hash in tx.parents:
                await pipe.zincrby("unconfirmed", 1, parent_hash)
            await pipe.execute()

        logger.info(f"Transaction confirmed: {tx_hash}")
        return True

class TransactionService:
    def __init__(self):
        self.db = MongoDB()
        self.rabbitmq = RabbitMQ(host='localhost', queue_name='tx')
        self.redis = RedisClient()
        self.processor = TransactionProcessor(self.db, self.rabbitmq, self.redis)
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