import asyncio
import json
import time
import logging
from dataclasses import dataclass
from typing import Optional
from aio_pika import IncomingMessage
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.models.tx import TxStore, TxWrite
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
    # Специальный адрес разработчика для обхода проверки баланса
    DEV_ADDRESS: str = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy"

class RedisClient:
    def __init__(self):
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self.client = await redis.Redis(host='localhost', db=0, decode_responses=True)  # Единая база

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()

class TransactionProcessor:
    """Класс, отвечающий за обработку транзакций"""
    def __init__(self, db: MongoDB, rabbitmq: RabbitMQ, redis_client: RedisClient):
        self.db = db  # База данных MongoDB
        self.rabbitmq = rabbitmq  # Очередь RabbitMQ
        self.redis = redis_client  # Клиенты Redis
        self.config = TransactionConfig()  # Конфигурация транзакций

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает входящее сообщение из RabbitMQ"""
        try:
            tx = self.parse_message(message)  # Парсим сообщение в объект
            await self.process_transaction(tx, message)  # Обрабатываем транзакцию
        except json.JSONDecodeError:
            logger.error("Failed to parse message: Invalid JSON format")
            await message.nack(requeue=False)  # Отклоняем сообщение без повторной постановки
        except asyncio.TimeoutError:
            logger.error("Timeout during processing")
            await message.nack(requeue=True)  # Повторная попытка
        except Exception as e:
            logger.error(f"Unexpected processing error: {e}", exc_info=True)
            await message.nack(requeue=False)

    def parse_message(self, message: IncomingMessage) -> TxStore:
        """Преобразует сырые данные сообщения в объект TxStore"""
        data = json.loads(message.body.decode("utf-8"))  # Декодируем байты в JSON
        content = data["content"]  # Извлекаем содержимое
        content["sign"] = data["sign"]  # Добавляем подпись
        return TxStore(**content)  # Создаем объект транзакции

    async def process_transaction(self, tx: TxStore, message: IncomingMessage) -> None:
        """Основная логика обработки одной транзакции"""

        if len(await self.redis.client.zrange("unconfirmed", 0, -1, withscores=False)) > 250:
            logger.info(f"Unconfirmed > 250")
            return await message.nack(requeue=False)
        
        # Проверяем, не является ли транзакция дубликатом
        if await self.check_duplicate(tx):
            return await message.nack(requeue=False)

        # Проверяем, достаточно ли средств на балансе
        if not await self.verify_balance(tx):
            return await message.nack(requeue=False)

        # Проверяем наличие родительских транзакций
        if not await self.verify_parents(tx):
            return await message.nack(requeue=False)

        # Выполняем пуллинг и обновляем данные
        if await self.execute_pooling(tx):
            return await message.ack()  # Подтверждаем успешную обработку
        else:
            return await message.nack(requeue=False)

    async def check_duplicate(self, tx: TxStore) -> bool:
        """Проверяет, существует ли уже такая транзакция в базе"""
        exists = await self.db.find_one({"credit": tx.credit, "uniq": tx.uniq})
        if exists:
            logger.info(f"Duplicate detected: {tx.uniq}")
            return True
        return False

    async def verify_balance(self, tx: TxStore) -> bool:
        """Проверяет, достаточно ли средств для выполнения транзакции"""
        balance = await self.get_balance(tx.credit)  # Получаем текущий баланс
        # Проверяем баланс, исключая DEV_ADDRESS
        if balance < tx.amount and tx.credit != self.config.DEV_ADDRESS:
            logger.error(f"Insufficient funds: {balance} < {tx.amount} for {tx.credit}")
            return False
        return True
    
    async def get_balance(self, credit: str) -> int:
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
        """Проверяет наличие всех родительских транзакций в пуле или home"""
        for parent_hash in tx.parents:
            pool_key = f"pool:{parent_hash}"
            home_key = f"home:{parent_hash}"
            parent_exists = await self.redis.client.get(pool_key) or await self.redis.client.get(home_key)
            if not parent_exists:
                logger.warning(f"Parent {parent_hash} not found for transaction {tx.uniq}")
                return False
        return True

    async def execute_pooling(self, tx: TxStore) -> None:
        """Добавляет транзакцию в пул, обновляет балансы и обрабатывает родителей"""
        tx_hash = hash_hex(ordered_sum(dict(tx)))
        now=int(time.time_ns() / 1000)

        pool_key = f"pool:{tx_hash}"
        refs_key = f"refs:{tx_hash}"
        time_key = f"time:{tx_hash}"
        balance_debit_key = f"balance:{tx.debit}"
        balance_credit_key = f"balance:{tx.credit}"
        unconfirmed_key = "unconfirmed"

        if self.redis.client.exists(pool_key) and tx.credit != self.config.DEV_ADDRESS:
            return False
        
        tx_data = dict(tx, time=now)

        async with self.redis.client.pipeline() as pipe:

            await pipe.set(pool_key, json.dumps(tx_data))
            await pipe.zadd(unconfirmed_key, {tx_hash: 0})
            await pipe.set(refs_key, "0")
            await pipe.set(time_key, now)
            await pipe.incrby(balance_debit_key, tx.amount)
            await pipe.decrby(balance_credit_key, tx.amount)
            try:
                await pipe.execute()
            except Exception as e:
                logger.error(f"Pipeline failed: {e}")
                return False

        logger.info(f"Transaction pooled: {tx_hash}")
        await self.handle_parent(tx.parents)

        return True

    async def handle_parent(self, parents: str) -> None:
        """Обрабатывает список родителей, увеличивая их счетчики ссылок"""
        for parent_hash in parents:
            pool_key = f"pool:{parent_hash}"
            refs_key = f"refs:{parent_hash}"
            if self.redis.client.exists(pool_key):
                async with self.redis.client.pipeline() as pipe:
                    await pipe.incr(refs_key)
                    await pipe.zincrby("unconfirmed", 1, parent_hash)
                    await pipe.execute()
                

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
    NUM_PROCESSES = 10
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