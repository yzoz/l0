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
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import sys

# Настройка логирования с красивым форматом вывода
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TransactionConfig:
    """Конфигурация параметров транзакций"""
    # Специальный адрес разработчика для обхода проверки баланса
    DEV_ADDRESS: str = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy"
    CLEAR: int = 5
    CONFIRM: int = 3

class RedisClient:
    def __init__(self):
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self.client = await redis.Redis(host='localhost', db=0)  # Единая база

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
        self.count = 0
        #self.loop = asyncio.get_event_loop()
        #self.loop.create_task(self.periodic_check_pool())

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает входящее сообщение из RabbitMQ"""
        logger.info(f"{'_' * 80}\nNew message received")  # Разделитель в логах
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
        if self.count % self.config.CLEAR == 0:
            await self.check_pool()

    def parse_message(self, message: IncomingMessage) -> TxStore:
        """Преобразует сырые данные сообщения в объект TxStore"""
        data = json.loads(message.body.decode("utf-8"))  # Декодируем байты в JSON
        content = data["content"]  # Извлекаем содержимое
        content["sign"] = data["sign"]  # Добавляем подпись
        return TxStore(**content)  # Создаем объект транзакции

    async def process_transaction(self, tx: TxStore, message: IncomingMessage) -> None:
        """Основная логика обработки одной транзакции"""
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
        await self.execute_pooling(tx)
        await message.ack()  # Подтверждаем успешную обработку
        self.count += 1

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
    """
    async def get_balance(self, credit: str) -> int:
        balance_key = f"balance:{credit}"
        try:
            balance = await asyncio.wait_for(self.redis.client.get(balance_key), timeout=5.0)
            if balance is None:
                balance = await asyncio.wait_for(get_balance(credit, self.db), timeout=5.0) or 0
                await self.redis.client.set(balance_key, balance)
            else:
                balance = int(balance.decode("utf-8"))
            return balance
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching balance for {credit}")
            return 0
    """
    async def get_balance(self, credit: str) -> int:
        """Получает или обновляет баланс аккаунта"""
        balance_key = f"balance:{credit}"
        balance = await self.redis.client.get(balance_key)  # Проверяем Redis
        if balance is None:  # Если в Redis нет данных
            balance = await get_balance(credit, self.db) or 0  # Запрашиваем из базы
            await self.redis.client.set(balance_key, balance)  # Кэшируем в Redis
        else:
            balance = int(balance.decode("utf-8"))  # Декодируем из байтов
        return balance

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
        tx_data = dict(tx, time=int(time.time_ns() / 1000))

        async with self.redis.client.pipeline() as pipe:
            pool_key = f"pool:{tx_hash}"
            refs_key = f"refs:{tx_hash}"
            balance_debit_key = f"balance:{tx.debit}"
            balance_credit_key = f"balance:{tx.credit}"
            unconfirmed_key = "unconfirmed"

            await pipe.set(pool_key, json.dumps(tx_data))
            await pipe.zadd(unconfirmed_key, {tx_hash: 0})
            await pipe.set(refs_key, "0")
            await pipe.incr(balance_debit_key, amount=tx.amount)
            await pipe.decr(balance_credit_key, amount=tx.amount)
            await pipe.execute()

        logger.info(f"Transaction pooled: {tx_hash}")
        await self.handle_parent(tx.parents)

    async def handle_parent(self, parents: str) -> None:
        """Обрабатывает список родителей, увеличивая их счетчики ссылок"""
        for parent_hash in parents:
            pool_key = f"pool:{parent_hash}"
            refs_key = f"refs:{parent_hash}"
            if await self.redis.client.get(pool_key):
                async with self.redis.client.pipeline() as pipe:
                    await pipe.incr(refs_key)
                    await pipe.zincrby("unconfirmed", 1, parent_hash)
                    await pipe.execute()

    async def confirm_tx(self, tx_hash: str) -> bool:
        home_key = f"home:{tx_hash}"
        refs_key = f"refs:{tx_hash}"
        pool_key = f"pool:{tx_hash}"
        if await self.redis.client.get(home_key):
            return False
        refs = int(await self.redis.client.get(refs_key) or 0)
        if refs >= self.config.CONFIRM:
            tx_data = await self.redis.client.get(pool_key)
            if tx_data:
                # Создаем и сохраняем транзакцию в базе
                confirmed_tx = self.create_tx(tx_hash, tx_data)
                success = await self.db.insert_tx(confirmed_tx)
                logger.info(f"Confirmed TX saved: {tx_hash}" if success else f"Failed to save: {tx_hash}")
                if success:
                    async with self.redis.client.pipeline() as pipe:
                        await pipe.zrem("unconfirmed", tx_hash)
                        await pipe.delete(pool_key)
                        await pipe.delete(refs_key)
                        await pipe.set(home_key, tx_data)
                        await pipe.execute()
                return success
        return False

    def create_tx(self, hash: str, data: bytes) -> TxWrite:
        try:
            tx_dict = json.loads(data.decode("utf-8"))
            tx_dict["hash"] = hash
            return TxWrite(**tx_dict)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Failed to parse parent data for {hash}: {e}")
            raise  # Или вернуть None и обработать выше

    # Периодическая проверка пула
    async def check_pool(self):
        tx_hashes = await self.redis.client.zrange("unconfirmed", 0, -1)
        for tx_hash in tx_hashes:
            await self.confirm_tx(tx_hash.decode())

    """async def periodic_check_pool(self):
        while True:
            await self.check_pool()
            await asyncio.sleep(1)"""

class TransactionService:
    """Основной сервис для обработки транзакций"""
    def __init__(self):
        self.db = MongoDB()  # Инициализация MongoDB
        self.rabbitmq = RabbitMQ(host='localhost', queue_name='tx')  # Инициализация RabbitMQ
        self.redis = RedisClient()  # Инициализация Redis клиентов
        self.processor = TransactionProcessor(self.db, self.rabbitmq, self.redis)  # Процессор
        self.running = False

    async def run(self) -> None:
        """Запускает сервис обработки транзакций"""
        self.running = True
        try:
            await self.startup()  # Подключаемся к сервисам
            logger.info("Service started. Processing messages...")
            # Начинаем слушать очередь
            await self.rabbitmq.consume_messages(self.processor.process_message)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()  # Очищаем ресурсы

    async def startup(self) -> None:
        """Инициализирует все необходимые подключения"""
        await self.redis.connect()  # Подключаемся к Redis
        await self.rabbitmq.connect()  # Подключаемся к RabbitMQ
        logger.info("Connected to RabbitMQ and Redis")

    async def shutdown(self) -> None:
        """Закрывает все соединения при завершении работы"""
        self.running = False
        await self.redis.close()  # Закрываем Redis
        if self.rabbitmq.connection and not self.rabbitmq.connection.is_closed:
            await self.rabbitmq.close()  # Закрываем RabbitMQ, если открыт
            logger.info("RabbitMQ connection closed")
        await self.db.close()  # Закрываем MongoDB
        logger.info("All connections closed")

class ReloadHandler(FileSystemEventHandler):
    """Обработчик событий изменения конкретного файла для перезапуска"""
    def __init__(self, service: TransactionService, target_file: str):
        self.service = service
        self.target_file = os.path.abspath(target_file)  # Полный путь к файлу
        self.loop = asyncio.get_event_loop()

    def on_modified(self, event):
        """Вызывается при изменении файла"""
        if not event.is_directory and os.path.abspath(event.src_path) == self.target_file:
            logger.info(f"Target file {self.target_file} modified, restarting...")
            asyncio.ensure_future(self.stop_and_restart(), loop=self.loop)

    async def stop_and_restart(self):
        """Останавливает сервис и перезапускает программу"""
        if self.service.running:
            await self.service.shutdown()
            logger.info("Restarting application...")
            os.execv(sys.executable, [sys.executable] + sys.argv)

async def main() -> None:
    """Точка входа в приложение с поддержкой перезапуска"""
    service = TransactionService()
    
    # Указываем конкретный файл для отслеживания (замените на имя вашего файла)
    target_file = "consumer.py"  # Имя текущего файла 
    event_handler = ReloadHandler(service, target_file)
    
    # Настройка наблюдателя только для директории с целевым файлом
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(os.path.abspath(target_file)), recursive=False)
    observer.start()
    logger.info(f"File watcher started, monitoring {target_file} for changes...")

    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
        observer.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        observer.stop()
    finally:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    asyncio.run(main())