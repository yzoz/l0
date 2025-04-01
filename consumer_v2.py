import asyncio
import json
import time
import logging
from typing import Optional, Dict
from aio_pika import IncomingMessage, Message, DeliveryMode
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite, TxStore
from src.utils.data import hash_hex, ordered_sum
from src.utils.balance import get_balance
import redis.asyncio as redis

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Константы
HOME_TIME = 1800 * 1_000_000  # 30 минут
MAX_ATTEMPTS = 5  # Максимум попыток обработки

class TransactionProcessor:
    def __init__(self):
        self.db = MongoDB()
        self.rabbitmq = RabbitMQ(host='localhost', queue_name='tx')
        self.mom = redis.Redis(host='localhost', db=1)
        self.dad = redis.Redis(host='localhost', db=2)
        self.home = redis.Redis(host='localhost', db=3)

    async def is_duplicate(self, tx: TxStore) -> bool:
        """Проверка дубликата транзакции."""
        return bool(await self.db.find_one({"credit": tx.credit, "uniq": tx.uniq}))

    async def has_sufficient_balance(self, tx: TxStore) -> bool:
        """Проверка баланса отправителя."""
        balance = await get_balance(tx.credit, self.db)
        sufficient = balance >= tx.amount
        if not sufficient:
            logger.error(f"Insufficient balance for {tx.credit}: {balance} < {tx.amount}")
        return sufficient

    async def check_parents(self, tx: TxStore) -> tuple[bool, bool]:
        """Проверка наличия родителей в Redis или MongoDB."""
        pool_mom = await self.mom.get(tx.mom)
        pool_mom_dad = await self.dad.get(tx.mom)
        pool_home_mom = await self.home.get(tx.mom)
        mongo_mom = await self.db.find_one({"hash": tx.mom})

        pool_dad = await self.dad.get(tx.dad)
        pool_home_dad = await self.home.get(tx.dad)
        mongo_dad = await self.db.find_one({"hash": tx.dad})

        mom_exists = bool(pool_mom or pool_mom_dad or pool_home_mom or mongo_mom)
        dad_exists = bool(pool_dad or pool_home_dad or mongo_dad)

        logger.info(f"Checking MOM: pool_mom={pool_mom}, pool_mom_dad={pool_mom_dad}, "
                    f"pool_home_mom={pool_home_mom}, mongo_mom={mongo_mom is not None}")
        logger.info(f"Checking DAD: pool_dad={pool_dad}, pool_home_dad={pool_home_dad}, "
                    f"mongo_dad={mongo_dad is not None}")

        return mom_exists, dad_exists

    async def move_to_dad(self, tx_hash: str, pool_mom: bytes):
        """Перемещение транзакции из mom в dad."""
        await self.mom.delete(tx_hash)
        await self.dad.set(tx_hash, pool_mom)
        await self.dad.set("last", tx_hash)
        logger.info(f"Moved {tx_hash} from mom to dad: {pool_mom}")

    async def move_to_home(self, tx_hash: str, pool_data: bytes, is_mom: bool = True):
        """Перемещение транзакции в home и сохранение в MongoDB."""
        await (self.dad if is_mom else self.dad).delete(tx_hash)
        await self.home.set(tx_hash, pool_data)
        tx_data = json.loads(pool_data.decode("utf-8"))
        tx = TxWrite(**tx_data)
        tx.hash = tx_hash
        result = await self.db.insert_tx(tx)
        logger.info(f"Moved {tx_hash} to home")
        if result:
            logger.info(f"Saved transaction to MongoDB: {dict(tx)}")
        else:
            logger.error("Failed to save transaction to MongoDB")

    async def cleanup_home(self, tx: TxStore, now: int):
        """Удаление старых транзакций из home."""
        for parent, pool in [(tx.mom, await self.home.get(tx.mom)), (tx.dad, await self.home.get(tx.dad))]:
            if pool:
                t = json.loads(pool.decode("utf-8"))["time"]
                if now - t > HOME_TIME:
                    await self.home.delete(parent)
                    logger.info(f"Removed from home: {parent} - {pool}")

    async def process_transaction(self, tx: TxStore, hash: str):
        """Добавление транзакции в пул mom."""
        t = dict(tx)
        t["time"] = int(time.time_ns() / 1000)
        await self.mom.set(hash, json.dumps(t))
        await self.mom.set("last", hash)
        logger.info(f"Pooled transaction in mom: {t}")

    async def requeue_message(self, message: IncomingMessage, attempts: int):
        """Повторная отправка сообщения с увеличением попыток."""
        new_headers = (message.headers or {}).copy()
        new_headers["x-attempts"] = attempts + 1
        await self.rabbitmq.channel.default_exchange.publish(
            Message(
                body=message.body,
                headers=new_headers,
                delivery_mode=DeliveryMode.PERSISTENT
            ),
            routing_key=message.routing_key
        )
        logger.info(f"Requeued message with attempts={new_headers['x-attempts']}")

    async def on_message(self, message: IncomingMessage):
        """Обработка входящего сообщения из RabbitMQ."""
        logger.info("_" * 80)
        now = int(time.time_ns() / 1000)
        headers = message.headers or {}
        attempts = headers.get("x-attempts", 0)

        if attempts >= MAX_ATTEMPTS:
            logger.error(f"Max attempts ({MAX_ATTEMPTS}) reached, rejecting message")
            await message.reject(requeue=False)
            return

        try:
            tx_data = message.body.decode("utf-8")
            logger.info(f"Received tx: {tx_data}")
            tx_dict = json.loads(tx_data)
            c = tx_dict["content"]
            c["sign"] = tx_dict["sign"]
            m = ordered_sum(c)
            hash = hash_hex(m)
            tx = TxStore(**c)

            if await self.is_duplicate(tx):
                logger.info(f"Duplicate transaction detected: {tx.uniq}")
                await message.ack()
                return

            if not await self.has_sufficient_balance(tx):
                await message.reject(requeue=False)
                return

            mom_exists, dad_exists = await self.check_parents(tx)
            logger.info(f"Processing tx with mom={tx.mom}, dad={tx.dad}")

            if mom_exists and dad_exists:
                await self.process_transaction(tx, hash)

                # Перемещение родителей
                pool_mom = await self.mom.get(tx.mom)
                pool_mom_dad = await self.dad.get(tx.mom)
                pool_dad = await self.dad.get(tx.dad)

                if pool_mom:
                    await self.move_to_dad(tx.mom, pool_mom)
                if pool_mom_dad:
                    await self.move_to_home(tx.mom, pool_mom_dad, is_mom=True)
                if pool_dad:
                    await self.move_to_home(tx.dad, pool_dad, is_mom=False)

                await self.cleanup_home(tx, now)
                await message.ack()
            else:
                logger.error("No mom & dad in Pool")
                await self.requeue_message(message, attempts)
                await message.reject(requeue=False)  # Убираем из очереди после переотправки

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            await message.reject(requeue=True)

    async def run(self):
        """Запуск потребителя."""
        try:
            await self.rabbitmq.connect()
            logger.info("Connected to RabbitMQ. Waiting for messages...")
            await self.rabbitmq.consume_messages(self.on_message)
        except Exception as e:
            logger.error(f"Error in processor: {e}", exc_info=True)
            raise
        finally:
            await self.rabbitmq.close()
            await self.db.close()
            await self.mom.aclose()  # Используем aclose вместо close
            await self.dad.aclose()
            await self.home.aclose()
            logger.info("All connections closed")

if __name__ == "__main__":
    processor = TransactionProcessor()
    try:
        asyncio.run(processor.run())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)