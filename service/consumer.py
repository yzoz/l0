import asyncio
import json
import time
import logging
from aio_pika import IncomingMessage
import networkx as nx
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.database.graph import Dag
from src.models.tx import TxStore
from src.utils.data import hash_hex, ordered_sum
from src.utils.profile import get_balance
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConsumerProcessor:
    """Класс, отвечающий за обработку транзакций и управление графом"""
    def __init__(self, db: MongoDB, rabbitmq: RabbitMQ, redis: Redis, dag: Dag):
        self.db = db
        self.rabbitmq = rabbitmq
        self.redis = redis
        self.dag = dag

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
        async with self.redis.client.lock(f"rabbit:{tx_hash}"):
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
            else:
                await message.ack()

    async def check_duplicate(self, tx: TxStore) -> bool:
        """Проверяет, существует ли уже такая транзакция в базе"""
        exists = await self.db.find_one({"credit": tx.credit, "uniq": tx.uniq})
        if exists:
            logger.warning(f"Duplicate detected: {tx.uniq}")
            return True
        return False

    async def verify_balance(self, tx: TxStore) -> bool:
        """Проверяет, достаточно ли средств для выполнения транзакции"""
        balance = await get_balance(tx.credit, self.db, self.redis)
        if balance < tx.amount:
            logger.error(f"Insufficient funds: {balance} < {tx.amount} for {tx.credit}")
            return False
        return True

    async def verify_parents(self, tx: TxStore) -> bool:
        """Проверяет наличие всех родительских транзакций"""
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
            logger.warning(f"Cycle detected for {tx_hash}")
            return False

        # Add transaction to graph
        now = int(time.time_ns() / 1000)
        tx_data = dict(tx, time=now)
        self.dag.G.add_node(tx_hash, weight=0.0, data=tx_data)
        for parent in tx.parents:
            self.dag.G.add_edge(parent, tx_hash)
        await self.dag.save(self.redis, self.dag.G)
        return True

class ConsumerService:
    def __init__(self):
        self.db = MongoDB()
        self.rabbitmq = RabbitMQ()
        self.redis = Redis()
        self.dag = Dag()
        self.processor = ConsumerProcessor(self.db, self.rabbitmq, self.redis, self.dag)
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