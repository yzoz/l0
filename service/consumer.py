import asyncio
import json
import time
import logging
from aio_pika import IncomingMessage
import networkx as nx
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.database.graph import Memgraph
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
    def __init__(self, db: MongoDB, redis: Redis, dag: Memgraph):
        self.db = db
        self.redis = redis
        self.dag = dag

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает входящее сообщение из RabbitMQ"""
        tx = self.parse_message(message)
        await self.process_transaction(tx, message)
        """try:
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
            await message.nack(requeue=False)"""

    def parse_message(self, message: IncomingMessage) -> TxStore:
        """Преобразует сырые данные сообщения в объект TxStore"""
        data = json.loads(message.body.decode("utf-8"))
        content = data["content"]
        content["sign"] = data["sign"]
        return TxStore(**content)

    async def process_transaction(self, tx: TxStore, message: IncomingMessage) -> None:
        """Обрабатывает одну транзакцию"""
        tx_hash = hash_hex(ordered_sum(dict(tx)))
        async with self.redis.client.lock(f"rabbit:{tx_hash}", timeout=10):
            # Проверка дубликатов
            if await self.check_duplicate(tx):
                logger.warning(f"Duplicate transaction: {tx_hash}")
                await message.nack(requeue=False)
                return

            # Проверка баланса
            if not await self.verify_balance(tx):
                logger.error(f"Insufficient funds for {tx_hash}")
                await message.nack(requeue=False)
                return

            # Проверка наличия родителей
            if not await self.verify_parents(tx):
                logger.warning(f"One or more parents not found for transaction {tx_hash}")
                await message.nack(requeue=False)
                return

            # Проверка и добавление в граф
            if not await self.add_to_graph(tx_hash, tx):
                logger.warning(f"Failed to add transaction {tx_hash} to graph")
                await message.nack(requeue=False)
                return

            await message.ack()
            logger.info(f"Transaction {tx_hash} processed and added to graph")

    async def check_duplicate(self, tx: TxStore) -> bool:
        """Проверяет, существует ли уже такая транзакция в базе"""
        exists = await self.db.find_one({"credit": tx.credit, "uniq": tx.uniq})
        return bool(exists)

    async def verify_balance(self, tx: TxStore) -> bool:
        """Проверяет, достаточно ли средств для выполнения транзакции"""
        balance = await get_balance(tx.credit, self.db, self.redis)
        return balance >= tx.amount

    async def verify_parents(self, tx: TxStore) -> bool:
        """Проверяет наличие всех родительских транзакций"""
        async with self.dag.driver.session() as session:
            result = await session.run(
                """
                MATCH (t:Transaction)
                WHERE t.hash IN $parent_hashes
                RETURN count(t) as found
                """,
                parent_hashes=tx.parents
            )
            record = await result.single()
            return record["found"] == len(tx.parents)

    async def add_to_graph(self, tx_hash: str, tx: TxStore) -> bool:
        """Добавляет транзакцию в граф Memgraph"""
        async with self.dag.driver.session() as session:
            # Add transaction to graph
            now = int(time.time_ns() / 1000)
            tx_data = dict(tx, time=now)
            try:
                await session.run(
                    """
                    CREATE (t:Transaction {
                        hash: $hash,
                        own_weight: 1.0,
                        weight: 1.0,
                        data: {}
                    })
                    """,
                    hash=tx_hash,
                    data=tx_data
                )
                # Создание рёбер от родителей к нашему узлу
                for parent in tx.parents:
                    await session.run(
                        """
                        MATCH (parent:Transaction {hash: $parent_hash})
                        MATCH (child:Transaction {hash: $child_hash})
                        CREATE (parent)-[:CONFIRMS]->(child)
                        """,
                        parent_hash=parent,
                        child_hash=tx_hash
                    )
                # Проверка на цикл, пути от узла к самому себе
                result = await session.run(
                    """
                    MATCH path = (t:Transaction)-[:CONFIRMS*]->(t)
                    WHERE t.hash = $hash
                    RETURN count(path) as cycle_count
                    """,
                    hash=tx_hash
                )
                cycle_count = (await result.single())["cycle_count"]
                if cycle_count > 0:
                    logger.warning(f"Cycle detected for {tx_hash}")
                    await session.run(
                        "MATCH (t:Transaction {hash: $hash}) DETACH DELETE t",
                        hash=tx_hash
                    )
                    return False
                
                return True
            except Exception as e:
                logger.error(f"Service error: {e}", exc_info=True)
                return False 

class ConsumerService:
    def __init__(self):
        self.db = MongoDB()
        self.rabbitmq = RabbitMQ()
        self.redis = Redis()
        self.dag = Memgraph()
        self.processor = ConsumerProcessor(self.db, self.redis, self.dag)
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
        await self.dag.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        if self.rabbitmq.connection and not self.rabbitmq.connection.is_closed:
            await self.rabbitmq.close()
            logger.info("RabbitMQ connection closed")
        await self.db.close()
        await self.dag.close()
        logger.info("All connections closed")