import asyncio
import logging
import os

from neo4j.exceptions import Neo4jError
from pymongo.errors import DuplicateKeyError

from src.config import CONFIRM_WEIGHT, CONFIRMED_TTL
from src.database.graph import Memgraph
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.models.tx import TxWrite
from src.utils.profile import get_balance

logger = logging.getLogger(__name__)


class WriterProcessor:
    def __init__(self, db: MongoDB, redis: Redis, dag: Memgraph):
        self.db = db
        self.redis = redis
        self.dag = dag

    async def _delete_node(self, session, tx_hash: str) -> bool:
        """Удаляет узел транзакции из графа Memgraph."""
        try:
            async with self.redis.client.lock(f"dag:{tx_hash}"):
                await session.run(
                    """
                    MATCH (t:Transaction {hash: $hash})
                    DETACH DELETE t
                    """,
                    hash=tx_hash,
                )
            return True
        except Neo4jError as e:
            logger.error("Failed to delete graph node %s: %s", tx_hash, e)
            return False

    async def _confirm(self, session, tx_hash: str, data: dict) -> None:
        """Подтверждает одну транзакцию в явном порядке Mongo -> Redis -> Memgraph.

        MongoDB считается источником истины: запись в неё выполняется первой, а
        при сбое последующих шагов вставленный документ удаляется (компенсация).
        """
        tx = self.create_tx(tx_hash, data)
        confirmed_key = f"confirmed:{tx.hash}"

        # 1. MongoDB — источник истины.
        try:
            inserted = await self.db.insert_tx(tx)
        except DuplicateKeyError:
            logger.warning("Duplicate key error for hash: %s", tx.hash)
            await self._delete_node(session, tx.hash)
            return

        if not inserted:
            logger.error("Failed to insert confirmed TX: %s", tx.hash)
            return
        logger.info("Confirmed TX saved: %s", tx.hash)

        # 2. Redis — обновление балансов и маркера подтверждения.
        try:
            async with self.redis.client.pipeline() as pipe:
                await pipe.incrby(f"balance:{tx.debit}", tx.amount)
                await pipe.decrby(f"balance:{tx.credit}", tx.amount)
                await pipe.set(confirmed_key, 1, ex=CONFIRMED_TTL)
                await pipe.execute()
        except Exception as e:
            # Компенсация: откатываем источник истины, чтобы транзакция
            # не осталась подтверждённой без обновлённых балансов.
            logger.error("Redis pipeline failed for %s, rolling back Mongo: %s", tx.hash, e)
            try:
                await self.db.tx.delete_one({"hash": tx.hash})
            except Exception:
                logger.error("Failed to roll back Mongo TX %s", tx.hash, exc_info=True)
            return

        # 3. Memgraph — очистка узла из графа.
        # На этом шаге данные уже согласованы; при сбое узел останется в графе,
        # но из-за маркера confirmed_key повторно обработан не будет.
        await self._delete_node(session, tx.hash)

    async def check_pool(self):
        """Проверяет пул неподтверждённых транзакций и подтверждает те, что превышают порог"""
        async with self.dag.driver.session() as session:
            # Получение транзакций с весом >= порога
            result = await session.run(
                """
                MATCH (t:Transaction)
                WHERE t.weight >= $threshold
                RETURN t.hash, t.data
                ORDER BY rand() LIMIT 100
                """,
                threshold=CONFIRM_WEIGHT,
            )
            confirmed = [{"hash": record["t.hash"], "data": record["t.data"]} async for record in result]

            for t in confirmed:
                confirmed_key = f"confirmed:{t['hash']}"
                async with self.redis.client.lock(f"writer:{t['hash']}"):
                    if await self.redis.client.exists(confirmed_key):
                        continue

                    tx = self.create_tx(t["hash"], t["data"])
                    balance = await get_balance(tx.credit, self.db, self.redis)
                    if balance < tx.amount and "genesis" not in (tx.msg or ""):
                        # Недостаточно средств — транзакция не может быть подтверждена.
                        await self._delete_node(session, tx.hash)
                        continue

                    await self._confirm(session, t["hash"], t["data"])

    def create_tx(self, hash: str, data: dict) -> TxWrite:
        tx_dict = dict(data)
        tx_dict["hash"] = hash
        return TxWrite(**tx_dict)

    async def periodic_check_pool(self, stop_event=None):
        while not (stop_event and stop_event.is_set()):
            await self.check_pool()
            await asyncio.sleep(0.1)


class WriterService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = Redis()
        self.dag = Memgraph()
        self.processor = WriterProcessor(self.db, self.redis, self.dag)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Writer process {os.getpid()} started")
            await self.processor.periodic_check_pool(stop_event)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.db.connect()
        await self.dag.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.db.close()
        await self.dag.close()
        logger.info("All connections closed")
