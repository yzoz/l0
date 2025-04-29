import logging
from dataclasses import dataclass
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite
from src.database.redis import Redis
from src.database.graph import Memgraph
import os
import asyncio
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Config:
    WEIGHT: int = 10

class WriterProcessor:
    def __init__(self, db: MongoDB, redis: Redis, dag: Memgraph):
        self.db = db
        self.redis = redis
        self.dag = dag
        self.config = Config()

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
                threshold=self.config.WEIGHT
            )
            confirmed = [
                {
                    "hash": record["t.hash"],
                    "data": record["t.data"]
                }
                async for record in result
            ]

            for t in confirmed:
                try:
                    confirmed_key = f"confirmed:{t["hash"]}"
                    async with self.redis.client.lock(f"writer:{t["hash"]}"):
                        if not await self.redis.client.exists(confirmed_key):
                            tx = self.create_tx(t["hash"], t["data"])
                        
                            success = await self.db.insert_tx(tx)
                            logger.info(f"Confirmed TX saved: {tx.hash}" if success else f"Failed to save: {tx.hash}")
                            if success:
                                try:
                                    async with self.redis.client.lock(f"dag:{tx.hash}"):
                                        await session.run(
                                            """
                                            MATCH (t:Transaction {hash: $hash})
                                            DETACH DELETE t
                                            """,
                                            hash=tx.hash
                                        )
                                except Exception as e:
                                    logger.error(f"Service error: {e}")

                                """Обновляет балансы"""
                                balance_debit_key = f"balance:{tx.debit}"
                                balance_credit_key = f"balance:{tx.credit}"
                                async with self.redis.client.pipeline() as pipe:
                                    await pipe.incrby(balance_debit_key, tx.amount)
                                    await pipe.decrby(balance_credit_key, tx.amount)
                                    await pipe.set(confirmed_key, 1, ex=360)
                                    try:
                                        await pipe.execute()
                                    except Exception as e:
                                        logger.error(f"Pipeline failed: {e}")
                                        return False
                                    
                except DuplicateKeyError:
                    logger.warning(f"Duplicate key error for hash: {tx.hash}")
                    try:
                        async with self.redis.client.lock(f"dag:{tx.hash}"):
                            await session.run(
                                """
                                MATCH (t:Transaction {hash: $hash})
                                DETACH DELETE t
                                """,
                                hash=tx.hash
                            )
                    except Exception as e:
                        logger.error(f"Service error: {e}")

    def create_tx(self, hash: str, data: dict) -> TxWrite:
        tx_dict = data
        tx_dict["hash"] = hash
        #print(tx_dict)
        return TxWrite(**tx_dict)

    async def periodic_check_pool(self, stop_event=None):
        while not (stop_event and stop_event.is_set()):
            await self.check_pool()
            await asyncio.sleep(0.025)

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