import logging
from dataclasses import dataclass
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite
from src.database.redis import Redis
from src.database.graph import Dag
import os
import asyncio
from pymongo.errors import DuplicateKeyError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Config:
    WEIGHT: int = 25.0

class WriterProcessor:
    def __init__(self, db: MongoDB, redis: Redis, dag: Dag):
        self.db = db
        self.redis = redis
        self.dag = dag
        self.config = Config()

    async def check_pool(self):
        #async with self.redis.client.lock("graph:check_pool"):

        await self.dag.load(self.redis)

        confirmed = [node for node in self.dag.G.nodes if self.dag.G.nodes[node]['weight'] >= self.config.WEIGHT]

        for node in confirmed:
            lock = self.redis.client.lock(f"mongo:{node}", timeout=10)
            acquired = await lock.acquire(blocking=False)  # Одна попытка без ожидания
            if not acquired:
                print(f"Failed to acquire lock for {node}")
                return
            tx = self.create_tx(node, self.dag.G.nodes[node]['data'])
            try:
                success = await self.db.insert_tx(tx)
                logger.info(f"Confirmed TX saved: {tx.hash}" if success else f"Failed to save: {tx.hash}")
                if success:
                    self.dag.G.remove_node(node)
                    await self.dag.save(self.redis, self.dag.G)
                    """Обновляет балансы"""  
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
            except DuplicateKeyError:
                logger.warning(f"Duplicate key error for hash: {tx.hash}")
                self.dag.G.remove_node(node)
                await self.dag.save(self.redis, self.dag.G)
            finally:
                if acquired:
                    await lock.release()

    def create_tx(self, hash: str, data: dict) -> TxWrite:
        tx_dict = data
        tx_dict["hash"] = hash
        return TxWrite(**tx_dict)

    async def periodic_check_pool(self):
        while True:
            await self.check_pool()
            await asyncio.sleep(0.1)

class WriterService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = Redis()
        self.dag = Dag()
        self.processor = WriterProcessor(self.db, self.redis, self.dag)
        self.running = False

    async def run(self) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Writer process {os.getpid()} started")
            await self.processor.periodic_check_pool()
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.db.connect()
        await self.dag.load(self.redis)

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.db.close()
        logger.info("All connections closed")