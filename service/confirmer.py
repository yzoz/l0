import logging
from dataclasses import dataclass
from src.database.graph import Memgraph
from src.database.redis import Redis
import os
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Config:
    DAMPING: int = 0.5

class ConfirmationProcessor:
    def __init__(self, redis: Redis, dag: Memgraph):
        self.dag = dag
        self.redis = redis
        self.config = Config()

    async def check_pool(self):
        """Пересчитывает веса всех транзакций в графе Memgraph"""
        async with self.dag.driver.session() as session:
            # Получение всех узлов
            result = await session.run("MATCH (t:Transaction) RETURN t.hash")
            nodes = [record["t.hash"] async for record in result]
            i = 0
            # Пересчёт весов
            for node in nodes:
                # Получение потомков и глубины путей
                #async with self.redis.client.lock(f"graph:{node}", timeout=10):
                lock = self.redis.client.lock(f"mongo:{t["hash"]}")
                acquired = await lock.acquire(blocking=False)  # Одна попытка без ожидания
                if not acquired:
                    logger.info(f"Failed to acquire lock for {t["hash"]}")
                    return
                try:
                    result = await session.run(
                        """
                        MATCH path=(t:Transaction {hash: $hash})-[:CONFIRMS*1..]->(descendant)
                        RETURN length(path) as depth, descendant.own_weight as own_weight
                        """,
                        hash=node
                    )
                    indirect_weight = 0.0
                    async for record in result:
                        depth = record["depth"]
                        own_weight = record["own_weight"]
                        # Вычисление затухания на Python: 0.5^depth
                        damping_factor = self.config.DAMPING ** depth
                        indirect_weight += damping_factor * own_weight

                    total_weight = 1.0 + indirect_weight  # own_weight + indirect_weight

                    # Обновление веса узла
                    await session.run(
                        """
                        MATCH (t:Transaction {hash: $hash})
                        SET t.weight = $weight
                        """,
                        hash=node,
                        weight=total_weight
                    )
                    i += 1
                except Exception as e:
                    logger.error(f"Service error: {e}", exc_info=True)
                finally:
                        if acquired:
                            await lock.release()

            # Логирование количества узлов
            logger.info(f"Number of nodes processed: {i}")

    async def periodic_check_pool(self):
        while True:
            await self.check_pool()
            await asyncio.sleep(0.25)

class ConfirmationService:
    def __init__(self):
        self.dag = Memgraph()
        self.redis = Redis()
        self.processor = ConfirmationProcessor(self.redis, self.dag)
        self.running = False

    async def run(self) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Confirmation process {os.getpid()} started")
            await self.processor.periodic_check_pool()
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.dag.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.dag.close()
        logger.info("All connections closed")