import logging
from dataclasses import dataclass
from src.database.graph import Memgraph
from src.database.redis import Redis
from src.database.queues import RabbitMQ
import os
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from neo4j.exceptions import TransientError

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Config:
    DAMPING: float = 0.5

class ConfirmationProcessor:
    def __init__(self, redis: Redis, dag: Memgraph):
        self.dag = dag
        self.redis = redis
        self.config = Config()

    @retry(
        stop=stop_after_attempt(5),  # Максимум 3 попытки
        wait=wait_exponential(multiplier=1, min=0.1, max=2),  # Экспоненциальная задержка
        retry=retry_if_exception_type(TransientError),  # Повтор для TransientError
        reraise=True  # Проброс оригинальной ошибки при неудаче
    )
    async def check_pool(self):
        """Пересчитывает веса всех транзакций в графе Memgraph"""
        # Получение потомков и глубины путей
        async with self.dag.driver.session() as session:
            # Получение всех узлов
            result = await session.run("MATCH (t:Transaction) RETURN count(t) AS total")
            record = await result.single()
            print("Nodes:", record["total"])
            result = await session.run("MATCH (t:Transaction) RETURN t.hash ORDER BY rand() LIMIT 100")
            nodes = [record["t.hash"] async for record in result]
            # Пересчёт весов
            for node in nodes:
                try:
                    async with self.redis.client.lock(f"dag:{node}"): 
                        result = await session.run(
                            """//cypher
                            MATCH path=(t:Transaction {hash: $hash})-[:CONFIRMS*1..7]->(descendant)
                            WITH length(path) as depth, descendant
                            WITH depth, descendant.own_weight as own_weight,
                                CASE depth
                                    WHEN 1 THEN 0.5
                                    WHEN 2 THEN 0.25
                                    WHEN 3 THEN 0.125
                                    WHEN 4 THEN 0.0625
                                    WHEN 5 THEN 0.03125
                                    WHEN 6 THEN 0.015625
                                    WHEN 7 THEN 0.0078125
                                    ELSE 0.0
                                END as damping_factor
                            RETURN sum(damping_factor * own_weight) as indirect_weight
                            """,
                            hash=node
                        )
                        record = await result.single()
                        indirect_weight = record["indirect_weight"] if record else 0.0
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
                except Exception as e:
                    logger.error(f"Service error: {e}")

    async def periodic_check_pool(self, stop_event=None):
        while not (stop_event and stop_event.is_set()):
            await self.check_pool()
            await asyncio.sleep(1)

class ConfirmationService:
    def __init__(self):
        self.dag = Memgraph()
        self.redis = Redis()
        self.rabbit = RabbitMQ()
        self.processor = ConfirmationProcessor(self.redis, self.dag)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Confirmation process {os.getpid()} started")
            await self.processor.periodic_check_pool(stop_event)
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