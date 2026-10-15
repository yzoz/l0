import asyncio
import logging
import os

from neo4j.exceptions import Neo4jError, TransientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import DAMPING, MAX_DEPTH
from src.database.graph import Memgraph
from src.database.queues import RabbitMQ
from src.database.redis import Redis

logger = logging.getLogger(__name__)


def _damping_case(max_depth: int, damping: float) -> str:
    """Строит Cypher-выражение CASE для коэффициентов затухания по глубине."""
    branches = "\n".join(
        f"                                    WHEN {depth} THEN {damping**depth}" for depth in range(1, max_depth + 1)
    )
    return f"""CASE depth
{branches}
                                    ELSE 0.0
                                END"""


class ConfirmationProcessor:
    def __init__(self, redis: Redis, dag: Memgraph):
        self.dag = dag
        self.redis = redis
        self.damping_case = _damping_case(MAX_DEPTH, DAMPING)

    @retry(
        stop=stop_after_attempt(5),  # Максимум 5 попыток
        wait=wait_exponential(multiplier=1, min=0.1, max=2),  # Экспоненциальная задержка
        retry=retry_if_exception_type(TransientError),  # Повтор для TransientError
        reraise=True,  # Проброс оригинальной ошибки при неудаче
    )
    async def check_pool(self):
        """Пересчитывает веса всех транзакций в графе Memgraph"""
        # Получение потомков и глубины путей
        async with self.dag.driver.session() as session:
            # Получение всех узлов
            result = await session.run("MATCH (t:Transaction) RETURN count(t) AS total")
            record = await result.single()
            logger.info("Nodes: %s", record["total"])
            result = await session.run("MATCH (t:Transaction) RETURN t.hash ORDER BY rand() LIMIT 100")
            nodes = [record["t.hash"] async for record in result]
            # Пересчёт весов
            for node in nodes:
                try:
                    async with self.redis.client.lock(f"dag:{node}"):
                        result = await session.run(
                            f"""//cypher
                            MATCH path=(t:Transaction {{hash: $hash}})-[:CONFIRMS*1..{MAX_DEPTH}]->(descendant)
                            WITH length(path) as depth, descendant
                            WITH depth, descendant.own_weight as own_weight,
                                {self.damping_case} as damping_factor
                            RETURN sum(damping_factor * own_weight) as indirect_weight
                            """,
                            hash=node,
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
                            weight=total_weight,
                        )
                except (Neo4jError, OSError) as e:
                    logger.error("Failed to recalculate weight for %s: %s", node, e)

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
