import asyncio
import logging
import os
import time

from neo4j.exceptions import Neo4jError, TransientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import CONFIRM_WEIGHT, DAMPING, MAX_DEPTH
from src.database.graph import Memgraph
from src.database.queues import RabbitMQ
from src.database.redis import Redis
from src.telemetry import metrics

logger = logging.getLogger(__name__)

# Корзины распределения весов обработанной выборки [lo, hi).
WEIGHT_BUCKETS = ((0, 1), (1, 2), (2, 3), (3, 5), (5, 10), (10, None))
CYCLE_HISTORY = 30


def _damping_case(max_depth: int, damping: float) -> str:
    """Строит Cypher-выражение CASE для коэффициентов затухания по глубине."""
    branches = "\n".join(f"WHEN {depth} THEN {damping**depth}" for depth in range(1, max_depth + 1))
    return f"""CASE depth
        {branches}
            ELSE 0.0
        END"""


class ConfirmationProcessor:
    def __init__(self, redis: Redis, dag: Memgraph):
        self.dag = dag
        self.redis = redis
        self.damping_case = _damping_case(MAX_DEPTH, DAMPING)
        metrics.set("confirm_weight", CONFIRM_WEIGHT)
        metrics.set("max_depth", MAX_DEPTH)
        metrics.set("damping", DAMPING)

    @retry(
        stop=stop_after_attempt(5),  # Максимум 5 попыток
        wait=wait_exponential(multiplier=1, min=0.1, max=2),  # Экспоненциальная задержка
        retry=retry_if_exception_type(TransientError),  # Повтор для TransientError
        reraise=True,  # Проброс оригинальной ошибки при неудаче
    )
    async def check_pool(self):
        """Пересчитывает веса выборки транзакций в графе Memgraph (не полный обход)."""
        cycle_started = time.time()
        processed = 0
        errors = 0
        weights: list[float] = []
        # Получение потомков и глубины путей
        async with self.dag.driver.session() as session:
            # Получение всех узлов (только число — без полного обхода данных)
            result = await session.run("MATCH (t:Transaction) RETURN count(t) AS total")
            record = await result.single()
            logger.info("Nodes: %s", record["total"])
            metrics.set("nodes_total", record["total"])
            result = await session.run("MATCH (t:Transaction) RETURN t.hash ORDER BY rand() LIMIT 100")
            nodes = [record["t.hash"] async for record in result]
            metrics.set("sample_size", len(nodes))
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
                        processed += 1
                        weights.append(total_weight)
                except (Neo4jError, OSError) as e:
                    logger.error("Failed to recalculate weight for %s: %s", node, e)
                    errors += 1
                    metrics.event("error", "weight", f"{node[:8]} вес не пересчитан", ref=node, key=f"weight:{node}")

        self._record_cycle(len(nodes), processed, errors, weights, cycle_started)

    def _record_cycle(
        self, sample: int, processed: int, errors: int, weights: list[float], cycle_started: float
    ) -> None:
        """Фиксирует телеметрию одного цикла (только обработанная выборка)."""
        now = time.time()
        cycle_ms = (now - cycle_started) * 1000
        metrics.inc("cycles")
        metrics.inc("processed", processed)
        metrics.inc("errors", errors)
        metrics.set("cycle_ms", cycle_ms)
        metrics.set("last_cycle_at", now)
        metrics.set("last_sample", sample)
        metrics.set("last_processed", processed)
        metrics.set("last_errors", errors)

        if weights:
            metrics.set("weights_min", min(weights))
            metrics.set("weights_avg", sum(weights) / len(weights))
            metrics.set("weights_max", max(weights))
            buckets = []
            for lo, hi in WEIGHT_BUCKETS:
                count = sum(1 for w in weights if lo <= w < (hi if hi is not None else float("inf")))
                buckets.append([lo, hi, count])
            metrics.set("weight_buckets", buckets)
        else:
            metrics.set("weights_min", None)
            metrics.set("weights_avg", None)
            metrics.set("weights_max", None)
            metrics.set("weight_buckets", None)

        history = list(metrics.gauge("cycle_history") or [])
        history.append({"ts": now, "ms": cycle_ms, "sample": sample, "processed": processed, "errors": errors})
        metrics.set("cycle_history", history[-CYCLE_HISTORY:])
        metrics.event(
            "info",
            "cycle",
            f"цикл: выборка {sample} → обработано {processed} → {errors} ошибки",
            key="cycle",
        )

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
