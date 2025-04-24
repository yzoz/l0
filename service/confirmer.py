import logging
from dataclasses import dataclass
from src.database.graph import Memgraph
from src.database.redis import Redis
from src.database.queues import RabbitMQ
from aio_pika import IncomingMessage
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

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает входящее сообщение из RabbitMQ"""
        try:
            tx_hash = message.body.decode("utf-8")
            print(tx_hash)
            await self.check_pool(tx_hash, message)
        except asyncio.TimeoutError:
            logger.error("Timeout during processing")
            await message.nack(requeue=True)
        except Exception as e:
            logger.error(f"Unexpected processing error: {e}", exc_info=True)
            await message.nack(requeue=False)

    async def check_pool(self, tx, message: IncomingMessage):
        """Пересчитывает веса всех транзакций в графе Memgraph"""
        # Получение потомков и глубины путей
        async with self.redis.client.lock(f"graph:{tx}"):
            async with self.dag.driver.session() as session:
                """
                MATCH path=(t:Transaction)-[:CONFIRMS*0..10]-(related:Transaction)
                WHERE t.hash = $hash
                RETURN DISTINCT related.hash AS hash
                UNION
                MATCH (t:Transaction {hash: $hash})
                RETURN t.hash AS hash
                """
                # Получение всех узлов
                # Получение связанных узлов (предки, сама транзакция, потомки)
                result = await session.run(
                    """
                    MATCH path=(t:Transaction)-[:CONFIRMS*0..5]-(related:Transaction)
                    WHERE t.hash = $hash
                    RETURN DISTINCT related.hash AS hash
                    UNION
                    MATCH (t:Transaction {hash: $hash})
                    RETURN t.hash AS hash
                    """,
                    hash=tx
                )
                nodes = [record["hash"] async for record in result]
                i = 0
                # Пересчёт весов
                for node in nodes:
                    try:
                        result = await session.run(
                            """
                            MATCH path=(t:Transaction {hash: $hash})-[:CONFIRMS*1..5]->(descendant)
                            WITH length(path) as depth, descendant
                            WITH depth, descendant.own_weight as own_weight,
                                CASE depth
                                    WHEN 1 THEN 0.5
                                    WHEN 2 THEN 0.25
                                    WHEN 3 THEN 0.125
                                    WHEN 4 THEN 0.0625
                                    WHEN 5 THEN 0.03125
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
                        i += 1
                    except Exception as e:
                        logger.error(f"Service error: {e}")
            await message.ack()
            # Логирование количества узлов
            logger.info(f"Number of nodes processed: {i}")

class ConfirmationService:
    def __init__(self):
        self.dag = Memgraph()
        self.redis = Redis()
        self.rabbit = RabbitMQ()
        self.processor = ConfirmationProcessor(self.redis, self.dag)
        self.running = False

    async def run(self) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Confirmation process {os.getpid()} started")
            await self.rabbit.consume_messages("dag", self.processor.process_message)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.dag.connect()
        await self.rabbit.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.dag.close()
        await self.rabbit.close()
        logger.info("All connections closed")