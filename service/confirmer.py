import logging
from dataclasses import dataclass
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.database.graph import Dag
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
    def __init__(self, db: MongoDB, redis_client: Redis, dag: Dag):
        self.db = db
        self.redis = redis_client
        self.dag = dag
        self.config = Config()

    async def check_pool(self):
        await self.dag.load(self.redis)

        def calculate_indirect_weight(node, depth=0, visited=None):
            if visited is None:
                visited = set()
            if node in visited:
                return 0.0
            visited.add(node)
            weight = 0.0
            for child in self.dag.G.successors(node):
                weight += self.config.DAMPING ** depth
                weight += calculate_indirect_weight(child, depth + 1, visited.copy())
            return weight
        
        for node in self.dag.G.nodes:
            own_weight = 1.0
            indirect_weight = calculate_indirect_weight(node)
            self.dag.G.nodes[node]['weight'] = own_weight + indirect_weight

        await self.dag.save(self.redis, self.dag.G)
        print(len(self.dag.G.nodes))

    async def periodic_check_pool(self):
        while True:
            await self.check_pool()
            await asyncio.sleep(0.1)

class ConfirmationService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = Redis()
        self.dag = Dag()
        self.processor = ConfirmationProcessor(self.db, self.redis, self.dag)
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
        await self.db.connect()
        await self.dag.load(self.redis)

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.db.close()
        logger.info("All connections closed")