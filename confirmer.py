import json
import logging
from dataclasses import dataclass
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite
from src.database.redis import Redis
from src.utils.graph import Dag
from multiprocessing import Process, Event
import signal
import sys
import os
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TransactionConfig:
    DAMPING: int = 0.5

class ConfirmationProcessor:
    def __init__(self, db: MongoDB, redis_client: Redis, dag: Dag):
        self.db = db
        self.redis = redis_client
        self.dag = dag
        self.config = TransactionConfig()

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
        '''sorted_nodes = sorted(
            self.dag.G.nodes(data=True),
            key=lambda x: x[1]['weight']
        )
        print("-" * 100)
        print(sorted_nodes)'''

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

def run_confirmation_process(stop_event):
    """Запускает асинхронный цикл для одного процесса"""
    async def async_run():
        service = ConfirmationService()
        try:
            await service.run()
        except Exception as e:
            logger.error(f"Confirmation process failed: {e}", exc_info=True)
        finally:
            await service.shutdown()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(async_run())
    except KeyboardInterrupt:
        logger.info("Confirmation process interrupted")
    finally:
        loop.close()

def main():
    NUM_PROCESSES = 5  # Количество процессов
    processes = []
    stop_events = []

    # Создаем процессы
    for i in range(NUM_PROCESSES):
        stop_event = Event()
        process = Process(target=run_confirmation_process, args=(stop_event,))
        processes.append(process)
        stop_events.append(stop_event)
        process.start()

    logger.info(f"Started {NUM_PROCESSES} confirmation processes")

    # Обработка сигналов для graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping processes...")
        for stop_event in stop_events:
            stop_event.set()
        for process in processes:
            process.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        for process in processes:
            process.join()
    except Exception as e:
        logger.error(f"Main process error: {e}")
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()

if __name__ == "__main__":
    main()