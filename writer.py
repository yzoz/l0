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
from pymongo.errors import DuplicateKeyError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TransactionConfig:
    WEIGHT: int = 25.0

class WriterProcessor:
    def __init__(self, db: MongoDB, redis: Redis, dag: Dag):
        self.db = db
        self.redis = redis
        self.dag = dag
        self.config = TransactionConfig()

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

def run_writer_process(stop_event):
    """Запускает асинхронный цикл для одного процесса"""
    async def async_run():
        service = WriterService()
        try:
            await service.run()
        except Exception as e:
            logger.error(f"Writer process failed: {e}", exc_info=True)
        finally:
            await service.shutdown()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(async_run())
    except KeyboardInterrupt:
        logger.info("Writer process interrupted")
    finally:
        loop.close()

def main():
    NUM_PROCESSES = 5  # Количество процессов
    processes = []
    stop_events = []

    # Создаем процессы
    for i in range(NUM_PROCESSES):
        stop_event = Event()
        process = Process(target=run_writer_process, args=(stop_event,))
        processes.append(process)
        stop_events.append(stop_event)
        process.start()

    logger.info(f"Started {NUM_PROCESSES} writer processes")

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