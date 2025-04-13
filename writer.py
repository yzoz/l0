import asyncio
import json
import logging
import os
import time
import hashlib
from dataclasses import dataclass
from typing import Optional, List
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite
import redis.asyncio as redis
from multiprocessing import Process, Event
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self.client = await redis.Redis(host='localhost', db=0, decode_responses=True)

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()

class WriterProcessor:
    def __init__(self, db: MongoDB, redis_client: RedisClient, process_id: int, total_processes: int):
        self.db = db
        self.redis = redis_client
        self.process_id = process_id
        self.total_processes = total_processes
        self.written = 0

    def _should_process(self, key: str) -> bool:
        """Определяет, должен ли текущий процесс обрабатывать этот ключ"""
        hash_value = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return (hash_value % self.total_processes) == self.process_id

    async def write_batch(self) -> None:
        # Получаем ключи с префиксом write:*
        keys = await self.redis.client.keys("write:*")
        relevant_keys = [k for k in keys if self._should_process(k)]
        if not relevant_keys:
            return

        BATCH_SIZE = 100
        for batch in [relevant_keys[i:i + BATCH_SIZE] for i in range(0, len(relevant_keys), BATCH_SIZE)]:
            # Получаем данные одним pipeline
            async with self.redis.client.pipeline() as pipe:
                for key in batch:
                    await pipe.get(key)
                data_list = await pipe.execute()

            to_write = []
            tx_hashes = []
            for key, data in zip(batch, data_list):
                if data:
                    tx_hash = key.replace("write:", "")
                    try:
                        tx_dict = json.loads(data)
                        tx_dict["hash"] = tx_hash
                        to_write.append(TxWrite(**tx_dict))
                        tx_hashes.append(key)
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.error(f"Failed to parse data for {tx_hash}: {e}")

            # Батчевая запись в MongoDB
            if to_write:
                success = await self.db.insert_many_tx(to_write)
                self.written += len(to_write)
                logger.info(f"Written {len(to_write)} TXs to MongoDB, total: {self.written}")
                if success:
                    # Удаляем успешно записанные ключи
                    async with self.redis.client.pipeline() as pipe:
                        for key in tx_hashes:
                            await pipe.delete(key)
                            #await pipe.set(f"home:{key}", "1")
                        await pipe.execute()

    async def periodic_write(self):
        while True:
            await self.write_batch()
            await asyncio.sleep(0.1)  # Интервал для уменьшения нагрузки

class WriterService:
    def __init__(self, process_id: int, total_processes: int):
        self.db = MongoDB()
        self.redis = RedisClient()
        self.processor = WriterProcessor(self.db, self.redis, process_id, total_processes)
        self.running = False

    async def run(self) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Writer process {os.getpid()} started")
            await self.processor.periodic_write()
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.db.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.db.close()
        logger.info("All connections closed")

def run_writer_process(process_id: int, total_processes: int, stop_event):
    async def async_run():
        service = WriterService(process_id, total_processes)
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
    NUM_PROCESSES = 5  # Используем число CPU ядер
    processes = []
    stop_events = []

    for i in range(NUM_PROCESSES):
        stop_event = Event()
        process = Process(target=run_writer_process, args=(i, NUM_PROCESSES, stop_event))
        processes.append(process)
        stop_events.append(stop_event)
        process.start()

    logger.info(f"Started {NUM_PROCESSES} writer processes")

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