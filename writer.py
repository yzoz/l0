import json
import logging
from dataclasses import dataclass
from typing import Optional
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite
import redis.asyncio as redis
from multiprocessing import Process, Event
import signal
import sys
import os
import asyncio
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TransactionConfig:
    CONFIRM: int = 3

class RedisClient:
    def __init__(self):
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self.client = await redis.Redis(host='localhost', db=0)

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()

class ConfirmationProcessor:
    def __init__(self, db: MongoDB, redis_client: RedisClient):
        self.db = db
        self.redis = redis_client
        self.config = TransactionConfig()

    async def confirm_tx(self, tx_hash: str) -> bool:
        lock_key = f"lock:{tx_hash}"
        # Пытаемся установить блокировку с TTL 5 секунд
        locked = await self.redis.client.set(lock_key, "1", nx=True, ex=5)
        if not locked:
            return False  # Транзакция уже обрабатывается другим процессом

        try:
            home_key = f"home:{tx_hash}"
            refs_key = f"refs:{tx_hash}"
            pool_key = f"pool:{tx_hash}"

            if await self.redis.client.get(home_key):
                async with self.redis.client.pipeline() as pipe:
                    await pipe.zrem("unconfirmed", tx_hash)
                    await pipe.delete(pool_key)
                    await pipe.delete(refs_key)
                    await pipe.execute()
                return False

            refs = int(await self.redis.client.get(refs_key) or 0)
            if refs >= self.config.CONFIRM:
                tx_data = await self.redis.client.get(pool_key)
                if tx_data:
                    confirmed_tx = self.create_tx(tx_hash, tx_data)
                    success = await self.db.insert_tx(confirmed_tx)  # Предполагается асинхронный метод
                    logger.info(f"Confirmed TX saved: {tx_hash}" if success else f"Failed to save: {tx_hash}")
                    if success:
                        async with self.redis.client.pipeline() as pipe:
                            await pipe.zrem("unconfirmed", tx_hash)
                            await pipe.delete(pool_key)
                            await pipe.delete(refs_key)
                            await pipe.set(home_key, tx_data)
                            await pipe.execute()
                    return success
            return False
        finally:
            await self.redis.client.delete(lock_key)  # Снимаем блокировку

    def create_tx(self, hash: str, data: bytes) -> TxWrite:
        try:
            tx_dict = json.loads(data.decode("utf-8"))
            tx_dict["hash"] = hash
            return TxWrite(**tx_dict)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Failed to parse transaction data for {hash}: {e}")
            raise

    async def check_pool(self):
        tx_hashes = await self.redis.client.zrange("unconfirmed", 0, -1)
        for tx_hash in tx_hashes:
            await self.confirm_tx(tx_hash.decode("utf-8"))

    async def periodic_check_pool(self):
        while True:
            await self.check_pool()
            await asyncio.sleep(round(random.uniform(0, 0.5), 2))  # Проверка каждую секунду

class ConfirmationService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = RedisClient()
        self.processor = ConfirmationProcessor(self.db, self.redis)
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
        logger.info("Connected to Redis and MongoDB")

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.db.close()
        logger.info("All connections closed")

def run_confirmation_process(stop_event: Event):
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
    NUM_PROCESSES = 10  # Количество процессов
    processes = []
    stop_events = []

    # Создаем процессы
    for _ in range(NUM_PROCESSES):
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