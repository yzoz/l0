import json
import logging
import time
from dataclasses import dataclass
from typing import Optional
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite
import redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TransactionConfig:
    DEV_ADDRESS: str = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy"
    CONFIRM: int = 3

class RedisClient:
    def __init__(self):
        self.client: Optional[redis.Redis] = None

    def connect(self) -> None:
        self.client = redis.Redis(host='localhost', db=0)

    def close(self) -> None:
        if self.client:
            self.client.close()

class ConfirmationProcessor:
    def __init__(self, db: MongoDB, redis_client: RedisClient):
        self.db = db
        self.redis = redis_client
        self.config = TransactionConfig()

    def confirm_tx(self, tx_hash: str) -> bool:
        home_key = f"home:{tx_hash}"
        refs_key = f"refs:{tx_hash}"
        pool_key = f"pool:{tx_hash}"
        
        if self.redis.client.get(home_key):
            with self.redis.client.pipeline() as pipe:
                pipe.zrem("unconfirmed", tx_hash)
                pipe.delete(pool_key)
                pipe.delete(refs_key)
                pipe.execute()
            return False
        
        refs = int(self.redis.client.get(refs_key) or 0)
        if refs >= self.config.CONFIRM:
            tx_data = self.redis.client.get(pool_key)
            if tx_data:
                confirmed_tx = self.create_tx(tx_hash, tx_data)
                success = self.db.insert_tx(confirmed_tx)  # Предполагается, что insert_tx синхронный
                logger.info(f"Confirmed TX saved: {tx_hash}" if success else f"Failed to save: {tx_hash}")
                if success:
                    with self.redis.client.pipeline() as pipe:
                        pipe.zrem("unconfirmed", tx_hash)
                        pipe.delete(pool_key)
                        pipe.delete(refs_key)
                        pipe.set(home_key, tx_data)
                        pipe.execute()
                return success
        return False

    def create_tx(self, hash: str, data: bytes) -> TxWrite:
        try:
            tx_dict = json.loads(data.decode("utf-8"))
            tx_dict["hash"] = hash
            return TxWrite(**tx_dict)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Failed to parse transaction data for {hash}: {e}")
            raise

    def check_pool(self):
        tx_hashes = self.redis.client.zrange("unconfirmed", 0, -1)
        for tx_hash in tx_hashes:
            self.confirm_tx(tx_hash.decode("utf-8"))

class ConfirmationService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = RedisClient()
        self.processor = ConfirmationProcessor(self.db, self.redis)
        self.running = False

    def run(self) -> None:
        self.running = True
        try:
            self.startup()
            logger.info("Confirmation service started. Checking unconfirmed transactions...")
            while self.running:
                self.processor.check_pool()
                time.sleep(1)  # Проверка каждую секунду
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            self.shutdown()

    def startup(self) -> None:
        self.redis.connect()
        logger.info("Connected to Redis and MongoDB")

    def shutdown(self) -> None:
        self.running = False
        self.redis.close()
        self.db.close()  # Предполагается, что close синхронный
        logger.info("All connections closed")

def main():
    service = ConfirmationService()
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()