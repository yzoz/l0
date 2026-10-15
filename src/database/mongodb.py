# from pymongo import WriteConcern
import logging
from typing import List

from pymongo import AsyncMongoClient

from src.models.tx import TxWrite

logger = logging.getLogger(__name__)


class MongoDB:
    def __init__(self):
        self.client = None
        self.db = None
        self.tx = None

    async def connect(self):
        self.client = AsyncMongoClient(
            "mongodb://localhost:27017",
            # maxPoolSize=10000,
            # minPoolSize=1000,
            # maxIdleTimeMS=30000,
            # socketTimeoutMS=2000,
            # connectTimeoutMS=2000
        )
        self.db = self.client["feo"]
        self.tx = self.db.tx

    async def insert_tx(self, tx: TxWrite) -> bool:
        """Вставляет одну транзакцию в коллекцию tx"""
        try:
            # Опция WriteConcern(w=0) для неподтверждённой записи (если нужно)
            # result = await self.tx.with_options(write_concern=WriteConcern(w=0)).insert_one(dict(tx))
            result = await self.tx.insert_one(dict(tx))
            logger.debug(f"Inserted transaction with _id: {result.inserted_id}")
            return result.acknowledged
        except Exception:
            # logger.error(f"Failed to insert transaction: {e}")
            raise

    async def insert_many_tx(self, txs: List[TxWrite]) -> bool:
        """Вставляет несколько транзакций в коллекцию tx"""
        if not txs:
            logger.debug("No transactions to insert")
            return True
        try:
            result = await self.tx.insert_many([dict(tx) for tx in txs])
            logger.debug(f"Inserted {len(result.inserted_ids)} transactions")
            return result.acknowledged
        except Exception as e:
            logger.error(f"Failed to insert many transactions: {e}")
            raise

    async def find_one(self, query: dict):
        """Ищет одну транзакцию по запросу"""
        try:
            result = await self.tx.find_one(query)
            logger.debug(f"Find one query: {query}, result: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to find one: {e}")
            raise

    async def aggregate(self, pipeline: list):
        """Выполняет агрегацию по пайплайну и возвращает все документы."""
        try:
            cursor = await self.tx.aggregate(pipeline)
            results = [doc async for doc in cursor]
            logger.debug(f"Aggregate pipeline: {pipeline}, result: {results}")
            return results if results else None
        except Exception as e:
            logger.error(f"Failed to aggregate: {e}")
            raise

    async def find(self, query: dict, skip: int, limit: int):
        """Ищет транзакции с пропуском и лимитом"""
        try:
            cursor = self.tx.find(query).skip(skip).limit(limit)
            result = await cursor.to_list(length=None)
            logger.debug(f"Find query: {query}, skip: {skip}, limit: {limit}, results: {len(result)}")
            return result
        except Exception as e:
            logger.error(f"Failed to find: {e}")
            raise

    async def close(self):
        """Закрывает соединение с MongoDB"""
        if self.client:
            await self.client.close()
            logger.info("MongoDB connection closed")
            self.client = None
            self.db = None
            self.tx = None
