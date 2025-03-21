from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from src.models.tx import Tx

class MongoDB:
    def __init__(self, host='localhost', port=27017, db_name='feo'):
        self.client = MongoClient(host, port)
        self.async_client = AsyncIOMotorClient(host, port)
        self.db = self.client[db_name]
        self.async_db = self.async_client[db_name]
        self.txs = self.db.tx
        self.async_txs = self.async_db.tx

    async def insert_tx(self, tx: Tx):
        result = await self.async_txs.insert_one(tx.dict())  # Используем tx.dict()
        return result.inserted_id

    async def find_one(self, query: dict):
        return await self.async_txs.find_one(query)

    async def aggregate(self, pipeline: list):
        result = await self.async_txs.aggregate(pipeline).to_list(1)
        return result if result else None  # Упрощаем возврат результата

    async def find(self, query: dict, skip: int, limit: int):
        return await self.async_txs.find(query).skip(skip).limit(limit).to_list(None)

    async def close(self):
        self.client.close()
        self.async_client.close()