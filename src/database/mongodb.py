from motor.motor_asyncio import AsyncIOMotorClient
from src.models.tx import TxWrite
from typing import List
#from pymongo import WriteConcern
import logging
#from pymongo import AsyncMongoClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MongoDB:
    def __init__(self):
        self.client = None
        self.db = None
        self.tx = None

    async def connect(self):
        self.client = AsyncIOMotorClient(
            'localhost',
            27017,
            #maxPoolSize=10000,
            #minPoolSize=1000,
            #maxIdleTimeMS=30000,
            #socketTimeoutMS=2000,
            #connectTimeoutMS=2000
        )
        self.db = self.client['feo']
        self.tx = self.db.tx
        # Создаем индекс на поле hash
        await self.tx.create_index("hash", unique=True)
        await self.tx.create_index([("credit", 1), ("uniq", 1)])

    async def insert_tx(self, tx: TxWrite):
        #result = await self.tx.with_options(write_concern=WriteConcern(w=0)).insert_one(dict(tx))
        result = await self.tx.insert_one(dict(tx))
        return result.acknowledged
    
    async def insert_many_tx(self, txs: List[TxWrite]) -> bool:
        if not txs:
            return True
        result = await self.tx.insert_many([dict(tx) for tx in txs])
        return result.acknowledged

    async def find_one(self, query: dict):
        return await self.tx.find_one(query)

    async def aggregate(self, pipeline: list):
        result = await self.tx.aggregate(pipeline).to_list(1)
        return result if result else None  # Упрощаем возврат результата

    async def find(self, query: dict, skip: int, limit: int):
        return await self.tx.find(query).skip(skip).limit(limit).to_list(None)

    async def close(self):
        if self.client:
            self.client.close()