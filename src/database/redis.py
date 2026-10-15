import redis.asyncio as redis
from redis.asyncio import Redis as RedisClient


class Redis:
    def __init__(self):
        self.client: RedisClient | None = None

    async def connect(self) -> None:
        self.client = redis.Redis(host="localhost", db=0, decode_responses=True)

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None
