import redis.asyncio as redis

class Redis:
    def __init__(self):
        self.client = None

    async def connect(self) -> None:
        self.client = redis.Redis(host='localhost', db=0)

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()