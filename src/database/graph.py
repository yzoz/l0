from neo4j import AsyncGraphDatabase


class Memgraph:
    def __init__(self, uri: str = "bolt://localhost:7687"):
        self.uri = uri
        self.driver = None

    async def connect(self) -> None:
        self.driver = AsyncGraphDatabase.driver(self.uri)

    async def close(self) -> None:
        if self.driver:
            await self.driver.close()
            self.driver = None
