from fastapi import APIRouter, Depends, Request
import random

router = APIRouter()

async def get_memgraph(request: Request):
    return request.app.state.graph.driver

@router.get("/pool")
async def pool(memgraph = Depends(get_memgraph)):
    """
    Возвращает список хэшей N неподтверждённых транзакций для использования в качестве родителей.
    Транзакции выбираются из N + P узлов, отсортированных по весу, где N и P зависят от размера графа.
    """
    async with memgraph.session() as session:
        # Получение количества узлов
        result = await session.run("MATCH (t:Transaction) RETURN count(t) as N")
        num_txs = (await result.single())["N"]

        # Определение N и P в зависимости от размера графа
        N = 2 # количество
        P = 0 # с запасом для рандома
        if num_txs > 10:
            N = 3
            P = 4
        if num_txs > 25:
            N = 4
            P = 12
        if num_txs > 50:
            N = 5
            P = 30
        if num_txs > 100:
            N = 6
            P = 50
        if num_txs > 250:
            N = 8
            P = 100
        if num_txs > 500:
            N = 10
            P = 250

        # Получение N + P узлов, отсортированных по весу
        result = await session.run(
            """
            MATCH (t:Transaction)
            RETURN t.hash
            ORDER BY t.weight ASC
            LIMIT $limit
            """,
            limit=N + P
        )
        parents = [record["t.hash"] async for record in result]

        # Перемешивание и выбор первых N родителей
        random.shuffle(parents)
        return parents[:N]
    