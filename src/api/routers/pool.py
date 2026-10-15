import random

from fastapi import APIRouter, Depends, Request

from src.config import POOL_SIZES

router = APIRouter()


async def get_memgraph(request: Request):
    return request.app.state.graph.driver


@router.get("/pool")
async def pool(memgraph=Depends(get_memgraph)):
    """
    Возвращает список хэшей N неподтверждённых транзакций для использования в качестве родителей.
    Транзакции выбираются из N + P узлов, отсортированных по весу, где N и P зависят от размера графа.
    """
    async with memgraph.session() as session:
        # Получение количества узлов
        result = await session.run("MATCH (t:Transaction) RETURN count(t) as N")
        num_txs = (await result.single())["N"]

        # Определение N и P в зависимости от размера графа
        N, P = POOL_SIZES[0][1], POOL_SIZES[0][2]
        for threshold, size_n, size_p in POOL_SIZES:
            if num_txs > threshold:
                N, P = size_n, size_p

        # Получение N + P узлов, отсортированных по весу
        result = await session.run(
            """
            MATCH (t:Transaction)
            RETURN t.hash
            ORDER BY t.weight ASC
            LIMIT $limit
            """,
            limit=N + P,
        )
        parents = [record["t.hash"] async for record in result]

        # Перемешивание и выбор первых N родителей
        random.shuffle(parents)
        return parents[:N]
