import random

from fastapi import APIRouter, Depends, Request

from src.config import POOL_SIZES, PARENTS

router = APIRouter()


async def get_memgraph(request: Request):
    return request.app.state.graph.driver


@router.get("/pool")
async def pool(memgraph=Depends(get_memgraph)):
    """
    Транзакции выбираются из 2 + P узлов, отсортированных по весу, где P зависит от размера графа.
    """
    async with memgraph.session() as session:
        # Получение количества узлов
        result = await session.run("MATCH (t:Transaction) RETURN count(t) as N")
        num_txs = (await result.single())["N"]

        # Определение N и P в зависимости от размера графа
        P = POOL_SIZES[0][2]
        for threshold, _, size_p in POOL_SIZES:
            if num_txs > threshold:
                P = size_p

        # Получение PARENTS + P узлов, отсортированных по весу
        result = await session.run(
            """
            MATCH (t:Transaction)
            RETURN t.hash
            ORDER BY t.data.time ASC
            LIMIT $limit
            """,
            limit=PARENTS + P,
        )
        parents = [record["t.hash"] async for record in result]

        # Перемешивание и выбор первых N родителей
        random.shuffle(parents)
        return parents[:PARENTS]
