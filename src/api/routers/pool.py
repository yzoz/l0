import random

from fastapi import APIRouter, Depends, Request

from src.config import PARENTS, POOL_SIZES

router = APIRouter()


async def get_memgraph(request: Request):
    return request.app.state.graph.driver


@router.get("/pool")
async def pool(memgraph=Depends(get_memgraph)):
    """
    Из пула берутся PARENTS + P старейших транзакций (по t.data.time ASC),
    перемешиваются и возвращаются первые PARENTS (=3). P — «запас» кандидатов
    и зависит от текущего размера графа (POOL_SIZES).
    """
    async with memgraph.session() as session:
        # Получение количества узлов
        result = await session.run("MATCH (t:Transaction) RETURN count(t) as N")
        num_txs = (await result.single())["N"]

        # Определение P в зависимости от размера графа
        P = POOL_SIZES[0][1]
        for threshold, size_p in POOL_SIZES:
            if num_txs > threshold:
                P = size_p

        # Получение PARENTS + P кандидатов, отсортированных по убыванию
        # internal_score (сильнейшие первыми).
        result = await session.run(
            """
            MATCH (t:Transaction)
            RETURN t.hash
            // ORDER BY t.internal_score DESC
            // LIMIT $limit
            """,
            # limit=0,
        )
        parents = [record["t.hash"] async for record in result]

        # Перемешивание и выбор первых PARENTS родителей
        random.shuffle(parents)
        return parents[:PARENTS]
