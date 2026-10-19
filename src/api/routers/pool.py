import random

from fastapi import APIRouter, Depends, Request

from src.config import PARENTS

router = APIRouter()


async def get_memgraph(request: Request):
    return request.app.state.graph.driver


@router.get("/pool")
async def pool(memgraph=Depends(get_memgraph)):
    async with memgraph.session() as session:
        # Получение PARENTS
        # internal_score (сильнейшие первыми).
        result = await session.run(
            """
            MATCH (t:Transaction)
            RETURN t.hash
            ORDER BY t.internal_score DESC
            // LIMIT $limit
            """,
            # limit=PARENTS,
        )
        parents = [record["t.hash"] async for record in result]

        # Перемешивание и выбор первых PARENTS родителей
        random.shuffle(parents)
        return parents[:PARENTS]
