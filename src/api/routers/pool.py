from fastapi import APIRouter, Depends, Request
import random
from src.database.graph import Dag

router = APIRouter()
dag = Dag()

async def get_redis(request: Request):
    return request.app.state.redis

@router.get("/pool")
async def pool(redis = Depends(get_redis)):
    await dag.load(redis)
    N = 2
    P = 0
    G = dag.G
    sorted_nodes = sorted(
        G.nodes(data=True),
        key=lambda x: x[1]['weight']
    )
    parents = []
    #num_parents = max(2, math.floor(math.sqrt(num_txs)))
    if len(G.nodes) > 10: N = 3; P = 4
    if len(G.nodes) > 25: N = 4; P = 12
    if len(G.nodes) > 50: N = 5; P = 30
    if len(G.nodes) > 100: N = 6; P = 50
    if len(G.nodes) > 250: N = 8; P = 100
    if len(G.nodes) > 500: N = 10; P = 250
    for node, _ in sorted_nodes:
        parents.append(node)
        if len(parents) == N + P:
            break
    random.shuffle(parents)
    return parents[:N]