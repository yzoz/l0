from fastapi import APIRouter, Depends, Request
import random

router = APIRouter()

async def get_pool(request: Request):
    return request.app.state.pool

@router.get("/pool")
async def pool(pool = Depends(get_pool)):
    N = 2
    P = 0
    tx_hashes = await pool.zrange("unconfirmed", 0, -1, withscores=False) #-1 - все, N до какого
    parents = []
    if len(tx_hashes) > 10: N = 3; P = 4
    if len(tx_hashes) > 25: N = 4; P = 12
    if len(tx_hashes) > 50: N = 5; P = 30
    if len(tx_hashes) > 100: N = 6; P = 50
    if len(tx_hashes) > 250: N = 8; P = 100
    if len(tx_hashes) > 500: N = 10; P = 250
    for tx_hash in tx_hashes:
        tx_hash = tx_hash.decode()
        parents.append(tx_hash)
        if len(parents) == N + P:
            break
    random.shuffle(parents)
    return parents[:N]