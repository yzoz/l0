from fastapi import APIRouter, Depends, Request
import random

router = APIRouter()

async def get_pool(request: Request):
    return request.app.state.pool

async def get_parents(r, n = 2) -> list[str]:
    # Берем транзакции с минимальным числом ссылок
    min_score = r.zrange("unconfirmed", 0, 0, withscores=True)[0][1]
    candidates = r.zrangebyscore("unconfirmed", min_score, min_score)
    if len(candidates) < n:
        return []  # Недостаточно транзакций
    parents = random.sample([c.decode() for c in candidates], n)
    return [p for p in parents if r.get(p)]  # Проверяем наличие

@router.get("/pool")
async def pool(pool = Depends(get_pool)):
    N = 2
    tx_hashes = await pool.zrange("unconfirmed", 0, -1, withscores=False) #-1 - все, N до какого
    parents = []
    if len(tx_hashes) > 10: N = 3
    if len(tx_hashes) > 25: N = 5
    if len(tx_hashes) > 100: N = 10
    for tx_hash in tx_hashes:
        tx_hash = tx_hash.decode()
        parents.append(tx_hash)
        if len(parents) == N:
            break
    return parents