from fastapi import APIRouter, Depends, Request
import random

router = APIRouter()

async def get_pool(request: Request):
    return request.app.state.pool

async def get_home(request: Request):
    return request.app.state.home


@router.get("/pool")
async def pool(pool = Depends(get_pool), home = Depends(get_home)):
    tx_hashes = await pool.zrange("unconfirmed", 0, -1, withscores=False) #-1
    parents = []
    for tx_hash in tx_hashes:
        tx_hash = tx_hash.decode()
        #tx_data = pool.get(tx_hash)
        #if tx_data:
            # Можно добавить дополнительную проверку валидности
        parents.append(tx_hash)
        """if len(parents) == 2:
            break"""
        #print(tx_hash, s)
    return parents  # Возвращаем 2 или ничего
    #return {"mom": parents[0], "dad": parents[1]}