from fastapi import APIRouter, Depends, Request
import random

router = APIRouter()

async def get_pool(request: Request):
    return request.app.state.pool

async def get_home(request: Request):
    return request.app.state.home

@router.get("/pool")
async def pool(pool = Depends(get_pool), home = Depends(get_home)):
    #mom_keys = mom.keys('*')
    pool_list = []
    cursor = 0
    while True:
        cursor, keys = await pool.scan(cursor=cursor, count=100)
        for key in keys:
            pool_list.append(key.decode('utf-8'))
        if cursor == 0:
            break
    if len(pool_list) < 3:
        while True:
            cursor, keys = await home.scan(cursor=cursor, count=100)
            for key in keys:
                pool_list.append(key.decode('utf-8'))
            if cursor == 0:
                break
    while True:
        mom = random.randint(0, len(pool_list) - 1)
        dad = random.randint(0, len(pool_list) - 1)
        bro = random.randint(0, len(pool_list) - 1)
        variables = [mom, dad, bro]
        if len(set(variables)) == len(variables): break
    return {"mom": pool_list[mom], "dad": pool_list[dad], "bro": pool_list[bro]}