from fastapi import APIRouter, Depends, Request
import random

router = APIRouter()

async def get_mom(request: Request):
    return request.app.state.mom

async def get_dad(request: Request):
    return request.app.state.dad

async def get_db(request: Request):
    return request.app.state.db

@router.get("/pool")
async def pool(mom = Depends(get_mom), dad = Depends(get_dad), db = Depends(get_db)):
    #mom_keys = mom.keys('*')
    mom_list = []
    cursor = 0
    while True:
        cursor, keys = await mom.scan(cursor=cursor, count=100)
        for key in keys:
            mom_list.append(key.decode('utf-8'))
        if cursor == 0:
            break
    if not mom_list:
        last = await db.find_one({'$query': {}, '$orderby': {'_id': -1}})
        mom_list.append(last['hash'])
    dad_list = []
    cursor = 0
    while True:
        cursor, keys = await dad.scan(cursor=cursor, count=100)
        for key in keys:
            dad_list.append(key.decode('utf-8'))
        if cursor == 0:
            break
    if not dad_list:
        last = await db.find_one({'$query': {}, '$orderby': {'_id': -1}})
        dad_list.append(last['hash'])
    return {"mom": random.choice(mom_list), "dad": random.choice(dad_list)}