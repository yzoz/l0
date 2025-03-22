from fastapi import APIRouter, Depends, Request
from src.models.tx import Tx
from src.database.mongodb import MongoDB
from src.utils.crypto import verify_message
from fastapi.encoders import jsonable_encoder
import json
import base58

router = APIRouter()

async def get_db(request: Request):
    return request.app.state.db

async def get_rabbit(request: Request):
    return request.app.state.rabbit

@router.post("/send/")
async def send(
    tx: Tx,
    db: MongoDB = Depends(get_db),
    rabbit = Depends(get_rabbit)
):
    try:
        uniq = await db.find_one({"uniq": tx.uniq})
        if uniq:
            return {'detail': [{'success': 0, 'msg': f'not uniq ({tx.uniq})'}]}
        if tx.credit == tx.debit:
            return {'detail': [{'success': 0, 'msg': 'not for yourself'}]}

        try:
            addr_len = len(base58.b58decode(tx.debit))
            if addr_len != 32:
                return {'detail': [{'success': 0, 'msg': 'bad address size'}]}
        except Exception:
            return {'detail': [{'success': 0, 'msg': 'bad address'}]}

        m = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + (tx.msg or "")
        print(f"Message to verify: {m}")
        v = verify_message(tx.credit, tx.sign)
        print(f"Verified signature: {v}")
        if v == m:
            balance = await get_balance(tx.credit, db)
            print(f"Balance for {tx.credit}: {balance}")
            if balance == 0 or balance < tx.amount:
                return {'detail': [{'success': 0, 'msg': 'bad balance'}]}
            message = json.dumps(jsonable_encoder(tx))
            print("Publishing to RabbitMQ:", message)
            await rabbit.publish_message(message)
            return {'detail': [{'success': 1, 'msg': 'ok'}]}
        else:
            return {'detail': [{'success': 0, 'msg': 'bad sign'}]}
    except Exception as e:
        print(f"Error in send: {str(e)}")
        return {'detail': [{'success': 0, 'msg': str(e)}]}

async def get_balance(addr: str, db: MongoDB):
    income_q = [
        {'$match': {'debit': addr}},
        {'$group': {'_id': None, 'sum': {'$sum': '$amount'}}}
    ]
    outcome_q = [
        {'$match': {'credit': addr}},
        {'$group': {'_id': None, 'sum': {'$sum': '$amount'}}}
    ]
    income_result = await db.aggregate(income_q)
    if income_result:
        income = income_result[0]['sum']
        outcome_result = await db.aggregate(outcome_q)
        outcome = outcome_result[0]['sum'] if outcome_result else 0
        return income - outcome
    return 0
