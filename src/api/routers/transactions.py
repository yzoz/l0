from fastapi import APIRouter, Depends
from src.models.tx import Tx
from src.database.mongodb import MongoDB
from src.utils.crypto import verify_message
from fastapi.encoders import jsonable_encoder
import json
import base58

router = APIRouter()

# Зависимость для получения db из app.state
async def get_db(app):
    return app.state.db

# Зависимость для получения rabbit из app.state
async def get_rabbit(app):
    return app.state.rabbit

@router.post("/send/")
async def send(tx: Tx, db: MongoDB = Depends(get_db), rabbit=Depends(get_rabbit)):
    try:
        uniq = await db.find_one({"uniq": tx.uniq})
        if uniq:
            return {'detail': [{'success': 0, 'msg': f'not uniq ({tx.uniq})'}]}
        if tx.credit == tx.debit:
            return {'detail': [{'success': 0, 'msg': f'not for yourself'}]}

        try:
            addr_len = len(base58.b58decode(tx.debit))
            if addr_len != 32:
                return {'detail': [{'success': 0, 'msg': f'bad address size'}]}
        except:
            return {'detail': [{'success': 0, 'msg': f'bad address'}]}

        m = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + (tx.msg or "")
        print(f"Message to verify: {m}")
        v = verify_message(tx.credit, tx.sign)
        print(f"Verified signature: {v}")
        if v == m:
            balance = await get_balance(tx.credit, db)
            print(f"Balance for {tx.credit}: {balance}")
            if balance == 0 or balance < tx.amount:
                return {'detail': [{'success': 0, 'msg': f'bad balance'}]}
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
    income_q = [{'$match': {'debit': addr}}, {'$group': {'_id': None, 'sum': {'$sum': '$amount'}}}]
    outcome_q = [{'$match': {'credit': addr}}, {'$group': {'_id': None, 'sum': {'$sum': '$amount'}}}]
    if (income := await db.aggregate(income_q)):
        income = income[0]['sum']
        if (outcome := await db.aggregate(outcome_q)):
            outcome = outcome[0]['sum']
            return income - outcome
        else:
            return income
    else:
        return 0