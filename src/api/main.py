# src/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from strawberry.fastapi import GraphQLRouter
from src.api.schema import schema
from src.models.tx import Tx
from src.database.mongodb import MongoDB
from src.database.queues import publish_message
from src.utils.crypto import verify_message
from fastapi.encoders import jsonable_encoder
import json
import base58

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = MongoDB()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/send/")
async def send(tx: Tx):
    db = app.state.db
    try:
        uniq = await db.find_one({"time": tx.uniq})
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

        m = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + tx.msg
        print(f"Message to verify: {m}")  # Отладка
        v = verify_message(tx.credit, tx.sign)
        print(f"Verified signature: {v}")  # Отладка
        if v == m:
            balance = await get_balance(tx.credit, db)
            print(f"Balance for {tx.credit}: {balance}")  # Отладка
            if balance == 0 or balance < tx.amount:
                return {'detail': [{'success': 0, 'msg': f'bad balance'}]}
            print("Publishing to RabbitMQ:", jsonable_encoder(tx))  # Отладка
            await publish_message(json.dumps(jsonable_encoder(tx)))
            return {'detail': [{'success': 1, 'msg': 'ok'}]}
        else:
            return {'detail': [{'success': 0, 'msg': 'bad sign'}]}
    except Exception as e:
        print(f"Error in send: {str(e)}")  # Отладка
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

@app.get("/")
def read_root():
    return {"Hello": "World"}

graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")