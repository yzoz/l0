import strawberry
import motor.motor_asyncio
import json
from typing import Optional, List
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from strawberry.fastapi import GraphQLRouter
from nacl.signing import VerifyKey
import base58
from aio_pika import connect_robust, Message, DeliveryMode
from src.models import Tx

@asynccontextmanager
async def lifespan(app: FastAPI):
    global mq
    connection = await connect_robust("amqp://guest:guest@localhost/")
    mq = await connection.channel()
    await mq.declare_queue("tx", durable=True)
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cli = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)

db = cli.feo
txs = db.tx

async def get_balance(addr):
    income_q = [{
        '$match': {
            'debit': addr
        }}, {
        '$group': {
            '_id': None, 
            'sum': {
                '$sum': '$amount'
            }
        }
    }]
    outcome_q = [{
        '$match': {
            'credit': addr
        }}, {
        '$group': {
            '_id': None, 
            'sum': {
                '$sum': '$amount'
            }
        }
    }]
    if (income := await txs.aggregate(income_q).to_list(1)):
        print(f'in: {income}')
        income = income[0]['sum']
        if (outcome := await txs.aggregate(outcome_q).to_list(1)):
            print(outcome)
            outcome = outcome[0]['sum']
            return income - outcome
        else:
            return income
    else:
        return 0

@strawberry.experimental.pydantic.type(model=Tx, all_fields=True)
class TxType:
    pass

@app.post("/send/")
async def send(tx: Tx):
    try:
        uniq = await txs.find_one({"time": tx.uniq})
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
        print(tx)
        verify_key = VerifyKey(base58.b58decode(tx.credit))
        v = verify_key.verify(base58.b58decode(tx.sign)).decode("utf-8")
        print(m, v)
        if v == m:
            balance = await get_balance(tx.credit)
            if balance == 0 or balance < tx.amount:
                return {'detail': [{'success': 0, 'msg': f'bad balance'}]}
            await mq.default_exchange.publish(Message(json.dumps(jsonable_encoder(tx)).encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT),routing_key="tx")
            return {'detail': [{'success': 1, 'msg': 'ok'}]}
        else:
            return {'detail': [{'success': 0, 'msg': 'bad sign'}]}
        
    except Exception as e:
        return {'detail': [{'success': 0, 'msg': str(e)}]}

async def get_tx(amount, addr, msg, time, skip, limit) -> List[TxType]:
    if amount >= 0: amount_direction = '$gt'
    else: amount_direction = '$lt'; amount = -amount
    
    if time >= 0: time_direction = '$gt'
    else: time_direction = '$lt'; time = -amount

    q = {
        'amount': {amount_direction: amount},
        'time': {time_direction: time},
    }

    if msg != None:
        if msg == True: q['msg'] = {'$ne': 'null'}
        else: q['msg'] = 'null'

    if addr:
        q['$or'] = [{'credit': addr}, {'debit': addr}]
    
    print(q)

    return [
        TxType.from_pydantic(Tx(**t)) for t in await txs.find({
            '$query': q,
            '$orderby': {'_id': -1}
            })
            .skip(skip)
            .limit(limit)
            .to_list(None)
    ]
@strawberry.type
class Query:
    @strawberry.field
    async def getTx(
        self,
        msg: bool = None,
        addr: str = '',
        amount: int = 0,
        time: int = 0,
        skip: int = 0,
        limit: int = 100
    ) -> List[TxType]:
        return await get_tx(amount, addr, msg, time, skip, limit)


@app.get("/")
def read_root():
    return {"Hello": "World"}

schema = strawberry.Schema(Query)
graphql_app = GraphQLRouter(schema)#graphiql=False
app.include_router(graphql_app, prefix="/graphql")
