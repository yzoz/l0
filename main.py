import strawberry
import motor.motor_asyncio
from typing import Optional
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
    queue = await mq.declare_queue("tx", durable=True)
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
        #print(f'in: {income}')
        income = income[0]['sum']
        if (outcome := await txs.aggregate(outcome_q).to_list(1)):
            #print(outcome)
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
        uniq = await txs.find_one({"time": tx.time})
        if uniq:
            return {'detail': [{'success': 0, 'msg': f'not uniq ({tx.time})'}]}
        if tx.credit == tx.debit:
            return {'detail': [{'success': 0, 'msg': f'not for yourself'}]}

        try:
            addr_len = len(HexEncoder.decode(tx.debit))
            if addr_len != 32:
                return {'detail': [{'success': 0, 'msg': f'bad address size'}]}
        except:
            return {'detail': [{'success': 0, 'msg': f'bad address'}]}

        m = tx.credit + tx.debit + str(tx.amount) + str(tx.msg) + str(tx.time)

        verify_key = VerifyKey(base58.b58decode(tx.credit))
        v = verify_key.verify(base58.b58decode(tx.sign)).decode("utf-8")
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

async def send_tx(tx: Tx) -> str:
    #-->DB
    print(jsonable_encoder(tx))
    return "ok"

@strawberry.type
class Query:
    #@strawberry.field(graphql_type=TxType)
    @strawberry.field()
    async def sendTx(
        self,
        credit: str,
        debit: str,
        amount: float,
        time: int,
        sign: str,
        msg: Optional[str] = ''
    ) -> str:
        return await send_tx(Tx(credit=credit, debit=debit, amount=amount, time=time, sign=sign, msg=msg))


@app.get("/")
def read_root():
    return {"Hello": "World"}

schema = strawberry.Schema(Query)
graphql_app = GraphQLRouter(schema)#graphiql=False
app.include_router(graphql_app, prefix="/graphql")
