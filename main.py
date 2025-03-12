import strawberry
from typing import List, Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from strawberry.fastapi import GraphQLRouter
from src.models import Tx

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@strawberry.experimental.pydantic.type(model=Tx, all_fields=True)
class TxType:
    pass

async def send_tx(tx: Tx) -> str:
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
