# src/api/schema.py
import strawberry
from typing import List, Optional
from src.models.tx import Tx
from src.database.mongodb import MongoDB

@strawberry.experimental.pydantic.type(model=Tx, all_fields=True)
class TxType:
    pass

async def get_tx(amount: int, addr: str, msg: Optional[bool], time: int, skip: int, limit: int, db: MongoDB) -> List[TxType]:
    if amount >= 0:
        amount_direction = '$gt'
    else:
        amount_direction = '$lt'
        amount = -amount

    if time >= 0:
        time_direction = '$gt'
    else:
        time_direction = '$lt'
        time = -amount

    q = {
        'amount': {amount_direction: amount},
        'time': {time_direction: time},
    }

    if msg is not None:
        if msg:
            q['msg'] = {'$ne': 'null'}
        else:
            q['msg'] = 'null'

    if addr:
        q['$or'] = [{'credit': addr}, {'debit': addr}]

    return [
        TxType.from_pydantic(Tx(**t)) for t in await db.find(
            {'$query': q, '$orderby': {'_id': -1}}, skip, limit
        )
    ]

@strawberry.type
class Query:
    @strawberry.field
    async def getTx(
            self,
            msg: Optional[bool] = None,
            addr: str = '',
            amount: int = 0,
            time: int = 0,
            skip: int = 0,
            limit: int = 100
    ) -> List[TxType]:
        db = MongoDB()
        return await get_tx(amount, addr, msg, time, skip, limit, db)

schema = strawberry.Schema(Query)