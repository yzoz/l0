# src/api/schema.py
import strawberry
from typing import List, Optional
from src.models.tx import Tx
from src.database.mongodb import MongoDB
from src.utils.balance import get_balance  # Импортируем функцию

"""@strawberry.experimental.pydantic.input(model=Tx, all_fields=True)
class TxType:
    pass
    
TxType.from_pydantic(Tx(**t))
"""
# Определение типа транзакции для GraphQL
@strawberry.type
class TxType:
    _id: str
    credit: str
    debit: str
    asset: str
    amount: float
    uniq: str
    sign: str
    msg: str
    v: int
    time: float
    hash: str

# Функция для получения списка транзакций (оставляем как есть или добавляем, если её нет)
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
        time = -time

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
        TxType(**t) for t in await db.find(
            {'$query': q, '$orderby': {'_id': -1}}, skip, limit
        )
    ]

# Определение GraphQL схемы
@strawberry.type
class Query:
    @strawberry.field
    async def tx(
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

    @strawberry.field
    async def balance(self, addr: str) -> float:
        db = MongoDB()
        return await get_balance(addr, db)

schema = strawberry.Schema(Query)