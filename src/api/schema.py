# src/api/schema.py
from typing import List, Optional

import strawberry

from src.database.mongodb import MongoDB
from src.models.tx import TxSend
from src.utils.profile import get_balance


# Определение типа транзакции для GraphQL
@strawberry.experimental.pydantic.type(model=TxSend, all_fields=True)
class TxType:
    pass


# Функция для получения списка транзакций (оставляем как есть или добавляем, если её нет)
async def get_tx(
    amount: int,
    addr: str,
    msg: Optional[bool],
    time: int,
    skip: int,
    limit: int,
    db: MongoDB,
) -> List[TxType]:
    if amount >= 0:
        amount_direction = "$gt"
    else:
        amount_direction = "$lt"
        amount = -amount

    if time >= 0:
        time_direction = "$gt"
    else:
        time_direction = "$lt"
        time = -time

    q = {
        "amount": {amount_direction: amount},
        "time": {time_direction: time},
    }

    if msg is not None:
        if msg:
            q["msg"] = {"$ne": "null"}
        else:
            q["msg"] = "null"

    if addr:
        q["$or"] = [{"credit": addr}, {"debit": addr}]

    return [
        TxType.from_pydantic(TxSend(**t)) for t in await db.find({"$query": q, "$orderby": {"_id": -1}}, skip, limit)
    ]


# Определение GraphQL схемы
@strawberry.type
class Query:
    @strawberry.field
    async def tx(
        self,
        info,
        msg: Optional[bool] = None,
        addr: str = "",
        amount: int = 0,
        time: int = 0,
        skip: int = 0,
        limit: int = 100,
    ) -> List[TxType]:
        db = info.context["db"]
        return await get_tx(amount, addr, msg, time, skip, limit, db)

    @strawberry.field
    async def balance(self, info, addr: str) -> float:
        db = info.context["db"]
        redis = info.context["redis"]
        return await get_balance(addr, db, redis)


schema = strawberry.Schema(Query)
