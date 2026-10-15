import asyncio

from src.config import BALANCE_TIMEOUT
from src.database.mongodb import MongoDB
from src.database.redis import Redis


async def get_balance_sync(addr: str, db: MongoDB):
    """Считает баланс адреса напрямую по коллекции подтверждённых транзакций.

    Конвенция credit/debit: ``credit`` — плательщик (отправитель средств),
    ``debit`` — получатель. Поэтому приход адреса — это транзакции, где он
    ``debit``, а расход — где он ``credit``.
    """
    pipeline = [
        {
            "$facet": {
                "income": [
                    {"$match": {"debit": addr}},  # Входящие транзакции (где адрес получатель)
                    {"$group": {"_id": None, "sum": {"$sum": "$amount"}}},
                ],
                "outcome": [
                    {"$match": {"credit": addr}},  # Исходящие транзакции (где адрес отправитель)
                    {"$group": {"_id": None, "sum": {"$sum": "$amount"}}},
                ],
            }
        }
    ]
    result = await db.aggregate(pipeline)
    income = result[0]["income"][0]["sum"] if result[0]["income"] else 0
    outcome = result[0]["outcome"][0]["sum"] if result[0]["outcome"] else 0
    return income - outcome


async def get_balance(addr: str, db: MongoDB, redis: Redis, sync=False) -> int:
    """Возвращает баланс адреса, по возможности из кэша Redis.

    ``credit`` — плательщик, ``debit`` — получатель (см. ``get_balance_sync``).
    """
    if sync:
        return await asyncio.wait_for(get_balance_sync(addr, db), timeout=BALANCE_TIMEOUT) or 0
    else:
        """Получает баланс из Redis или MongoDB"""
        balance_key = f"balance:{addr}"
        try:
            balance = await asyncio.wait_for(redis.client.get(balance_key), timeout=BALANCE_TIMEOUT)
            if balance is None:
                balance = await asyncio.wait_for(get_balance_sync(addr, db), timeout=BALANCE_TIMEOUT) or 0
                await redis.client.set(balance_key, balance)
            return int(balance)
        except asyncio.TimeoutError:
            return 0
