from src.database.mongodb import MongoDB
from src.database.redis import Redis
import asyncio

async def get_balance_sync(addr: str, db: MongoDB):
    """Функция для получения баланса по адресу."""
    pipeline = [
        {"$facet": {
            "income": [
                {"$match": {"debit": addr}},  # Входящие транзакции (где адрес получатель)
                {"$group": {"_id": None, "sum": {"$sum": "$amount"}}}
            ],
            "outcome": [
                {"$match": {"credit": addr}},  # Исходящие транзакции (где адрес отправитель)
                {"$group": {"_id": None, "sum": {"$sum": "$amount"}}}
            ]
        }}
    ]
    result = await db.aggregate(pipeline)
    income = result[0]["income"][0]["sum"] if result[0]["income"] else 0
    outcome = result[0]["outcome"][0]["sum"] if result[0]["outcome"] else 0
    return income - outcome

async def get_balance(addr: str, db: MongoDB, redis: Redis, sync = False) -> int:
    if sync:
        return await asyncio.wait_for(get_balance_sync(addr, db), timeout=1) or 0
    else:
        """Получает баланс из Redis или MongoDB"""
        balance_key = f"balance:{addr}"
        try:
            balance = await asyncio.wait_for(redis.client.get(balance_key), timeout=1)
            if balance is None:
                balance = await asyncio.wait_for(get_balance_sync(addr, db), timeout=1) or 0
                await redis.client.set(balance_key, balance)
            return int(balance)
        except asyncio.TimeoutError:
            return 0