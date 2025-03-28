from src.database.mongodb import MongoDB

async def get_balance(addr: str, db: MongoDB) -> int:
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