import asyncio

from src.config import DEV_ADDR
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.utils.profile import get_balance, get_balance_sync


async def main():
    # Инициализация клиентов
    mongodb = MongoDB()
    redis = Redis()

    try:
        # Подключение к базам
        await mongodb.connect()
        await redis.connect()

        # Проверка балансов для каждого адреса из генезис-набора
        for addr in DEV_ADDR:
            # Получаем баланс из Redis
            redis_balance = await get_balance(addr, mongodb, redis, sync=False)

            # Получаем баланс напрямую из MongoDB
            mongo_balance = await get_balance_sync(addr, mongodb)

            print(f"{addr}: Redis={redis_balance}, MongoDB={mongo_balance} | {redis_balance == mongo_balance}")

    finally:
        # Закрытие соединений
        await mongodb.close()
        await redis.close()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
