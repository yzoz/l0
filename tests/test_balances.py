import pytest

from src.config import DEV_ADDR
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.utils.profile import get_balance, get_balance_sync


@pytest.mark.integration
@pytest.mark.asyncio
async def test_balance_consistency():
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

            # Сравниваем балансы
            assert redis_balance == mongo_balance, (
                f"Balance mismatch for address {addr}: Redis={redis_balance}, MongoDB={mongo_balance}"
            )

            print(f"Address {addr}: Balance={redis_balance} (consistent)")

    finally:
        # Закрытие соединений
        await mongodb.close()
        await redis.close()


if __name__ == "__main__":
    pytest.main(["-v", __file__])
