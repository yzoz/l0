import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.utils.profile import get_balance, get_balance_sync
from keys import pk

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
        
        # Проверка балансов для каждого публичного ключа
        for addr in pk:
            # Получаем баланс из Redis
            redis_balance = await get_balance(addr, mongodb, redis, sync=False)
            
            # Получаем баланс напрямую из MongoDB
            mongo_balance = await get_balance_sync(addr, mongodb)
            
            # Сравниваем балансы
            assert redis_balance == mongo_balance, (
                f"Balance mismatch for address {addr}: "
                f"Redis={redis_balance}, MongoDB={mongo_balance}"
            )
            
            print(f"Address {addr}: Balance={redis_balance} (consistent)")
            
    finally:
        # Закрытие соединений
        await mongodb.close()
        await redis.close()

if __name__ == "__main__":
    pytest.main(["-v", __file__])
