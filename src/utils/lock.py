import asyncio
import uuid
from redis.asyncio import Redis
from contextlib import asynccontextmanager

class RedisLock:
    def __init__(self, redis: Redis, lock_key: str, timeout: int = 10, retry_delay: float = 0.1):
        """
        Инициализация Redis Lock.
        
        Args:
            redis: Асинхронный клиент Redis.
            lock_key: Ключ блокировки (например, 'lock:resource:123').
            timeout: Время жизни блокировки в секундах.
            retry_delay: Задержка между попытками захвата блокировки.
        """
        self.redis = redis
        self.lock_key = lock_key
        self.timeout = timeout
        self.retry_delay = retry_delay
        self.lock_value = str(uuid.uuid4())  # Уникальное значение для блокировки

    async def acquire(self) -> bool:
        """
        Попытка захватить блокировку.
        
        Returns:
            bool: True, если блокировка захвачена, False иначе.
        """
        # Пытаемся установить ключ с TTL
        result = await self.redis.set(
            self.lock_key,
            self.lock_value,
            nx=True,  # Set if not exists
            px=int(self.timeout * 1000)  # TTL в миллисекундах
        )
        return result is True

    async def release(self) -> bool:
        """
        Освобождение блокировки.
        
        Returns:
            bool: True, если блокировка освобождена, False если ключ не принадлежит этому процессу.
        """
        # Атомарно проверяем и удаляем ключ
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            result = await self.redis.eval(script, 1, self.lock_key, self.lock_value)
            return result == 1
        except Exception:
            return False

    @asynccontextmanager
    async def lock(self, retries: int = 5):
        """
        Контекстный менеджер для захвата и автоматического освобождения блокировки.
        
        Args:
            retries: Количество попыток захвата блокировки.
        
        Raises:
            TimeoutError: Если не удалось захватить блокировку после всех попыток.
        """
        acquired = False
        for _ in range(retries):
            if await self.acquire():
                acquired = True
                break
            await asyncio.sleep(self.retry_delay)
        
        if not acquired:
            raise TimeoutError(f"Failed to acquire lock {self.lock_key} after {retries} retries")
        
        try:
            yield
        finally:
            if acquired:
                await self.release()

# Пример использования
async def process_transaction(transaction_id: str, redis: Redis):
    lock = RedisLock(redis, f"lock:transaction:{transaction_id}")
    async with lock.lock():
        # Критическая секция: обработка транзакции
        print(f"Processing transaction {transaction_id}")
        await asyncio.sleep(1)  # Симуляция работы
        print(f"Finished transaction {transaction_id}")

# Тестирование
async def main():
    redis = Redis(host='localhost', port=6379, decode_responses=False)
    try:
        # Запускаем несколько корутин для обработки одной транзакции
        await asyncio.gather(
            process_transaction("tx123", redis),
            process_transaction("tx123", redis),
            process_transaction("tx123", redis)
        )
    finally:
        await redis.close()

if __name__ == "__main__":
    asyncio.run(main())

"""
async def process_transaction(transaction_id: str, redis: Redis):
    lock = redis.lock(f"lock:transaction:{transaction_id}", timeout=10, blocking_timeout=0)
    acquired = await lock.acquire()  # Одна попытка захвата
    if not acquired:
        print(f"Failed to acquire lock for {transaction_id}")
        return
    
    try:
        print(f"Processing transaction {transaction_id}")
        await asyncio.sleep(1)  # Симуляция работы
    finally:
        if acquired:
            await lock.release()

async def process_transaction(transaction_id: str, redis: Redis):
    lock = redis.lock(f"lock:transaction:{transaction_id}", timeout=10)
    acquired = await lock.acquire(blocking=False)  # Одна попытка без ожидания
    if not acquired:
        print(f"Failed to acquire lock for {transaction_id}")
        return
    
    try:
        print(f"Processing transaction {transaction_id}")
        await asyncio.sleep(1)
    finally:
        if acquired:
            await lock.release()

"""