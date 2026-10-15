import asyncio
import uuid
from contextlib import asynccontextmanager

from redis.asyncio import Redis


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
            px=int(self.timeout * 1000),  # TTL в миллисекундах
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
