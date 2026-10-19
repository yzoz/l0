"""Rate limiter для consumer'а — ограничение записи транзакций в Memgraph.

Алгоритм: leaky bucket в форме постоянного межтранзакционного интервала
("constant spacing" / single-slot GCRA без burst-ёмкости).

Почему не fixed window и не token bucket:
  * fixed window допускает boundary burst до 2N на стыке окон (.docs/rate-limit/deep_SPEC §6);
  * token bucket копит токены во время простоя -> всплеск после idle
    (.docs/rate-limit/grok_SPEC: B = 1.5-3x R) и burst на старте.

Цель — держать число узлов графа под контролем (Little's law: nodes ≈ rate ×
время жизни узла), а не снизить нагрузку на Memgraph. Поэтому нужен ровно
противоположный инвариант: НЕ копить пропущенные слоты.

Свойства:
  * первая транзакция (и первая после простоя) допускается немедленно;
  * после простоя слоты НЕ накапливаются (max(now, next_at));
  * равномерный интервал 1/TPS между допусками;
  * asyncio-native: await asyncio.sleep() не блокирует event loop, поэтому
    очередь RabbitMQ `in` естественно растёт (backpressure).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


class RateLimiter:
    """Не более TPS допусков в секунду. Использование: ``await limiter.acquire()``."""

    def __init__(
        self,
        tps: int,
        *,
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        # Fail fast на некорректной конфигурации. bool отвергаем явно: True/False — не TPS.
        if type(tps) is not int or tps <= 0:
            raise ValueError("tps must be a positive integer")

        self.tps = tps
        self._interval = 1.0 / tps
        self._now = now
        self._sleep = sleep
        # Монотонные часы: wall clock запрещён (NTP-скачки ломают расписание).
        self._next_at = now()  # первый acquire — немедленно
        self._lock = asyncio.Lock()

    @property
    def interval(self) -> float:
        return self._interval

    async def acquire(self) -> float:
        """Зарезервировать следующий слот и дождаться его.

        Возвращает время ожидания в секундах (0.0 при немедленном допуске).
        """
        # Резервируем слот под локом (FIFO по порядку захвата), но спим ВНЕ
        # лока — критическая секция минимальна.
        async with self._lock:
            now = self._now()
            at = max(now, self._next_at)  # после простоя не копим слоты
            self._next_at = at + self._interval
            wait = at - now

        if wait > 0:
            await self._sleep(wait)
        return wait
