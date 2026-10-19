"""
Rate Limiter Specification for Consumer (Memgraph write protection)

Два алгоритма:
1. TokenBucketRateLimiter  — с контролируемым бурстом + jitter
2. LeakyBucketRateLimiter  — строго гладкий outflow (constant rate)

Общие требования (чтобы не было хуйни):
- Только monotonic clock (time.monotonic_ns)
- float64 для токенов / уровня
- Токен/слот берётся ТОЛЬКО перед успешной записью в Memgraph
- При неудачной записи токен НЕ возвращается
- Легко переключаться через фабрику
- Полностью детерминировано при фиксированном clock (для тестов)
- Jitter добавлен в ожидание, чтобы не было синхронных "гребёнок" при нескольких consumer'ах в будущем
"""

from __future__ import annotations

import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


# ====================== ОБЩИЙ ИНТЕРФЕЙС ======================

class RateLimiter(ABC):
    """
    Единый интерфейс.
    acquire() блокирует до момента, когда можно писать в Memgraph.
    Возвращает время ожидания в секундах (для метрик).
    """

    @abstractmethod
    def acquire(self) -> float:
        """Блокирует до разрешения. Возвращает seconds waited."""
        ...

    @abstractmethod
    def try_acquire(self) -> bool:
        """Неблокирующая попытка. True = можно писать прямо сейчас."""
        ...

    @abstractmethod
    def tokens_available(self) -> float:
        """Текущий уровень (для метрик)."""
        ...


# ====================== TOKEN BUCKET ======================

@dataclass
class TokenBucketConfig:
    rate: float          # R — целевой TPS
    burst: float         # B — максимальный бурст (ёмкость)
    min_wait_ms: float = 1.0      # минимальный sleep при ожидании
    max_wait_ms: float = 5.0      # максимальный sleep
    jitter_ratio: float = 0.3     # ±30% джиттера к sleep


class TokenBucketRateLimiter(RateLimiter):
    """
    Классический Token Bucket с непрерывным пополнением.

    Поведение:
    - Короткие бурсты до `burst` проходят мгновенно
    - После исчерпания — равномерная скорость ≈ rate
    - Jitter в ожидании защищает от синхронных пиков
    """

    def __init__(self, config: TokenBucketConfig, clock=None):
        if config.rate <= 0 or config.burst < 1:
            raise ValueError("rate > 0 and burst >= 1 required")

        self.cfg = config
        self._clock = clock or time.monotonic_ns   # только monotonic!
        self._tokens: float = float(config.burst)
        self._last_refill_ns: int = self._clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed_ns = now - self._last_refill_ns
        if elapsed_ns <= 0:
            return

        # непрерывно добавляем токены
        self._tokens += (elapsed_ns / 1_000_000_000) * self.cfg.rate
        if self._tokens > self.cfg.burst:
            self._tokens = self.cfg.burst

        self._last_refill_ns = now

    def try_acquire(self) -> bool:
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def acquire(self) -> float:
        """Блокирующее ожидание с jitter."""
        start = self._clock()
        while True:
            if self.try_acquire():
                waited_ns = self._clock() - start
                return waited_ns / 1_000_000_000

            # считаем, сколько примерно нужно подождать
            deficit = 1.0 - self._tokens
            wait_sec = deficit / self.cfg.rate

            # ограничиваем и добавляем jitter
            wait_ms = max(self.cfg.min_wait_ms,
                          min(self.cfg.max_wait_ms, wait_sec * 1000))

            # jitter: ± jitter_ratio
            jitter = 1.0 + random.uniform(-self.cfg.jitter_ratio, self.cfg.jitter_ratio)
            sleep_sec = (wait_ms * jitter) / 1000.0

            time.sleep(max(0.0005, sleep_sec))  # минимум 0.5ms

    def tokens_available(self) -> float:
        self._refill()
        return self._tokens


# ====================== LEAKY BUCKET (чистый constant rate) ======================

@dataclass
class LeakyBucketConfig:
    rate: float                  # строго constant TPS
    min_wait_ms: float = 1.0
    max_wait_ms: float = 5.0
    jitter_ratio: float = 0.2    # чуть меньше джиттера, т.к. и так гладко


class LeakyBucketRateLimiter(RateLimiter):
    """
    Чистый Leaky Bucket / Constant Rate.

    Никаких бурстов вообще.
    Транзакции выходят строго со скоростью ≤ rate.
    Самый безопасный вариант для Memgraph.
    """

    def __init__(self, config: LeakyBucketConfig, clock=None):
        if config.rate <= 0:
            raise ValueError("rate > 0 required")

        self.cfg = config
        self._clock = clock or time.monotonic_ns
        self._next_allowed_ns: int = self._clock()   # когда можно выдать следующий слот

    def try_acquire(self) -> bool:
        now = self._clock()
        if now >= self._next_allowed_ns:
            # выдаём слот и сдвигаем следующий
            self._next_allowed_ns = now + int(1_000_000_000 / self.cfg.rate)
            return True
        return False

    def acquire(self) -> float:
        start = self._clock()
        while True:
            now = self._clock()
            if now >= self._next_allowed_ns:
                self._next_allowed_ns = now + int(1_000_000_000 / self.cfg.rate)
                waited_ns = now - start
                return waited_ns / 1_000_000_000

            # сколько осталось ждать
            remaining_ns = self._next_allowed_ns - now
            wait_ms = remaining_ns / 1_000_000

            wait_ms = max(self.cfg.min_wait_ms,
                          min(self.cfg.max_wait_ms, wait_ms))

            jitter = 1.0 + random.uniform(-self.cfg.jitter_ratio, self.cfg.jitter_ratio)
            sleep_sec = (wait_ms * jitter) / 1000.0

            time.sleep(max(0.0005, sleep_sec))

    def tokens_available(self) -> float:
        """Для leaky bucket возвращаем "сколько можно взять прямо сейчас" (0 или 1)."""
        now = self._clock()
        if now >= self._next_allowed_ns:
            return 1.0
        return 0.0


# ====================== ФАБРИКА (лёгкое переключение) ======================

def create_rate_limiter(
    algorithm: str,
    rate: float,
    burst: float = 1.0,
    **kwargs
) -> RateLimiter:
    """
    algorithm: "token" | "leaky"
    """
    algorithm = algorithm.lower().strip()

    if algorithm in ("token", "token_bucket", "tb"):
        cfg = TokenBucketConfig(rate=rate, burst=burst, **kwargs)
        return TokenBucketRateLimiter(cfg)

    if algorithm in ("leaky", "leaky_bucket", "lb", "constant"):
        cfg = LeakyBucketConfig(rate=rate, **kwargs)
        return LeakyBucketRateLimiter(cfg)

    raise ValueError(f"Unknown algorithm: {algorithm}. Use 'token' or 'leaky'")


# ====================== ПРИМЕР ИСПОЛЬЗОВАНИЯ В CONSUMER ======================

"""
# В consumer'е (псевдокод):

limiter = create_rate_limiter(
    algorithm="token",          # или "leaky"
    rate=250.0,                 # RATE_LIMIT_TPS
    burst=600.0,                # RATE_LIMIT_BURST
)

def process_user_tx(msg):
    # ... все проверки (подпись, хэш, валидация и т.д.) ...

    # Только здесь, прямо перед записью в Memgraph:
    waited = limiter.acquire()          # блокирует если нужно

    success = write_to_memgraph(tx)
    if not success:
        # токен уже потрачен — специально НЕ возвращаем
        raise WriteError(...)

    # ack сообщения в Rabbit только после успешной записи
"""