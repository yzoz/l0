"""Тесты RateLimiter на детерминированных часах."""

from __future__ import annotations

import asyncio

import pytest

from service.ratelimit import RateLimiter


class FakeClock:
    """Управляемые монотонные часы: sleep продвигает время, не спит реально."""

    def __init__(self, t0: float = 1000.0) -> None:
        self.t = t0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, delay: float) -> None:
        assert delay >= 0
        self.sleeps.append(delay)
        self.t += delay


def test_invalid_config():
    for tps in (0, -1, True, 1.5, "10", None):
        with pytest.raises(ValueError):
            RateLimiter(tps)


def test_first_acquire_immediate():
    clk = FakeClock()
    lim = RateLimiter(100, now=clk.now, sleep=clk.sleep)
    assert asyncio.run(lim.acquire()) == 0.0  # TPS=100 -> интервал 10мс
    assert clk.sleeps == []


def test_regular_spacing():
    clk = FakeClock()
    lim = RateLimiter(100, now=clk.now, sleep=clk.sleep)

    async def run():
        return [await lim.acquire() for _ in range(4)]

    waits = asyncio.run(run())
    assert waits[0] == 0.0
    assert waits[1:] == [pytest.approx(0.01)] * 3
    assert clk.t - 1000.0 == pytest.approx(0.03)


def test_no_catch_up_after_idle():
    """После длинного простоя пропущенные слоты НЕ накапливаются."""
    clk = FakeClock()
    lim = RateLimiter(100, now=clk.now, sleep=clk.sleep)
    asyncio.run(lim.acquire())  # t=1000, next=1000.01

    clk.t += 3600.0  # час простоя

    assert asyncio.run(lim.acquire()) == 0.0  # немедленно, никакого буста
    assert clk.sleeps == []


def test_tps_1_is_one_second():
    clk = FakeClock()
    lim = RateLimiter(1, now=clk.now, sleep=clk.sleep)

    async def run():
        return [await lim.acquire() for _ in range(3)]

    waits = asyncio.run(run())
    assert waits == [0.0, pytest.approx(1.0), pytest.approx(1.0)]


def test_boundary_no_double_rate_at_window_edge():
    """На стыке «окон» нет всплеска: интервал соблюдается всегда.

    Ключевое отличие от fixed window: 2N на границе невозможно.
    """
    clk = FakeClock()
    lim = RateLimiter(10, now=clk.now, sleep=clk.sleep)
    stamps = []

    async def run():
        for _ in range(25):
            await lim.acquire()
            stamps.append(clk.now())

    asyncio.run(run())
    gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False)]
    assert all(g >= 0.1 - 1e-9 for g in gaps), gaps
    # ровно 25 допусков за (25-1) интервалов, не больше
    assert stamps[-1] - stamps[0] == pytest.approx(24 * 0.1)


def test_concurrent_fifo_and_no_overrate():
    """M корутин одновременно: суммарно ровно один допуск на интервал."""
    clk = FakeClock()
    lim = RateLimiter(100, now=clk.now, sleep=clk.sleep)
    order: list[int] = []

    async def worker(i: int):
        await lim.acquire()
        order.append(i)

    async def run():
        await asyncio.gather(*(worker(i) for i in range(20)))

    asyncio.run(run())
    assert order == list(range(20))  # FIFO по захвату лока


def test_live_rate_close_to_n():
    """Реальное время: TPS=50 за 2 секунды -> ~100 допусков (не 2N на границе)."""

    async def run():
        lim = RateLimiter(50)
        t0 = asyncio.get_running_loop().time()
        count = 0
        while asyncio.get_running_loop().time() - t0 < 2.0:
            await lim.acquire()
            count += 1
        return count

    count = asyncio.run(run())
    assert 95 <= count <= 105, count
