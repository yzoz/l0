"""
Rate limiter для consumer'а: ограничение впуска транзакций в Memgraph.

Спецификация (см. ТЗ, §1–§11):

  - Алгоритм: fixed window, длина окна ровно 1 секунда.
  - Семантика: block (backpressure внутри процесса). Транзакции не теряются:
    worker спит на condition variable, пока permits == 0, и не поллит брокер.
  - Порядок в worker'е: acquire() -> poll брокера -> обработка.
  - Один экземпляр лимитера на процесс, общий для пула worker'ов.
  - Источник времени: монотонные часы (time.monotonic). Wall clock запрещён.
  - N читается один раз при старте из конфига, hot reload не поддерживается.
  - Мультиинстанс-координация отсутствует: каждый процесс независим,
    кластерный потолок = N * K (K — число инстансов).
  - Boundary burst до 2N на стыке окон — принят осознанно.
  - Refund permit'а при ошибке записи — не делается (permit сгорает).
  - Дедупликация по хешу, per-client квоты, приоритеты, anti-starvation —
    вне скоупа, не реализовывать.

Вне скоупа (не добавлять!):
  Redis, распределённые счётчики, лидер-выборщик, token bucket, GCRA,
  hot reload, observation-only режим, refund, дедуп, квоты, приоритеты.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Конфигурация (§7)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RateLimiterConfig:
    """
    Конфиг лимитера.

    N — целевой темп впуска, tx/s. Подбирается вручную один раз по формуле
    Little's law: N = C_max / W_avg, где C_max — желаемый потолок числа узлов
    в Memgraph, W_avg — среднее время жизни узла (определяется writer'ом/TTL,
    вне нашего кода).

    Валидация: N > 0, иначе ошибка старта (см. __post_init__).
    Hot reload не поддерживается: N фиксируется на всё время жизни процесса.
    """
    N: int

    def __post_init__(self) -> None:
        if not isinstance(self.N, int) or self.N <= 0:
            raise ValueError(f"RateLimiterConfig.N must be positive int, got {self.N!r}")


# ---------------------------------------------------------------------------
# Тикер: фоновый поток, сбрасывающий окно (§5.2)
# ---------------------------------------------------------------------------

class _WindowTicker:
    """
    Фоновый поток, который раз в tick_interval проверяет, не пора ли сбросить
    окно. При переходе в новое окно ставит permits = N и будит всех спящих.

    Свойства:
      - Окна идут строго последовательно от монотонного времени:
        [t0, t0+1), [t0+1, t0+2), ...
      - Если тикер проспал несколько окон (планировщик ОС, GIL, пауза GC),
        пропущенные окна НЕ накапливаются: permits = N, а не N*k.
        window_start сдвигается на целое число окон вперёд.
      - notify_all, чтобы разбудить всех worker'ов для перепроверки условия.
    """

    def __init__(
        self,
        state: "_LimiterState",
        tick_interval: float = 0.05,
    ) -> None:
        # tick_interval = 50 мс — компромисс между точностью границы окна
        # и накладными расходами. Спецификация допускает 50–100 мс.
        self._state = state
        self._tick_interval = tick_interval
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="rate-limiter-ticker",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2.0)

    def _run(self) -> None:
        # §5.2: тикер не завершается, пока живёт лимитер.
        while not self._stop.wait(self._tick_interval):
            now = time.monotonic()
            st = self._state

            with st.mtx:
                elapsed = now - st.window_start
                if elapsed < st.window_seconds:
                    continue

                # §5.2: пропущенные окна не накапливаются.
                # k — целое число окон, на которое надо сдвинуться.
                k = int(elapsed // st.window_seconds)
                st.window_start += k * st.window_seconds
                st.permits = st.N

                # §5.2: notify_all — разбудить всех спящих для перепроверки.
                st.cv.notify_all()

                # §8: метрики. Конкретный бэкенд (Prometheus/OTel) — по стеку.
                # Здесь только вызовы; реализация экспортёра вне скоупа.
                _metrics.window_reset()


# ---------------------------------------------------------------------------
# Внутреннее состояние лимитера (§4)
# ---------------------------------------------------------------------------

@dataclass
class _LimiterState:
    N: int
    permits: int
    window_start: float          # монотонное время начала текущего окна
    window_seconds: float = 1.0  # §5.1: длина окна ровно 1 секунда
    mtx: threading.Lock = field(default_factory=threading.Lock)
    # Condition связан с mtx: wait() атомарно отпускает mtx и засыпает.
    cv: threading.Condition = field(init=False)

    def __post_init__(self) -> None:
        # Condition должен использовать тот же lock, что и остальная защита.
        self.cv = threading.Condition(self.mtx)


# ---------------------------------------------------------------------------
# Публичный API лимитера (§5.3, §5.4)
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    In-process fixed-window rate limiter с block-семантикой.

    Использование worker'ом:

        limiter.acquire()                 # блокирует, если permits == 0
        msg = broker.poll(timeout=...)
        if msg is None:
            limiter.release_if_unused()   # вернуть permit, продолжить
            continue
        try:
            process(msg)
            broker.ack(msg)
        except Exception:
            broker.nack(msg)              # permit НЕ возвращается
    """

    def __init__(self, cfg: RateLimiterConfig) -> None:
        self._state = _LimiterState(
            N=cfg.N,
            permits=cfg.N,
            window_start=time.monotonic(),  # §4: старт окна от монотонных часов
        )
        self._ticker = _WindowTicker(self._state, tick_interval=0.05)

        # §7: лог при старте.
        log.info("rate_limiter_initialized N=%d", cfg.N)

    def start(self) -> None:
        """Запустить тикер. Вызывается один раз при инициализации consumer'а."""
        self._ticker.start()

    def stop(self) -> None:
        """Остановить тикер. Вызывается при graceful shutdown consumer'а."""
        self._ticker.stop()

    # -----------------------------------------------------------------
    # §5.3: блокирующий acquire
    # -----------------------------------------------------------------
    def acquire(self) -> None:
        """
        Дождаться разрешения и списать один permit.

        При permits == 0 worker спит на condvar: это и есть backpressure —
        брокерская очередь растёт, ничего не теряется, ничего не ack'ается.

        Никаких таймаутов ожидания. Никаких «если долго ждали — пустить
        всё равно». Ждём до появления permit'а. Spurious wakeups
        обрабатываются циклом while.
        """
        st = self._state
        wait_started_at = time.monotonic()

        with st.mtx:
            while st.permits == 0:
                # §8: delayed_total — факт блокировки.
                _metrics.delayed_inc()
                st.cv.wait()

            st.permits -= 1
            # §8: tokens_available — текущее значение permits.
            _metrics.set_tokens_available(st.permits)

        # §8: acquired_total.
        _metrics.acquired_inc()
        # §8: wait_seconds — гистограмма времени в acquire (включая 0).
        _metrics.observe_wait_seconds(time.monotonic() - wait_started_at)

    # -----------------------------------------------------------------
    # §5.4: возврат неиспользованного permit'а
    # -----------------------------------------------------------------
    def release_if_unused(self) -> None:
        """
        Вернуть permit, если worker сходил в брокер и не получил сообщение.

        Это НЕ refund после ошибки записи. Это возврат «неиспользованного
        авансом взятого» permit'а, когда poll вернул пусто. Если сообщение
        получено — permit сгорает, независимо от исхода обработки.
        """
        st = self._state
        with st.mtx:
            st.permits += 1
            # notify_one: вернулся ровно один permit — будим одного спящего.
            st.cv.notify_one()
            _metrics.set_tokens_available(st.permits)


# ---------------------------------------------------------------------------
# Метрики (§8) — интерфейс. Реализация экспортёра вне скоупа этой задачи.
# ---------------------------------------------------------------------------

class _Metrics:
    """
    Обязательные метрики лимитера (§8). Все — per-process.

      rate_limiter_acquired_total    counter   успешных acquire()
      rate_limiter_rejected_total    counter   всегда 0 в block-режиме;
                                                оставлено для будущего режима
      rate_limiter_delayed_total     counter   сколько раз acquire() уходил
                                                в wait()
      rate_limiter_wait_seconds      histogram время, проведённое в acquire()
      rate_limiter_tokens_available  gauge     текущее permits в окне
      rate_limiter_current_rate      gauge     текущее N
    """

    def acquired_inc(self) -> None: ...
    def rejected_inc(self) -> None: ...  # всегда 0 в block-режиме
    def delayed_inc(self) -> None: ...
    def observe_wait_seconds(self, seconds: float) -> None: ...
    def set_tokens_available(self, n: int) -> None: ...
    def set_current_rate(self, n: int) -> None: ...
    def window_reset(self) -> None: ...


# Заглушка. В реальном коде — заменить на экспортёр проекта
# (Prometheus client / OTel meter / что принято в стеке).
_metrics = _Metrics()


# ---------------------------------------------------------------------------
# Пример использования в worker'е (§5.5)
# ---------------------------------------------------------------------------

def worker_loop(
    limiter: RateLimiter,
    broker,               # объект с .poll(timeout) -> msg | None, .ack, .nack
    process,              # функция process(msg) -> None; пишет в Memgraph
    poll_timeout: float = 0.2,
) -> None:
    """
    Цикл worker'а consumer'а.

    Порядок строго: acquire -> poll -> (release_if_unused | process).

    §5.5:
      - acquire() блокирует, если permits == 0; брокер не поллится.
      - poll вернул пусто -> release_if_unused() -> continue.
      - сообщение получено -> permit не возвращается, независимо от исхода
        обработки (успех/ошибка записи в Memgraph).
    """
    while True:
        limiter.acquire()

        msg = broker.poll(timeout=poll_timeout)
        if msg is None:
            limiter.release_if_unused()
            continue

        try:
            process(msg)
            broker.ack(msg)
        except Exception:
            # §5.5: permit НЕ возвращается. Refund при ошибке записи —
            # сознательно вне скоупа (см. §2 ТЗ).
            broker.nack(msg)


# ---------------------------------------------------------------------------
# Тесты (§10) — перечисление обязательного минимума.
#
# Реализация тестов — в отдельном файле. Здесь только фиксируем, что именно
# должно быть покрыто, чтобы агент-исполнитель не пропустил.
# ---------------------------------------------------------------------------
#
# 1. test_throughput: N=100, 10 секунд, успешных acquire() ≈ 1000
#    (проверять на длинном интервале, не на границе окна — boundary burst
#    до 2N допустим).
# 2. test_block_semantics: при permits=0 acquire() блокируется,
#    не возвращается до сброса окна.
# 3. test_window_reset: после window_start + 1.0s permits=N,
#    все спящие просыпаются.
# 4. test_release_if_unused: acquire -> release_if_unused -> permits
#    вернулся; следующий acquire не блокируется.
# 5. test_permit_burns: acquire -> обработка (успех или ошибка) ->
#    permit НЕ вернулся.
# 6. test_skipped_windows: если тикер проспал 2 окна, permits = N,
#    а не 2N.
# 7. test_concurrency: M worker'ов одновременно в acquire(),
#    суммарно за окно не более N успешных.
# 8. test_config_validation: N <= 0 -> ValueError при старте.
# 9. test_monotonic_clock: скачок системного времени не влияет
#    на окна (тест на уровне абстракции времени / мок time.monotonic).
#
# Критерии готовности (§11):
#   - единая точка входа перед poll'ом брокера;
#   - fixed window, 1 секунда, монотонные часы;
#   - block-семантика: worker спит на condvar при исчерпании;
#   - release_if_unused при пустом poll'е;
#   - N из конфига, читается один раз, валидируется;
#   - все 6 метрик экспортируются;
#   - все 9 тестов зелёные;
#   - в README зафиксировано: boundary burst до 2N допустим;
#     кластерный потолок = N*K; refund/дедуп/приоритеты/per-client квоты/
#     hot reload — сознательно вне скоупа.
#
# Если агент захочет «улучшить» (token bucket, hot reload, Redis, refund) —
# это нарушение ТЗ, а не инициатива.