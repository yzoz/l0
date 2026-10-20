import asyncio
import json
import logging
import os
import random
import sys
import threading
import time
import uuid
from enum import IntEnum
from typing import AsyncIterator
from multiprocessing import Process

import httpx
from watchfiles import watch

from keys import pk, sk
from src.console.style import fmt_ms
from src.telemetry import metrics, start_dumper
from src.utils.crypto import sign_message
from src.utils.data import ordered_sum
from src.config import DEV_ADDR

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(process)d - %(levelname)s - %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("httpcore").setLevel(logging.ERROR)

start = time.time()

# Полные JSON и подписи — только в режиме подробного вывода.
VERBOSE = os.environ.get("L0_VERBOSE") == "1"

class Mode(IntEnum):
    NORMAL = 0
    DROP   = 1
    SPIKE  = 2
    WAVE   = 3
    ATTACK = 4

# целевые λ
LAMBDA = {
    Mode.NORMAL: 2.5,
    Mode.DROP:   1.0,
    Mode.SPIKE:  5.0,
    Mode.WAVE:  10.0,
    Mode.ATTACK:25.0,
}

# средние длительности (сек) — середины твоих диапазонов
MEAN_DUR = {
    Mode.NORMAL: 210.0,   # 3.5 мин
    Mode.DROP:   120.0,   # 2 мин
    Mode.SPIKE:   60.0,   # 1 мин
    Mode.WAVE:    30.0,   # ~28 с
    Mode.ATTACK:  10.0,   # 10 с
}
# матрица интенсивностей переходов (rate matrix, только off-diagonal)
# строки/столбцы в порядке Mode
# подобрана так, чтобы π ≈ [0.64, 0.20, 0.10, 0.05, 0.01]
Q = [
    # from NORMAL
    [0.0,  0.0018, 0.0012, 0.0005, 0.00015],
    # from DROP
    [0.006, 0.0,   0.0015, 0.0004, 0.0001],
    # from SPIKE
    [0.012, 0.003,  0.0,   0.002,  0.0005],
    # from WAVE
    [0.025, 0.005,  0.008,  0.0,   0.002],
    # from ATTACK
    [0.06,  0.01,   0.02,   0.01,   0.0],
]

RAMP_SEC = 2.0   # длительность плавного перехода λ

async def arange2(count: int) -> AsyncIterator[tuple[int, Mode]]:
    MEAN_DUR = {
        Mode.NORMAL: 15.0,
        Mode.DROP:    7.5,
        Mode.SPIKE:   3.25,
        Mode.WAVE:    1.625,
        Mode.ATTACK:  0.8125,
    }
    state = Mode.NORMAL
    cur_λ = LAMBDA[state]
    target_λ = cur_λ
    ramp_left = 0.0
    # время до выхода из текущего режима
    sojourn_left = random.expovariate(1.0 / MEAN_DUR[state])

    # вероятности перехода (куда идём, когда выходим)
    # строки нормализованы, сумма = 1
    P = [
        # from NORMAL → DROP, SPIKE, WAVE, ATTACK
        [0.50, 0.30, 0.15, 0.05],
        # from DROP
        [0.70, 0.20, 0.08, 0.02],
        # from SPIKE
        [0.55, 0.15, 0.20, 0.10],
        # from WAVE
        [0.50, 0.15, 0.20, 0.15],
        # from ATTACK
        [0.45, 0.15, 0.25, 0.15],
    ]

    for i in range(count):
        if sojourn_left <= 0:
            # выбираем новый state
            probs = P[state]
            r = random.random()
            acc = 0.0
            for j, p in enumerate(probs):
                acc += p
                if r <= acc:
                    # j+1 потому что 0-й элемент P — это первый возможный target
                    targets = [Mode.DROP, Mode.SPIKE, Mode.WAVE, Mode.ATTACK] if state == Mode.NORMAL else \
                              [Mode.NORMAL, Mode.SPIKE, Mode.WAVE, Mode.ATTACK] if state == Mode.DROP else \
                              [Mode.NORMAL, Mode.DROP, Mode.WAVE, Mode.ATTACK] if state == Mode.SPIKE else \
                              [Mode.NORMAL, Mode.DROP, Mode.SPIKE, Mode.ATTACK] if state == Mode.WAVE else \
                              [Mode.NORMAL, Mode.DROP, Mode.SPIKE, Mode.WAVE]
                    state = targets[j]
                    break

            target_λ = LAMBDA[state]
            ramp_left = RAMP_SEC
            sojourn_left = random.expovariate(1.0 / MEAN_DUR[state])

        # λ
        if ramp_left > 0:
            progress = 1.0 - (ramp_left / RAMP_SEC)
            λ = cur_λ + (target_λ - cur_λ) * progress
        else:
            λ = target_λ
            cur_λ = target_λ

        delay = random.expovariate(λ)
        await asyncio.sleep(delay)

        sojourn_left -= delay
        if ramp_left > 0:
            ramp_left = max(0.0, ramp_left - delay)
            if ramp_left == 0:
                cur_λ = target_λ

        yield i, state


async def arange(count: int) -> AsyncIterator[tuple[int, Mode]]:
    state = Mode.NORMAL
    cur_λ = LAMBDA[state]
    target_λ = cur_λ
    ramp_left = 0.0          # сколько секунд ещё идёт ramp
    next_switch = random.expovariate(sum(Q[state]))  # время до следующего скачка режима

    for i in range(count):
        # 1. решаем, не пора ли сменить режим
        if next_switch <= 0:
            # выбираем новый state пропорционально Q[state]
            rates = Q[state]
            total = sum(rates)
            r = random.random() * total
            acc = 0.0
            for new_s, rate in enumerate(rates):
                acc += rate
                if r <= acc:
                    state = Mode(new_s)
                    break
            target_λ = LAMBDA[state]
            ramp_left = RAMP_SEC
            next_switch = random.expovariate(sum(Q[state]))

        # 2. текущая λ (lerp если ramp)
        if ramp_left > 0:
            # линейная интерполяция
            progress = 1.0 - (ramp_left / RAMP_SEC)
            λ = cur_λ + (target_λ - cur_λ) * progress
        else:
            λ = target_λ
            cur_λ = target_λ

        # 3. Poisson delay
        delay = random.expovariate(λ)
        await asyncio.sleep(delay)

        # 4. тикаем таймеры
        next_switch -= delay
        if ramp_left > 0:
            ramp_left = max(0.0, ramp_left - delay)
            if ramp_left == 0:
                cur_λ = target_λ

        yield i, state


async def send(t, client):
    # await asyncio.sleep(random.randint(0, 30))
    """Отправка случайных транзакций в асинхронном режиме."""
    async for j, state in arange2(int(t)):
        n = len(sk) - 1
        i = random.randint(0, n)
        if j % 3:
            r = random.choice([29, 28, 27])
            c = pk[r]
            priv = sk[r]
        else:
            c = pk[i]  # credit (отправитель)
            priv = sk[i]  # приватный ключ
        a = random.randint(1, 1_000) #сумма
        if j % 5 == 0:
            a = random.randint(1_000, 5_000)
        if j % 25 == 0:
            a = random.randint(5_000, 25_000)
        if j % 100 == 0:
            a = random.randint(25_000, 100_000)
        d = pk[random.randint(0, n)]  # debit (получатель)
        u = str(uuid.uuid4())  # уникальный идентификатор
        m = str(random.randint(0, 100000))  # сообщение
        try:
            # p = await client.get('http://192.168.1.8:8000/pool')
            r = await client.get("http://localhost:8000/pool")
            p = r.json()
            # Формируем транзакцию
            tx = dict(parents=p, debit=d, credit=c, amount=a, uniq=u, msg=m)
            m = ordered_sum(tx)  # строка для подписи
            # Подпись транзакции
            s = sign_message(priv, m)
            tx_data = {"content": tx, "sign": s}
            if VERBOSE:
                logger.info(f"Sending transaction: {json.dumps(tx_data, indent=2)}")
            # r = await client.post('http://192.168.1.8:8000/send/', data=json.dumps(tx_data))
            t0 = time.perf_counter()
            r = await client.post("http://localhost:8000/send/", data=json.dumps(tx_data))
            elapsed_ms = (time.perf_counter() - t0) * 1000
            metrics.inc("sent")
            metrics.observe("latency_ms", elapsed_ms)
            short = u[:8]
            sp = ""
            for _p in p:
                sp += " " + _p[:8]
            if r.status_code < 400:
                metrics.inc("ok")
                metrics.event("ok", "send", f"{short} {r.status_code} {fmt_ms(elapsed_ms)}", ref=u, key=f"ok:{u}")
            else:
                metrics.inc("err")
                metrics.event(
                    "warn" if r.status_code < 500 else "error",
                    "send",
                    f"{short} {r.status_code} {fmt_ms(elapsed_ms)}",
                    ref=u,
                    key=f"err:{u}",
                )
            if VERBOSE:
                logger.info(f"Response: {r.text}")
            else:
                logger.info("%s ~ %s (%s) [%s] -> %s %s", state.name, short, a, sp, r.status_code, fmt_ms(elapsed_ms))
        except Exception as e:
            metrics.inc("err")
            metrics.event("error", "send", f"{u[:8]} запрос не удался: {type(e).__name__}", ref=u, key=f"exc:{u}")
            logger.error(f"Error in send: {e}")


async def run_sender(t, c):
    """Запуск C параллельных корутин в одном процессе."""
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[send(t, client) for _ in range(c)])  # 250


def process_target(t, c):
    """Функция-цель для каждого процесса."""
    metrics.set_role(f"tx-w{os.getpid()}")
    metrics.set("coroutines", c)
    metrics.set("param_t", t)
    metrics.set("param_c", c)
    dumper = start_dumper()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_sender(t, c))
    except KeyboardInterrupt:
        logger.info("Process interrupted")
    finally:
        try:
            metrics.dump()
        except OSError:
            pass
        loop.close()
        if hasattr(dumper, "cancel"):
            dumper.cancel()


def reload_on_self_change():
    """Перезапуск процесса при изменении самого tx_bot.py (режим --watch)."""
    self_path = os.path.abspath(__file__)
    for _ in watch(os.path.dirname(self_path), recursive=False, watch_filter=lambda _c, p: os.path.abspath(p) == self_path):
        logger.info("Detected change in %s, restarting...", self_path)
        os.execv(sys.executable, [sys.executable, *sys.argv])


def main():
    watch_mode = "--watch" in sys.argv[1:]
    args = [a for a in sys.argv[1:] if a != "--watch"]

    try:
        t = int(args[0])
    except IndexError:
        t = 1_000_000
    try:
        c = int(args[1])
    except IndexError:
        c = 1
    try:
        n = int(args[2])
    except IndexError:
        n = 1

    """Основная функция для запуска многопроцессного генератора транзакций."""
    if watch_mode:
        # Dev-режим: один процесс без multiprocessing, вотчер в потоке.
        threading.Thread(target=process_target, args=(t, c), daemon=True).start()
        reload_on_self_change()
        return

    num_processes = n  # Количество процессов (можно настроить)

    metrics.set_role("tx-main")
    metrics.set("processes", num_processes)
    metrics.set("coroutines", c)
    metrics.set("param_t", t)
    metrics.set("param_c", c)
    metrics.set("param_n", n)
    dumper = start_dumper()

    processes = []
    for _ in range(num_processes):
        p = Process(target=process_target, args=(t, c))
        processes.append(p)
        p.start()

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Main process interrupted, terminating all processes...")
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
        sys.exit(0)
    finally:
        try:
            metrics.dump()
        except OSError:
            pass
        if hasattr(dumper, "cancel"):
            dumper.cancel()


if __name__ == "__main__":
    main()
    print(int(time.time() - start))
