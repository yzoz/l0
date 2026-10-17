import asyncio
import json
import logging
import os
import random
import sys
import time
import uuid
from multiprocessing import Process

import httpx

from keys import pk, sk
from src.console.style import fmt_ms
from src.telemetry import metrics, start_dumper
from src.utils.crypto import sign_message
from src.utils.data import ordered_sum
from src.config import DEV_ADDR

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(process)d - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

start = time.time()

# Полные JSON и подписи — только в режиме подробного вывода.
VERBOSE = os.environ.get("L0_VERBOSE") == "1"


async def arange(count):
    """Генератор для асинхронного перебора чисел."""
    for i in range(count):
        yield i
        await asyncio.sleep(0.25)
        # if i % 5 == 0: await asyncio.sleep(round(random.uniform(0, 2.5), 3))
        # if i % 10 == 0: await asyncio.sleep(round(random.uniform(0, 5), 3))
        # await asyncio.sleep(round(random.uniform(0.01, 0.05), 3))
        """await asyncio.sleep(random.randint(0, 30))
        if i % 2 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 3 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 5 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 7 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 10 == 0: await asyncio.sleep(random.randint(0, 30))"""


async def send(t, client):
    # await asyncio.sleep(random.randint(0, 30))
    """Отправка случайных транзакций в асинхронном режиме."""
    async for j in arange(int(t)):
        n = len(sk) - 1
        i = random.randint(0, n)
        if j % 3:
            r = random.choice([29, 28, 27])
            c = pk[r]
            priv = sk[r]
        else:
            c = pk[i]  # credit (отправитель)
            priv = sk[i]  # приватный ключ
        d = pk[random.randint(0, n)]  # debit (получатель)
        a = random.randint(1, 1000000000)  # сумма
        u = str(uuid.uuid4())  # уникальный идентификатор
        m = str(random.randint(0, 100000))  # сообщение
        try:
            # p = await client.get('http://192.168.1.8:8000/pool')
            p = await client.get("http://localhost:8000/pool")
            r = p.json()
            # Формируем транзакцию
            tx = dict(parents=r, debit=d, credit=c, amount=a, uniq=u, msg=m)
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
                logger.info("tx %s -> %s %s", short, r.status_code, fmt_ms(elapsed_ms))
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


def main():
    args = sys.argv[1:]

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
