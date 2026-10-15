"""Честные проби состояния зависимостей L0 для панели «Система».

UP — сервис ответил на лёгкую проверку; DOWN — соединение отклонено;
UNKNOWN — таймаут или иная неопределённость. Запуск Docker не считается
здоровьем — проверяется именно отклик сервиса.
"""

from __future__ import annotations

import asyncio
import time

UP = "UP"
DOWN = "DOWN"
UNKNOWN = "UNKNOWN"

TIMEOUT = 0.7

# name, host, port, kind ("tcp" | "http")
PROBES = (
    ("API", "127.0.0.1", 8000, "http"),
    ("RabbitMQ", "127.0.0.1", 5672, "tcp"),
    ("MongoDB", "127.0.0.1", 27017, "tcp"),
    ("Redis", "127.0.0.1", 6379, "tcp"),
    ("Memgraph", "127.0.0.1", 7687, "tcp"),
)


async def probe_one(name: str, host: str, port: int, kind: str = "tcp", timeout: float = TIMEOUT) -> str:
    """Возвращает UP / DOWN / UNKNOWN для одного сервиса."""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
    except (ConnectionRefusedError, FileNotFoundError):
        return DOWN
    except asyncio.TimeoutError:
        return UNKNOWN
    except OSError:
        return UNKNOWN

    status = UP
    try:
        if kind == "http":
            writer.write(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
            await asyncio.wait_for(writer.drain(), timeout=timeout)
            data = await asyncio.wait_for(reader.read(64), timeout=timeout)
            if b"200" not in data.split(b"\r\n", 1)[0]:
                status = UNKNOWN
    except (asyncio.TimeoutError, OSError):
        status = UNKNOWN
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except OSError:
            pass
    return status


async def probe_all(timeout: float = TIMEOUT) -> dict[str, str]:
    results = await asyncio.gather(*(probe_one(n, h, p, k, timeout) for n, h, p, k in PROBES))
    return {name: status for (name, _h, _p, _k), status in zip(PROBES, results, strict=True)}


def now() -> float:
    return time.time()
