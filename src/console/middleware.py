"""ASGI-middleware телеметрии API: статусы, задержка, компактные события.

Не читает и не сохраняет тела запросов, подписи и секреты; не меняет
контракты ответов. Путь логируется без query-строки.
"""

from __future__ import annotations

import time
from typing import Any, Callable, MutableMapping

from src.console.metrics import Metrics, get_metrics
from src.console.style import fmt_ms


def status_class(status: int) -> str:
    if status >= 500:
        return "5xx"
    if status >= 400:
        return "4xx"
    if status >= 300:
        return "3xx"
    if status >= 200:
        return "2xx"
    return "other"


def level_for(status: int) -> str:
    if status >= 500:
        return "error"
    if status >= 400:
        return "warn"
    return "info"


class MetricsMiddleware:
    """Считает HTTP-метрики API поверх произвольного ASGI-приложения."""

    def __init__(self, app: Callable, metrics: Metrics | None = None) -> None:
        self.app = app
        self.metrics = metrics if metrics is not None else get_metrics()

    async def __call__(self, scope: MutableMapping[str, Any], receive: Callable, send: Callable) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        started = time.perf_counter()
        status_box = {"status": 0}

        async def send_wrapper(message: MutableMapping[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status_box["status"] = int(message["status"])
            await send(message)

        failed = False
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            failed = True
            raise
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            self.record(scope, status_box["status"], elapsed_ms, failed)

    def record(self, scope: MutableMapping[str, Any], status: int, elapsed_ms: float, failed: bool = False) -> None:
        if not status and failed:
            status = 500
        if not status:
            status = 0
        sclass = status_class(status) if status else "other"
        self.metrics.inc("requests_total")
        self.metrics.inc(f"status_{sclass}")
        self.metrics.observe("latency_ms", elapsed_ms)

        method = str(scope.get("method", "?"))
        path = str(scope.get("path", "?")).split("?", 1)[0]
        level = "error" if failed or (status and status >= 500) else level_for(status or 500)
        text = f"{method} {path} {status or 'ERR'} {fmt_ms(elapsed_ms)}"
        self.metrics.event(level, "http", text, key=f"http:{method}:{path}:{sclass}")
