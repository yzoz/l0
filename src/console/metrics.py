"""Лёгкая телеметрия L0: счётчики, гаuges, события и снапшоты для панелей.

Только учёт и форматирование — никакого влияния на бизнес-логику.
Счётчики живут в памяти процесса и сбрасываются при перезапуске сервиса.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

EVENT_RING = 50
DEFAULT_METRICS_DIR = ".l0/metrics"


def metrics_dir() -> Path:
    return Path(os.environ.get("L0_METRICS_DIR", DEFAULT_METRICS_DIR))


class Metrics:
    """Реестр метрик одного процесса (потокобезопасный)."""

    def __init__(self, role: str = "unknown") -> None:
        self._lock = threading.Lock()
        self.role = role
        self.started_at = time.time()
        self.service_started_at = self.started_at
        self.restarts = 0
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, Any] = {}
        self._observers: dict[str, dict[str, float]] = {}
        self._events: list[dict[str, Any]] = []

    # --- запись -----------------------------------------------------------

    def set_role(self, role: str) -> None:
        with self._lock:
            self.role = role

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value

    def set(self, name: str, value: Any) -> None:
        with self._lock:
            self._gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            obs = self._observers.setdefault(name, {"count": 0, "sum": 0.0, "min": value, "max": value})
            obs["count"] += 1
            obs["sum"] += float(value)
            obs["min"] = min(obs["min"], float(value))
            obs["max"] = max(obs["max"], float(value))

    def event(self, level: str, kind: str, text: str, ref: str | None = None, key: str | None = None) -> None:
        """Компактное событие для ленты. Повтор подряд сворачивается в ×N."""
        now = time.time()
        dedupe_key = key if key is not None else f"{kind}:{text}"
        with self._lock:
            if self._events and self._events[-1].get("key") == dedupe_key:
                self._events[-1]["count"] += 1
                self._events[-1]["ts"] = now
                return
            self._events.append(
                {
                    "ts": now,
                    "level": level,
                    "kind": kind,
                    "text": text,
                    "ref": ref,
                    "key": dedupe_key,
                    "count": 1,
                }
            )
            del self._events[:-EVENT_RING]

    def reset(self) -> None:
        """Сброс локальных счётчиков при перезапуске сервиса.

        Сбрасываются накопительные счётчики, наблюдения и лента событий;
        гаuges (параметры алгоритма) сохраняются, число перезапусков
        увеличивается на единицу.
        """
        with self._lock:
            self._counters.clear()
            self._observers.clear()
            self._events.clear()
            self.service_started_at = time.time()
            self.restarts += 1

    # --- чтение -----------------------------------------------------------

    def counter(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def gauge(self, name: str, default: Any = None) -> Any:
        with self._lock:
            return self._gauges.get(name, default)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "role": self.role,
                "ts": time.time(),
                "started_at": self.started_at,
                "service_started_at": self.service_started_at,
                "restarts": self.restarts,
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "observers": {k: dict(v) for k, v in self._observers.items()},
                "events": [dict(e) for e in self._events],
            }

    def dump(self, path: str | Path | None = None) -> Path:
        """Атомарно сохраняет снапшот для панелей."""
        target = Path(path) if path else metrics_dir() / f"{self.role}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.snapshot(), ensure_ascii=False)
        tmp = target.with_suffix(".tmp")
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, target)
        return target

    @staticmethod
    def load(path: str | Path) -> dict[str, Any] | None:
        try:
            return json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None


_registry = Metrics()


def get_metrics() -> Metrics:
    """Глобальный реестр текущего процесса."""
    return _registry


def dumps(role: str | None = None) -> list[dict[str, Any]]:
    """Все снапшоты роли (основной + воркеры tx), отсортированные по имени."""
    directory = metrics_dir()
    pattern = f"{role or _registry.role}*.json"
    if not directory.exists():
        return []
    out = []
    for file in sorted(directory.glob(pattern)):
        snap = Metrics.load(file)
        if snap:
            out.append(snap)
    return out


def start_dumper(role: str | None = None, interval: float = 1.0):
    """Фоновый сброс снапшота в файл. Возвращает объект задачи/потока.

    Для asyncio-процессов: `task = start_dumper(); ...; task.cancel()`.
    Для синхронных процессов (tx_bot): запускает демон-поток.
    """
    import asyncio

    def _write() -> None:
        try:
            _registry.dump()
        except OSError:
            pass

    if role:
        _registry.set_role(role)

    try:
        asyncio.get_running_loop()
    except RuntimeError:

        def _loop() -> None:
            while True:
                _write()
                time.sleep(interval)

        thread = threading.Thread(target=_loop, name="l0-metrics-dumper", daemon=True)
        thread.start()
        return thread

    async def _task() -> None:
        while True:
            _write()
            await asyncio.sleep(interval)

    return asyncio.create_task(_task())
