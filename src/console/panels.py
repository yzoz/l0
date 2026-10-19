"""Содержимое панелей L0: сводки и ленты событий по ролям.

Роли: sys (Система), api (API), cons (Consumer/ingress),
conf (Confirmer/DAG), write (Writer/settlement), tx (TX bot).
Значения, которых нет в телеметрии, показываются как UNKNOWN.
"""

from __future__ import annotations

import time
from typing import Any

from src.console.render import PanelData
from src.console.style import (
    fmt_dur,
    fmt_float,
    fmt_int,
    fmt_ms,
    fmt_rate,
    paint,
    short_hash,
)

UNKNOWN = "UNKNOWN"

TITLES = {
    "sys": "1 · СИСТЕМА",
    "api": "2 · API",
    "cons": "3 · CONSUMER / ingress",
    "conf": "4 · CONFIRMER / DAG",
    "write": "5 · WRITER / settlement",
    "tx": "6 · TX BOT",
}


def _counter(snap: dict[str, Any] | None, name: str) -> int:
    if not snap:
        return 0
    return int(snap.get("counters", {}).get(name, 0))


def _gauge(snap: dict[str, Any] | None, name: str, default: Any = None) -> Any:
    if not snap:
        return default
    return snap.get("gauges", {}).get(name, default)


def _obs(snap: dict[str, Any] | None, name: str) -> dict[str, float]:
    if not snap:
        return {}
    return snap.get("observers", {}).get(name, {})


def aggregate(snaps: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Суммирует снапшоты (основной процесс + воркеры tx) в один."""
    snaps = [s for s in snaps if s]
    if not snaps:
        return None
    if len(snaps) == 1:
        return snaps[0]
    counters: dict[str, int] = {}
    gauges: dict[str, Any] = {}
    observers: dict[str, dict[str, float]] = {}
    events: list[dict[str, Any]] = []
    for snap in snaps:
        for key, value in snap.get("counters", {}).items():
            counters[key] = counters.get(key, 0) + int(value)
        gauges.update(snap.get("gauges", {}))
        for key, obs in snap.get("observers", {}).items():
            acc = observers.setdefault(key, {"count": 0, "sum": 0.0, "min": obs.get("min"), "max": obs.get("max")})
            acc["count"] += obs.get("count", 0)
            acc["sum"] += obs.get("sum", 0.0)
            if obs.get("min") is not None:
                acc["min"] = min(acc["min"], obs["min"]) if acc["min"] is not None else obs["min"]
            if obs.get("max") is not None:
                acc["max"] = max(acc["max"], obs["max"]) if acc["max"] is not None else obs["max"]
        events.extend(snap.get("events", []))
    events.sort(key=lambda e: e.get("ts", 0))
    latest = max(snaps, key=lambda s: s.get("ts", 0))
    return {
        "role": latest.get("role"),
        "ts": latest.get("ts"),
        "started_at": min(s.get("started_at", 0) for s in snaps),
        "service_started_at": min(s.get("service_started_at", 0) for s in snaps),
        "restarts": max(s.get("restarts", 0) for s in snaps),
        "counters": counters,
        "gauges": gauges,
        "observers": observers,
        "events": events[-50:],
    }


def _rate(cur: int, prev: int | None, dt: float | None) -> float | None:
    if prev is None or not dt or dt <= 0:
        return None
    return max(0.0, (cur - prev) / dt)


def _events(snap: dict[str, Any] | None, limit: int = 8) -> list[tuple[str, str, int]]:
    if not snap:
        return []
    items = snap.get("events", [])[-limit:]
    out = []
    for event in reversed(items):
        ts = time.strftime("%H:%M:%S", time.localtime(event.get("ts", 0)))
        count = int(event.get("count", 1))
        out.append((event.get("level", "info"), f"{ts} {event.get('text', '')}", count))
    return out


def _uptime_row(snap: dict[str, Any] | None, now: float) -> str:
    started = (snap or {}).get("service_started_at")
    if not started:
        return f"работает {UNKNOWN}"
    return f"работает {fmt_dur(now - started)} · перезапуски {(snap or {}).get('restarts', 0)}"


def build_sys_panel(
    statuses: dict[str, str],
    snaps: dict[str, dict[str, Any] | None],
    session_uptime: float,
    restarts: dict[str, int],
    use_color: bool = True,
) -> PanelData:
    level_of = {"UP": "ok", "DOWN": "error", "UNKNOWN": "warn"}
    rows = []
    ports = {"API": 8000, "RabbitMQ": 5672, "MongoDB": 27017, "Redis": 6379, "Memgraph": 7687}
    for name in ("API", "RabbitMQ", "MongoDB", "Redis", "Memgraph"):
        status = statuses.get(name, UNKNOWN)
        name_cell = paint(f"{name:<9}", "label", use_color)
        status_cell = paint(status, level_of.get(status, "warn"), use_color)
        rows.append(f"{name_cell} {status_cell} :{ports[name]}")
    service_bits = []
    for role in ("api", "cons", "conf", "write", "tx"):
        snap = snaps.get(role)
        if snap:
            service_bits.append(f"{role} {fmt_dur(time.time() - snap.get('service_started_at', time.time()))}")
        else:
            service_bits.append(f"{role} {UNKNOWN}")
    rows.append(f"сессия {fmt_dur(session_uptime)} · перезапуски {sum(restarts.values())}")
    rows.append("сервисы: " + " · ".join(service_bits))
    events = []
    for name, status in statuses.items():
        if status != "UP":
            events.append(("error" if status == "DOWN" else "warn", f"{name} {status}", 1))
    for role, count in restarts.items():
        if count:
            events.append(("warn", f"{role} перезапущен", count))
    if not events:
        events.append(("info", "все зависимости отвечают", 1))
    return PanelData(title=TITLES["sys"], status="LIVE", status_level="ok", rows=rows, events=events)


def _status_level(status: str) -> str:
    return "ok" if status == "UP" else ("error" if status == "DOWN" else "warn")


def build_api_panel(
    snap: dict[str, Any] | None, prev: dict[str, Any] | None, status: str, use_color: bool = True
) -> PanelData:
    total = _counter(snap, "requests_total")
    prev_total = _counter(prev, "requests_total") if prev else None
    dt = (snap or {}).get("ts", 0) - (prev or {}).get("ts", 0) if snap and prev else None
    rate = _rate(total, prev_total, dt)
    obs = _obs(snap, "latency_ms")
    avg = obs["sum"] / obs["count"] if obs.get("count") else None
    rows = [
        f"запросов {fmt_int(total)} · {fmt_rate(rate)}",
        f"2xx {fmt_int(_counter(snap, 'status_2xx'))} · 3xx {fmt_int(_counter(snap, 'status_3xx'))} · "
        f"4xx {fmt_int(_counter(snap, 'status_4xx'))} · 5xx {fmt_int(_counter(snap, 'status_5xx'))}",
        f"задержка avg {fmt_ms(avg)} · max {fmt_ms(obs.get('max'))}",
        "тела и подписи не собираются · детали: --details api",
    ]
    return PanelData(
        title=TITLES["api"],
        status=status,
        status_level=_status_level(status),
        rows=rows,
        events=_events(snap),
    )


def build_cons_panel(
    snap: dict[str, Any] | None, prev: dict[str, Any] | None, status: str, use_color: bool = True
) -> PanelData:
    received = _counter(snap, "received")
    prev_received = _counter(prev, "received") if prev else None
    dt = (snap or {}).get("ts", 0) - (prev or {}).get("ts", 0) if snap and prev else None
    rate = _rate(received, prev_received, dt)
    rejects = sum(_counter(snap, k) for k in ("dup", "no_funds", "no_parent", "bad_format", "cycle", "error"))
    rows = [
        f"сообщений {fmt_int(received)} · в граф {fmt_int(_counter(snap, 'added'))} · "
        f"повторные {fmt_int(_counter(snap, 'retried'))} · отказы {fmt_int(rejects)}",
        f"дубликат {_counter(snap, 'dup')} · средства {_counter(snap, 'no_funds')} · "
        f"родитель {_counter(snap, 'no_parent')} · формат {_counter(snap, 'bad_format')}",
        f"цикл {_counter(snap, 'cycle')} · ошибка {_counter(snap, 'error')} · "
        f"конфликт {_counter(snap, 'conflict')} · скорость {fmt_rate(rate)}",
        _uptime_row(snap, time.time()),
    ]
    return PanelData(
        title=TITLES["cons"],
        status=status,
        status_level=_status_level(status),
        rows=rows,
        events=_events(snap),
    )


def build_conf_panel(
    snap: dict[str, Any] | None, prev: dict[str, Any] | None, status: str, use_color: bool = True
) -> PanelData:
    """Панель confirmer: событийная обработка mid и публикация кандидатов в out.

    score пересчитывается в памяти рекурсивно по поддеревьям и не хранится.
    """
    events = _counter(snap, "events")
    prev_events = _counter(prev, "events") if prev else None
    dt = (snap or {}).get("ts", 0) - (prev or {}).get("ts", 0) if snap and prev else None
    rate = _rate(events, prev_events, dt)
    obs = _obs(snap, "score")
    avg_score = obs["sum"] / obs["count"] if obs.get("count") else None
    rows = [
        f"события mid {fmt_int(events)} · родителей оценено {fmt_int(_counter(snap, 'scored'))} · {fmt_rate(rate)}",
        f"в out {fmt_int(_counter(snap, 'enqueued'))} · дедупликация {fmt_int(_counter(snap, 'dedup'))} · "
        f"не найдено {fmt_int(_counter(snap, 'not_found'))}",
        f"score min {fmt_float(obs.get('min'))} · avg {fmt_float(avg_score)} · max {fmt_float(obs.get('max'))}",
        f"ошибки {fmt_int(_counter(snap, 'errors'))} · битый формат {fmt_int(_counter(snap, 'bad_format'))}",
        "формула: score = transfer_score(own) + Σ score(child)/3 · рекурсивно · порог ≥ 1",
    ]
    return PanelData(
        title=TITLES["conf"],
        status=status,
        status_level=_status_level(status),
        rows=rows,
        events=_events(snap),
    )


def build_write_panel(
    snap: dict[str, Any] | None, prev: dict[str, Any] | None, status: str, use_color: bool = True
) -> PanelData:
    """Панель writer: обработка очереди out и финализация транзакций."""
    confirmed = _counter(snap, "confirmed")
    interval = confirmed - _counter(prev, "confirmed") if prev is not None else None
    rows = [
        f"подтверждено всего {fmt_int(confirmed)} · за интервал "
        f"{fmt_int(interval) if interval is not None else UNKNOWN}",
        f"отложено {fmt_int(_counter(snap, 'deferred'))} · перепоставлено {fmt_int(_counter(snap, 'requeued'))} · "
        f"отброшено (пул) {fmt_int(_counter(snap, 'dropped'))}",
        f"недостаток средств {_counter(snap, 'insufficient')} · дубликаты {_counter(snap, 'dup')} · "
        f"мёртвый родитель {_counter(snap, 'parent_dead')}",
        f"ошибки записи {_counter(snap, 'insert_errors')} · Redis {_counter(snap, 'redis_errors')} · "
        f"компенсации {_counter(snap, 'compensation')} · удаления {_counter(snap, 'delete_errors')}",
        _uptime_row(snap, time.time()),
    ]
    return PanelData(
        title=TITLES["write"],
        status=status,
        status_level=_status_level(status),
        rows=rows,
        events=_events(snap),
    )


def build_tx_panel(
    snap: dict[str, Any] | None, prev: dict[str, Any] | None, status: str, use_color: bool = True
) -> PanelData:
    sent = _counter(snap, "sent")
    prev_sent = _counter(prev, "sent") if prev else None
    dt = (snap or {}).get("ts", 0) - (prev or {}).get("ts", 0) if snap and prev else None
    rate = _rate(sent, prev_sent, dt)
    obs = _obs(snap, "latency_ms")
    avg = obs["sum"] / obs["count"] if obs.get("count") else None
    rows = [
        f"отправлено {fmt_int(sent)} · ok {fmt_int(_counter(snap, 'ok'))} · "
        f"ошибки {fmt_int(_counter(snap, 'err'))} · {fmt_rate(rate)}",
        f"задержка avg {fmt_ms(avg)} · max {fmt_ms(obs.get('max'))}",
        f"процессы {_gauge(snap, 'processes', UNKNOWN)} · корутины {_gauge(snap, 'coroutines', UNKNOWN)} · "
        f"параметры t={_gauge(snap, 'param_t', '?')} c={_gauge(snap, 'param_c', '?')} n={_gauge(snap, 'param_n', '?')}",
        "полные JSON и подписи: L0_VERBOSE=1 · детали: --details tx",
    ]
    return PanelData(
        title=TITLES["tx"],
        status=status,
        status_level=_status_level(status),
        rows=rows,
        events=_events(snap),
    )


BUILDERS = {
    "api": build_api_panel,
    "cons": build_cons_panel,
    "conf": build_conf_panel,
    "write": build_write_panel,
    "tx": build_tx_panel,
}


def build_panel(
    role: str, snap: dict[str, Any] | None, prev: dict[str, Any] | None, status: str, use_color: bool = True
) -> PanelData:
    builder = BUILDERS.get(role)
    if builder is None:
        return PanelData(
            title=TITLES.get(role, role), status=UNKNOWN, status_level="warn", rows=[f"нет данных: {UNKNOWN}"]
        )
    if snap is None:
        data = builder(None, None, UNKNOWN, use_color)
        data.rows.insert(0, paint("телеметрия ещё не поступала", "warn", use_color))
        data.status = UNKNOWN
        data.status_level = "warn"
        return data
    return builder(snap, prev, status, use_color)


def event_text_with_ref(text: str, ref: str | None) -> str:
    """Полный идентификатор для режима деталей (не для основного экрана)."""
    return f"{text} [{ref}]" if ref else text


def short_ref(ref: str | None) -> str:
    return short_hash(ref)
