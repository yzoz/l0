"""Тесты телеметрии и live-панелей L0: счётчики, события, middleware, рендер."""

import json
import socket
import time

import pytest

from src.console import health
from src.console.metrics import Metrics, get_metrics
from src.console.middleware import MetricsMiddleware, status_class
from src.console.panels import aggregate, build_conf_panel, build_cons_panel, build_panel
from src.console.render import render_panel
from src.console.style import clip, colors_enabled, fmt_dur, short_hash, sparkline, visible_len


@pytest.fixture(autouse=True)
def clean_registry():
    registry = get_metrics()
    registry.set_role("test")
    registry.reset()
    registry.reset()  # второй сброс — чтобы restarts не мешал точным проверкам
    before = registry.snapshot()
    yield
    registry.reset()
    assert get_metrics().counter("requests_total") == 0
    assert before["role"] == "test"


# --- Metrics ---------------------------------------------------------------


def test_counters_and_observer():
    m = get_metrics()
    m.inc("received")
    m.inc("received", 2)
    m.set("sample_size", 100)
    m.observe("latency_ms", 10)
    m.observe("latency_ms", 30)
    snap = m.snapshot()
    assert snap["counters"]["received"] == 3
    assert snap["gauges"]["sample_size"] == 100
    obs = snap["observers"]["latency_ms"]
    assert obs["count"] == 2 and obs["sum"] == 40 and obs["min"] == 10 and obs["max"] == 30


def test_event_dedupe_consecutive():
    m = get_metrics()
    m.event("warn", "dup", "abc12345 дубликат", ref="a" * 64, key="dup:aa")
    m.event("warn", "dup", "abc12345 дубликат", ref="a" * 64, key="dup:aa")
    m.event("warn", "dup", "abc12345 дубликат", ref="a" * 64, key="dup:aa")
    m.event("ok", "added", "def67890 добавлена", key="added:bb")
    events = m.snapshot()["events"]
    assert len(events) == 2
    assert events[0]["count"] == 3
    assert events[1]["count"] == 1
    assert events[0]["ref"] == "a" * 64  # полный хэш хранится в диагностике


def test_event_ring_is_bounded():
    m = get_metrics()
    for i in range(80):
        m.event("info", "x", f"e{i}", key=f"k{i}")
    assert len(m.snapshot()["events"]) == 50
    assert m.snapshot()["events"][-1]["text"] == "e79"


def test_reset_on_service_restart():
    m = get_metrics()
    m.inc("added", 5)
    m.observe("latency_ms", 1)
    m.event("ok", "added", "x", key="k")
    m.set("nodes_total", 42)
    restarts_before = m.snapshot()["restarts"]
    m.reset()
    snap = m.snapshot()
    assert snap["counters"] == {}
    assert snap["observers"] == {}
    assert snap["events"] == []
    assert snap["gauges"]["nodes_total"] == 42  # гаuges (параметры) сохраняются
    assert snap["restarts"] == restarts_before + 1
    assert snap["service_started_at"] >= snap["started_at"]


def test_dump_and_load(tmp_path):
    m = get_metrics()
    m.set_role("cons")
    m.inc("added")
    path = m.dump(tmp_path / "cons.json")
    loaded = Metrics.load(path)
    assert loaded is not None
    assert loaded["role"] == "cons"
    assert loaded["counters"]["added"] == 1
    assert Metrics.load(tmp_path / "missing.json") is None


def test_dumps_globs_role(tmp_path, monkeypatch):
    monkeypatch.setenv("L0_METRICS_DIR", str(tmp_path))
    main = Metrics("tx-main")
    main.inc("sent", 2)
    main.dump()
    worker = Metrics("tx-w1")
    worker.inc("sent", 3)
    worker.dump()
    from src.console.metrics import dumps

    snaps = dumps("tx")
    assert len(snaps) == 2
    assert aggregate(snaps)["counters"]["sent"] == 5


# --- style -----------------------------------------------------------------


def test_short_hash_and_clip():
    assert short_hash("a" * 64) == "aaaaaaaa"
    assert short_hash(None) == "-"
    assert clip("abcdef", 4) == "abc…"
    assert clip("abc", 10) == "abc"
    assert visible_len("\033[36mabc\033[0m") == 3
    clipped = clip("\033[36mabcdef\033[0m", 4)
    assert visible_len(clipped) == 4
    assert clipped.startswith("\033[36m")


def test_colors_respect_env(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    assert colors_enabled() is False
    monkeypatch.delenv("NO_COLOR")
    monkeypatch.setenv("L0_COLOR", "0")
    assert colors_enabled() is False
    monkeypatch.setenv("L0_COLOR", "1")
    assert colors_enabled() is True


def test_fmt_dur_and_sparkline():
    assert fmt_dur(45) == "45s"
    assert fmt_dur(125) == "2:05"
    assert fmt_dur(3723) == "1:02:03"
    assert fmt_dur(None) == "UNKNOWN"
    assert sparkline([], 8) == ""
    assert len(sparkline([1, 2, 3, 4], 8)) == 4


# --- middleware ------------------------------------------------------------


async def _asgi_app(scope, receive, send):
    status = scope.get("test_status", 200)
    await send({"type": "http.response.start", "status": status, "headers": []})
    await send({"type": "http.response.body", "body": b"secret-body"})


async def _failing_app(scope, receive, send):
    raise RuntimeError("boom")


def _scope(path="/send/", method="POST"):
    return {"type": "http", "method": method, "path": path}


async def _call(mw, scope):
    async def receive():
        return {"type": "http.request", "body": b"sign=SECRET"}

    sent = []

    async def send(message):
        sent.append(message)

    await mw(scope, receive, send)
    return sent


async def test_middleware_status_classes():
    m = get_metrics()
    mw = MetricsMiddleware(_asgi_app, m)
    for status in (200, 201, 301, 404, 500):
        scope = _scope()
        scope["test_status"] = status
        await _call(mw, scope)
    snap = m.snapshot()
    assert snap["counters"]["requests_total"] == 5
    assert snap["counters"]["status_2xx"] == 2
    assert snap["counters"]["status_3xx"] == 1
    assert snap["counters"]["status_4xx"] == 1
    assert snap["counters"]["status_5xx"] == 1
    assert status_class(204) == "2xx" and status_class(499) == "4xx" and status_class(503) == "5xx"
    obs = snap["observers"]["latency_ms"]
    assert obs["count"] == 5 and obs["max"] >= 0


async def test_middleware_records_events_without_bodies():
    m = get_metrics()
    mw = MetricsMiddleware(_asgi_app, m)
    await _call(mw, _scope("/send/"))
    await _call(mw, _scope("/send/"))
    await _call(mw, _scope("/pool", "GET"))
    events = m.snapshot()["events"]
    texts = " | ".join(e["text"] for e in events)
    assert "POST /send/" in texts and "GET /pool" in texts
    assert "SECRET" not in texts and "sign=" not in texts and "secret-body" not in texts
    # повторяющиеся запросы сворачиваются, а не заливают ленту
    send_events = [e for e in events if e["kind"] == "http" and "/send/" in e["text"]]
    assert sum(e["count"] for e in send_events) == 2


async def test_middleware_failed_request_counts_as_error():
    m = get_metrics()
    mw = MetricsMiddleware(_failing_app, m)
    with pytest.raises(RuntimeError):
        await _call(mw, _scope())
    assert m.counter("status_5xx") == 1


async def test_middleware_skips_non_http():
    m = get_metrics()

    async def scope_app(scope, receive, send):
        return None

    mw = MetricsMiddleware(scope_app, m)
    await mw({"type": "lifespan"}, None, None)
    assert m.counter("requests_total") == 0


# --- panels ----------------------------------------------------------------


def test_panels_show_unknown_without_data():
    for role in ("api", "cons", "conf", "write", "tx"):
        data = build_panel(role, None, None, "UNKNOWN", use_color=False)
        frame = render_panel(data, 80, 20, use_color=False)
        assert "UNKNOWN" in frame


def test_cons_panel_reasons_and_short_ids():
    m = get_metrics()
    m.inc("received", 10)
    m.inc("added", 7)
    m.inc("dup", 2)
    m.inc("no_funds", 1)
    m.event("warn", "dup", f"{'a1b2c3d4'} дубликат", ref="a" * 64, key="dup:x")
    snap = m.snapshot()
    data = build_cons_panel(snap, None, "UP", use_color=False)
    text = "\n".join(data.rows)
    assert "сообщений 10" in text and "в граф 7" in text and "отказы 3" in text
    assert "дубликат 2" in text and "средства 1" in text
    frame = render_panel(data, 100, 20, use_color=False)
    assert "a1b2c3d4" in frame
    assert "a" * 64 not in frame  # полный хэш не выводится на основной экран


def test_conf_panel_separates_nodes_and_sample():
    m = get_metrics()
    m.set("nodes_total", 5432)
    m.set("sample_size", 100)
    m.set("cycle_ms", 120.0)
    m.set("last_cycle_at", time.time())
    m.set("last_sample", 100)
    m.set("last_processed", 97)
    m.set("last_errors", 3)
    m.set("weights_min", 1.0)
    m.set("weights_avg", 3.2)
    m.set("weights_max", 12.4)
    m.set("weight_buckets", [[0, 1, 10], [10, None, 2]])
    m.set("confirm_weight", 10)
    m.set("max_depth", 7)
    m.set("damping", 0.5)
    m.set("cycle_history", [{"ts": time.time(), "ms": 100}])
    m.inc("cycles", 5)
    snap = m.snapshot()
    data = build_conf_panel(snap, None, "UP", use_color=False)
    text = "\n".join(data.rows)
    assert "узлов в графе 5,432" in text
    assert "выборка ≤100: 100" in text
    assert "CONFIRM_WEIGHT=10" in text and "MAX_DEPTH=7" in text and "DAMPING=0.5" in text
    assert "выбрано 100 → обработано 97 → 3 ошибки" in text
    assert "min 1.00" in text and "max 12.40" in text


def test_write_panel_interval_and_total():
    m = get_metrics()
    m.inc("confirmed", 30)
    cur = m.snapshot()
    m.inc("confirmed", 5)
    cur2 = m.snapshot()
    data = build_panel("write", cur2, cur, "UP", use_color=False)
    text = "\n".join(data.rows)
    assert "всего 35" in text and "за интервал 5" in text


def test_render_panel_respects_width_and_no_color():
    data = build_panel("api", None, None, "UNKNOWN", use_color=False)
    frame = render_panel(data, 40, 12, use_color=False)
    assert "\033[" not in frame
    for line in frame.splitlines():
        assert visible_len(line) <= 40
    colored = render_panel(data, 40, 12, use_color=True)
    assert "\033[" in colored


def test_render_panel_tiny_terminal():
    data = build_panel("cons", None, None, "DOWN", use_color=False)
    for width, height in ((10, 3), (20, 4), (30, 5), (200, 40)):
        frame = render_panel(data, width, height, use_color=False)
        for line in frame.splitlines():
            assert visible_len(line) <= width
        assert len(frame.splitlines()) <= height


def test_aggregate_merges_workers():
    a = {
        "role": "tx-w1",
        "ts": 1,
        "started_at": 1,
        "service_started_at": 1,
        "restarts": 0,
        "counters": {"sent": 2},
        "gauges": {"coroutines": 1},
        "observers": {},
        "events": [],
    }
    b = {
        "role": "tx-w2",
        "ts": 2,
        "started_at": 1,
        "service_started_at": 1,
        "restarts": 0,
        "counters": {"sent": 3, "ok": 3},
        "gauges": {"coroutines": 2},
        "observers": {},
        "events": [],
    }
    agg = aggregate([a, b])
    assert agg["counters"]["sent"] == 5
    assert agg["counters"]["ok"] == 3
    assert agg["gauges"]["coroutines"] == 2


# --- health ----------------------------------------------------------------


async def test_probe_tcp_up_and_down():
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    try:
        assert await health.probe_one("test", "127.0.0.1", port, "tcp") == health.UP
    finally:
        server.close()
    assert await health.probe_one("test", "127.0.0.1", port, "tcp") == health.DOWN


async def test_probe_unknown_on_bad_host():
    assert await health.probe_one("test", "no-such-host.invalid", 1, "tcp", timeout=0.3) == health.UNKNOWN


async def test_probe_all_returns_all_services():
    results = await health.probe_all(timeout=0.2)
    assert set(results) == {"API", "RabbitMQ", "MongoDB", "Redis", "Memgraph"}
    assert all(status in (health.UP, health.DOWN, health.UNKNOWN) for status in results.values())


# --- инструментация сервисов ----------------------------------------------


async def test_consumer_counts_events():
    from unittest.mock import AsyncMock, MagicMock

    from keys import pk
    from service.consumer import ConsumerProcessor

    m = get_metrics()
    db, rabbit, redis, dag = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    redis.client.lock = MagicMock()
    redis.client.lock.return_value.__aenter__ = AsyncMock()
    redis.client.lock.return_value.__aexit__ = AsyncMock()
    processor = ConsumerProcessor(rabbit, db, redis, dag)
    processor.check_duplicate = AsyncMock(return_value=True)

    content = {
        "parents": ["p1", "p2"],
        "credit": "c",
        "debit": pk[0],
        "amount": 1,
        "uniq": "u",
        "msg": "",
    }
    message = MagicMock()
    message.body = json.dumps({"content": content, "sign": "s"}).encode()
    message.nack = AsyncMock()
    await processor.process_message(message)
    assert m.counter("received") == 1
    assert m.counter("dup") == 1
    events = m.snapshot()["events"]
    assert events and events[-1]["level"] == "warn" and events[-1]["count"] == 1


async def test_consumer_bad_format_counts():
    from unittest.mock import AsyncMock, MagicMock

    from service.consumer import ConsumerProcessor

    m = get_metrics()
    processor = ConsumerProcessor(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    message = MagicMock()
    message.body = b"not-json"
    message.nack = AsyncMock()
    await processor.process_message(message)
    assert m.counter("bad_format") == 1
    message.nack.assert_awaited_once_with(requeue=False)


async def test_writer_duplicate_counts():
    from unittest.mock import AsyncMock, MagicMock

    from pymongo.errors import DuplicateKeyError

    from keys import pk
    from service.writer import WriterProcessor

    m = get_metrics()
    db, redis, dag = MagicMock(), MagicMock(), MagicMock()
    db.insert_tx = AsyncMock(side_effect=DuplicateKeyError("dup"))
    redis.client.lock = MagicMock()
    redis.client.lock.return_value.__aenter__ = AsyncMock()
    redis.client.lock.return_value.__aexit__ = AsyncMock()
    processor = WriterProcessor(db, redis, dag)
    processor._delete_node = AsyncMock(return_value=True)
    session = MagicMock()
    data = {
        "parents": ["p1", "p2"],
        "credit": "c",
        "debit": pk[0],
        "amount": 1,
        "uniq": "u",
        "msg": "",
        "sign": "s",
        "time": 1,
    }
    await processor._confirm(session, "h" * 64, data)
    assert m.counter("dup") == 1
    assert m.counter("confirmed") == 0
    events = m.snapshot()["events"]
    assert events and "дубликат" in events[-1]["text"]


async def test_confirmer_records_cycle_stats():
    from unittest.mock import AsyncMock, MagicMock

    from service.confirmer import ConfirmationProcessor

    m = get_metrics()

    class FakeResult:
        def __init__(self, records):
            self._records = records

        async def single(self):
            return self._records[0] if self._records else None

        def __aiter__(self):
            self._iter = iter(self._records)
            return self

        async def __anext__(self):
            try:
                return next(self._iter)
            except StopIteration as err:
                raise StopAsyncIteration from err

    class FakeSession:
        async def run(self, query, **params):
            if "count(t)" in query:
                return FakeResult([{"total": 5432}])
            if "ORDER BY rand()" in query:
                return FakeResult([{"t.hash": "a" * 64}, {"t.hash": "b" * 64}])
            return FakeResult([{"indirect_weight": 2.0}])

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    dag = MagicMock()
    dag.driver.session = MagicMock(return_value=FakeSession())
    redis = MagicMock()
    redis.client.lock = MagicMock()
    redis.client.lock.return_value.__aenter__ = AsyncMock()
    redis.client.lock.return_value.__aexit__ = AsyncMock()

    processor = ConfirmationProcessor(redis, dag)
    await processor.check_pool()
    snap = m.snapshot()
    assert snap["gauges"]["nodes_total"] == 5432
    assert snap["gauges"]["sample_size"] == 2
    assert snap["counters"]["processed"] == 2
    assert snap["counters"]["errors"] == 0
    assert snap["gauges"]["last_sample"] == 2
    assert snap["gauges"]["weights_avg"] == 3.0
    assert snap["gauges"]["weight_buckets"][3][2] == 2  # вес 3.0 попадает в корзину 3–5
    assert snap["gauges"]["cycle_history"]
