"""Запуск live-панелей L0: процесс-работник + обновляемая сводка в tmux.

Панель рендерится из снапшотов телеметрии (`.l0/metrics/*.json`).
Сырые логи пишутся в `.l0/logs/<role>.log` и доступны в режиме деталей.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import sys
import time
from pathlib import Path

from src.console import health
from src.console.metrics import dumps
from src.console.panels import aggregate, build_panel, build_sys_panel
from src.console.render import render_panel
from src.console.style import colors_enabled

REPO_ROOT = Path(__file__).resolve().parents[2]

ROLE_CMDS: dict[str, list[str]] = {
    "cons": [sys.executable, "run.py", "cons"],
    "conf": [sys.executable, "run.py", "conf"],
    "write": [sys.executable, "run.py", "write"],
    "api": [sys.executable, "-m", "uvicorn", "api:app", "--host", "127.0.0.1", "--port", "8000"],
    "tx": [sys.executable, "tx_bot.py"],
}

ROLES = ("sys", "api", "cons", "conf", "write", "tx")


def log_path(role: str) -> Path:
    return REPO_ROOT / ".l0" / "logs" / f"{role}.log"


def details(role: str) -> str:
    """Полный диагностический дамп: счётчики, события с полными хэшами, хвост лога."""
    lines = [f"# L0 · детали роли {role}", f"каталог метрик: {REPO_ROOT / '.l0' / 'metrics'}"]
    snaps = dumps(role) if role != "sys" else []
    if role == "sys":
        for other in ("api", "cons", "conf", "write", "tx"):
            lines.append(f"\n## {other}")
            lines.append(_format_snaps(dumps(other)))
    else:
        lines.append(_format_snaps(snaps))
    log = log_path(role)
    lines.append(f"\n## лог: {log}")
    if log.exists():
        tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-30:]
        lines.extend(tail or ["(пусто)"])
    else:
        lines.append(f"{UNKNOWN_TEXT}")
    return "\n".join(lines)


UNKNOWN_TEXT = "нет данных (UNKNOWN)"


def _format_snaps(snaps: list[dict]) -> str:
    if not snaps:
        return UNKNOWN_TEXT
    return "\n".join(json.dumps(s, ensure_ascii=False, indent=2) for s in snaps)


def run_raw(role: str, extra: list[str]) -> int:
    """Старый режим: просто запустить рабочий процесс с сырым выводом."""
    cmd = ROLE_CMDS.get(role)
    if not cmd:
        print(f"роль {role} не запускает процесс", file=sys.stderr)
        return 1
    import subprocess

    return subprocess.call(cmd + extra, cwd=REPO_ROOT)


class Viewer:
    """Собирает снапшоты и рисует одну панель роли."""

    def __init__(self, role: str, use_color: bool) -> None:
        self.role = role
        self.use_color = use_color
        self.prev: dict | None = None
        self.child_restarts = 0
        self.restarts = {"api": 0, "cons": 0, "conf": 0, "write": 0, "tx": 0}
        self.started_at = time.time()

    def load_role(self, role: str) -> dict | None:
        return aggregate(dumps(role))

    async def frame(self, child_alive: bool | None) -> str:
        width, height = _term_size()
        if self.role == "sys":
            statuses = await health.probe_all()
            snaps = {r: self.load_role(r) for r in ("api", "cons", "conf", "write", "tx")}
            data = build_sys_panel(statuses, snaps, time.time() - self.started_at, self.restarts, self.use_color)
            return render_panel(data, width, height, self.use_color)

        snap = self.load_role(self.role)
        if snap is not None:
            snap = dict(snap)
            snap["restarts"] = int(snap.get("restarts", 0)) + self.child_restarts
        if snap is None:
            status = "UNKNOWN"
        elif child_alive:
            status = "UP"
        else:
            status = "DOWN"
        data = build_panel(self.role, snap, self.prev, status, self.use_color)
        self.prev = snap
        return render_panel(data, width, height, self.use_color)


def _term_size() -> tuple[int, int]:
    size = shutil.get_terminal_size(fallback=(110, 36))
    return size.columns, size.lines


async def _spawn_and_watch(role: str, extra: list[str], viewer: Viewer, stop: asyncio.Event) -> None:
    cmd = ROLE_CMDS[role] + extra
    log = log_path(role)
    log.parent.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "L0_ROLE": role}
    while not stop.is_set():
        with log.open("a", encoding="utf-8") as stream:
            stream.write(f"\n--- {time.strftime('%F %T')} · start {role} ---\n")
            stream.flush()
            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(REPO_ROOT),
                env=env,
                stdout=stream,
                stderr=stream,
            )
            exit_task = asyncio.create_task(process.wait())
            stop_task = asyncio.create_task(stop.wait())
            done, _pending = await asyncio.wait({exit_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
            if stop_task in done:
                if process.returncode is None:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=3)
                    except asyncio.TimeoutError:
                        process.kill()
                exit_task.cancel()
                return
            code = process.returncode
            viewer.child_restarts += 1
            viewer.restarts[role] = viewer.restarts.get(role, 0) + 1
            with log.open("a", encoding="utf-8") as stream:
                stream.write(f"--- {time.strftime('%F %T')} · exit {role} code={code} ---\n")
            await asyncio.sleep(2.0)


async def run_tui(role: str, extra: list[str], interval: float, use_color: bool) -> None:
    viewer = Viewer(role, use_color)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    worker = None
    if role in ROLE_CMDS:
        worker = asyncio.create_task(_spawn_and_watch(role, extra, viewer, stop))

    interactive = sys.stdout.isatty()
    if interactive and use_color:
        sys.stdout.write("\033[?1049h\033[?25l")
        sys.stdout.flush()
    try:
        while not stop.is_set():
            child_alive = worker is not None and not worker.done()
            frame = await viewer.frame(child_alive if worker is not None else None)
            if interactive:
                out = "\033[H"
                for line in frame.splitlines():
                    out += line + "\033[K\n"
                sys.stdout.write(out)
                sys.stdout.flush()
            else:
                stamp = time.strftime("%H:%M:%S")
                sys.stdout.write(f"\n=== {role} · {stamp} ===\n{frame}\n")
                sys.stdout.flush()
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
    finally:
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        if interactive and use_color:
            sys.stdout.write("\033[?25h\033[?1049l")
            sys.stdout.flush()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="l0-console", description="Live-панель L0 (презентационный терминал).")
    parser.add_argument("role", choices=ROLES, help="роль панели")
    parser.add_argument("--details", action="store_true", help="полный диагностический дамп и выход")
    parser.add_argument("--raw", action="store_true", help="сырой режим: просто запустить рабочий процесс")
    parser.add_argument("--no-color", action="store_true", help="без ANSI-цвета")
    parser.add_argument("--interval", type=float, default=None, help="период обновления панели, сек")
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--" in argv:
        split_at = argv.index("--")
        argv, extra = argv[:split_at], argv[split_at + 1 :]
    else:
        extra = []
    args, unknown = build_parser().parse_known_args(argv)
    extra = unknown + extra
    role = args.role

    if args.details:
        print(details(role))
        return 0

    use_color = (not args.no_color) and colors_enabled()
    if args.raw:
        return run_raw(role, extra)

    interval = args.interval or float(os.environ.get("L0_INTERVAL", "1.5"))
    try:
        asyncio.run(run_tui(role, extra, interval, use_color))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
