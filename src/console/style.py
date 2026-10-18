"""Цвета и форматирование значений для live-панелей L0.

Работает без TTY и с отключённым цветом (NO_COLOR / L0_COLOR=0) —
просто возвращает обычный текст без управляющих последовательностей.
"""

from __future__ import annotations

import os
import re
import sys

ANSI = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "dim": "\033[2m",
    "cyan": "\033[36m",
    "green": "\033[32m",
    "amber": "\033[33m",
    "red": "\033[31m",
    "gray": "\033[90m",
}

LEVEL_COLORS = {
    "info": "cyan",
    "ok": "green",
    "warn": "amber",
    "error": "red",
    "muted": "gray",
    "label": "gray",
}

SPARK = "▁▂▃▄▅▆▇█"

_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def colors_enabled(stream=None) -> bool:
    """Цвет включён, если поток — TTY и не задано NO_COLOR / L0_COLOR=0."""
    force = os.environ.get("L0_COLOR", "")
    if force == "1":
        return True
    if force == "0" or os.environ.get("NO_COLOR") is not None:
        return False
    stream = stream if stream is not None else sys.stdout
    try:
        return bool(stream.isatty())
    except Exception:
        return False


def paint(text: str, color: str | None, enabled: bool = True) -> str:
    """Окрашивает текст, если цвет включён; иначе возвращает как есть."""
    if not enabled or not color:
        return text
    code = ANSI.get(color)
    if not code:
        return text
    return f"{code}{text}{ANSI['reset']}"


def visible_len(text: str) -> int:
    """Длина строки без управляющих последовательностей ANSI."""
    return len(_ANSI_RE.sub("", text))


def clip(text: str, width: int) -> str:
    """Обрезает строку до width видимых символов с многоточием справа.

    Управляющие последовательности ANSI сохраняются и не участвуют в ширине.
    """
    if width <= 0:
        return ""
    if visible_len(text) <= width:
        return text
    had_ansi = bool(_ANSI_RE.search(text))
    out: list[str] = []
    visible = 0
    i = 0
    limit = width - 1 if width > 1 else 0
    while i < len(text) and visible < limit:
        char = text[i]
        if char == "\033":
            match = _ANSI_RE.match(text, i)
            if match:
                out.append(match.group(0))
                i = match.end()
                continue
        out.append(char)
        visible += 1
        i += 1
    if width > 1:
        out.append("…")
    if had_ansi:
        out.append(ANSI["reset"])
    return "".join(out)


def short_hash(value: str | None) -> str:
    """Короткий идентификатор для UI (полный хранится в диагностике)."""
    if not value:
        return "-"
    return value[:8]


def fmt_int(value: int | float | None) -> str:
    if value is None:
        return "UNKNOWN"
    return f"{int(value):,}"


def fmt_rate(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    return f"{value:.1f}/s"


def fmt_ms(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    if value >= 1000:
        return f"{value / 1000:.2f}s"
    return f"{value:.0f}ms"


def fmt_float(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "UNKNOWN"
    return f"{value:.{digits}f}"


def fmt_dur(seconds: float | None) -> str:
    """Длительность в компактном виде: 45s, 2:05, 1:02:03, 2d 03:14."""
    if seconds is None:
        return "UNKNOWN"
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours:02d}:{minutes:02d}"
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    if minutes:
        return f"{minutes}:{secs:02d}"
    return f"{secs}s"


def sparkline(values: list[float | int], width: int = 16) -> str:
    """Компактная история значений полосой (без анимации)."""
    if not values or width <= 0:
        return ""
    data = [float(v) for v in values][-width:]
    lo, hi = min(data), max(data)
    if hi <= lo:
        return SPARK[0] * len(data)
    span = hi - lo
    return "".join(SPARK[min(len(SPARK) - 1, int((v - lo) / span * (len(SPARK) - 1)))] for v in data)
