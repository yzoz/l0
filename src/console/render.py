"""Рендер компактных панелей L0: рамки, заголовок, сводка, лента событий.

Деградирует мягко: без цвета, в узком/низком терминале, при перенаправлении.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.console.style import clip, paint, visible_len

TAGS = {
    "info": "·",
    "ok": "+",
    "warn": "!",
    "error": "×",
    "muted": "·",
}


@dataclass
class PanelData:
    title: str
    status: str = "UNKNOWN"
    status_level: str = "warn"
    rows: list[str] = field(default_factory=list)
    events: list[tuple[str, str, int]] = field(default_factory=list)  # (level, text, count)
    footer: str | None = None


def _hline(width: int, fill: str = "─") -> str:
    return fill * max(0, width)


def _border_line(inner: str, width: int, use_color: bool, left: str = "│", right: str = "│") -> str:
    inner_w = max(0, width - 2)
    body = clip(inner, inner_w)
    pad = max(0, inner_w - visible_len(body))
    return paint(left, "gray", use_color) + body + " " * pad + paint(right, "gray", use_color)


def render_panel(data: PanelData, width: int, height: int, use_color: bool = True) -> str:
    """Собирает панель в текст, не шире width и не выше height строк."""
    width = max(8, width)
    height = max(3, height)

    status = paint(data.status, data.status_level, use_color)
    header_text = clip(f" {data.title} · {data.status} ", width - 4)

    if width < 24 or height < 5:
        # Слишком маленький терминал — без рамок, только суть.
        lines = [clip(f"{data.title} · {status}", width)]
        lines += [clip(row, width) for row in data.rows[: max(1, height - 2)]]
        lines += [clip(f"{TAGS.get(lvl, '·')} {text}", width) for lvl, text, _cnt in data.events[:2]]
        return "\n".join(lines[:height])

    hfill = max(0, width - 4 - visible_len(header_text))
    top = (
        paint("┌─", "gray", use_color)
        + paint(header_text, "bold", use_color)
        + paint("─" * hfill + "─┐", "gray", use_color)
    )

    inner = width - 2
    lines = [top]
    for row in data.rows:
        lines.append(_border_line(row, width, use_color))
        if len(lines) >= height - 2:
            break

    if len(lines) < height - 1 and data.events:
        sep = "─" * 2 + " события " + "─" * max(0, inner - 11)
        lines.append(paint("├" + sep[:inner] + "┤", "gray", use_color))
        for level, text, count in data.events:
            tag = TAGS.get(level, "·")
            suffix = f" ×{count}" if count > 1 else ""
            painted = paint(f"{tag} ", level if level != "info" else "cyan", use_color)
            line = f"{painted}{text}{paint(suffix, 'gray', use_color)}"
            lines.append(_border_line(line, width, use_color))
            if len(lines) >= height - 1:
                break

    while len(lines) < height - 1:
        lines.append(_border_line("", width, use_color))

    bottom = paint("└" + _hline(inner) + "┘", "gray", use_color)
    lines.append(bottom)
    return "\n".join(lines[:height])
