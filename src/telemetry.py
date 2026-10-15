"""Телеметрия L0 для сервисов: тонкая точка входа к реегистру метрик.

Вызовы счётчиков не влияют на бизнес-логику: только учёт и события
для live-панелей (`src.console`)."""

from src.console.metrics import get_metrics, start_dumper

metrics = get_metrics()

__all__ = ["metrics", "start_dumper"]
