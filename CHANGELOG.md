# Changelog

Все заметные изменения в проекте документируются в этом файле.

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/),
проект придерживается [семантического версионирования](https://semver.org/).

## [Unreleased]

### Added
- Линтер и форматтер `ruff` с конфигурацией в `pyproject.toml`.
- `pytest-cov` с конфигурацией покрытия; интеграционные тесты отвязаны от корневого `keys.py`.
- Проверки `ruff check` и `ruff format --check` в CI, а также необязательный `pip-audit`.
- `Makefile` с частыми командами, `.dockerignore` и этот `CHANGELOG.md`.

### Changed
- `api.py`: зависимости FastAPI получаются через `Request`/`request.app.state`, убран хак `Depends(lambda: app)`.
- Раннеры `run.py` и `run.d.py`: строгая валидация аргументов, явная ошибка при неизвестном типе сервиса.
- `MongoDB.aggregate`: убран `break`, из-за которого возвращался максимум один документ.
- Логирование настроено один раз в точках входа; модули только используют `logging.getLogger`.
- Магические числа (тайм-ауты, размеры пула, веса, коэффициенты затухания) вынесены в `src/config.py`.
- `writer.py`: подтверждение выполняется в явном порядке MongoDB → Redis → Memgraph с откатом при сбое.
- README: добавлены схема архитектуры, поток транзакции, примеры REST/GraphQL и бейдж CI.

### Fixed
- Убраны `print()` из продового кода (`transactions.py`, `confirmer.py`).
- Исправлена запись `txg` в `[tool.poetry.scripts]` (указывала на несуществующий `tx_generator.py`).
- Исправлена ссылка на репозиторий в README.

## [0.1.0] - 2025-05-01

### Added
- Прототип DAG-реестра транзакций: FastAPI + Strawberry GraphQL, RabbitMQ, MongoDB,
  Redis и Memgraph; проверка подписей Ed25519.
