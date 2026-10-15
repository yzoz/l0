.PHONY: install up down logs genesis api test lint format run-cons run-conf run-write

install: ## Установить зависимости
	poetry install

up: ## Поднять локальные сервисы (RabbitMQ, MongoDB, Redis, Memgraph)
	docker compose up -d

down: ## Остановить сервисы
	docker compose down

logs: ## Логи локальных сервисов
	docker compose logs -f

genesis: ## Инициализировать данные (MongoDB, Redis, Memgraph)
	poetry run python genesis.py

api: ## Запустить FastAPI
	poetry run fastapi run api.py --host 127.0.0.1 --port 8000

test: ## Прогнать unit-тесты
	poetry run pytest

test-all: ## Прогнать все тесты, включая интеграционные
	poetry run pytest -m ""

lint: ## Проверка линтером
	poetry run ruff check .

format: ## Автоформатирование
	poetry run ruff format .
	poetry run ruff check . --fix

cons: ## Запустить consumer
	poetry run python run.py cons

conf: ## Запустить confirmer
	poetry run python run.py conf

write: ## Запустить writer
	poetry run python run.py write

tx: ## Запустить генерацию транзакций
	poetry run python tx_bot.py
