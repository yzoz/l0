# l0-python

Прототип распределённого реестра транзакций на DAG. Проект использует FastAPI и Strawberry GraphQL для API, RabbitMQ для очередей, MongoDB для хранения подтверждённых транзакций, Redis для кэширования и Memgraph для графа транзакций. Подписи транзакций проверяются с помощью Ed25519 (PyNaCl).

## Возможности

- REST API и GraphQL для работы с транзакциями.
- Асинхронная обработка через RabbitMQ.
- Проверка цифровых подписей и обработка графа транзакций.
- Сервисы записи и подтверждения, а также утилиты для тестовой нагрузки.

Это исследовательский прототип, а не готовая финансовая система. Не используйте его для хранения реальных средств или секретов.

## Требования

- Python 3.12+
- Poetry 2.x
- Docker Compose v2

## Настройка и запуск

```bash
git clone https://github.com/feonyxlab/l0-python.git
cd l0-python
poetry install
cp .env.example .env
```

Замените все значения в `.env` уникальными credentials. Не публикуйте файл `.env`. При публичном запуске InfluxDB credentials должны быть заменены, а ранее раскрытые значения — отозваны/ротированы.

Запустите локальные зависимости (порты привязаны к loopback):

```bash
docker compose up -d
```

Инициализируйте данные при необходимости и запустите API:

```bash
poetry run python genesis.py
poetry run fastapi run api.py --host 127.0.0.1 --port 8000
```

- REST/OpenAPI: <http://127.0.0.1:8000/docs>
- GraphQL: <http://127.0.0.1:8000/graphql>

Фоновые процессы запускаются отдельно; замените `SERVICE` на `cons`, `conf` или `write`:

```bash
poetry run python run.py SERVICE [N-processes]
```

Остановка зависимостей: `docker compose down`. Для удаления также сохранённых данных используйте `docker compose down -v`.

## Тесты

Обычный набор использует моки и не требует запущенных баз данных:

```bash
poetry run pytest
```

Интеграционные тесты помечены `integration` и исключены из обычного прогона. Для теста балансов сначала запустите зависимости, затем выполните:

```bash
poetry run pytest -m integration
```

## Структура

- `api.py` — приложение FastAPI.
- `src/api/` — REST и GraphQL маршруты.
- `src/database/` — клиенты MongoDB, Redis, RabbitMQ и Memgraph.
- `service/` — consumer, confirmer и writer.
- `tests/` — unit и интеграционные тесты.
- `docker-compose.yaml` — локальные сервисы разработки.

## Лицензия

MIT. См. [`LICENSE`](LICENSE).
