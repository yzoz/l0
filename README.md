# l0-python

![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Version](https://img.shields.io/badge/Version-0.1.0-orange.svg)

Проект представляет собой реализацию открытой, неизменяемой и доказуемой бухгалтерской книги на основе направленного ациклического графа (DAG). Это децентрализованная система для обработки транзакций, использующая Memgraph (графовая база данных), MongoDB (хранилище подтверждённых транзакций), Redis (кэширование и блокировки), RabbitMQ (очереди сообщений), InfluxDB (аналитика и временные ряды) и FastAPI с GraphQL для API. Основная цель — обеспечить надёжную, масштабируемую и производительную систему для обработки криптовалютных транзакций с использованием криптографических подписей для безопасности.

## Основные возможности
- Создание и валидация транзакций с использованием библиотеки PyNaCl (Ed25519).
- Асинхронная обработка транзакций через RabbitMQ.
- Хранение данных в MongoDB с поддержкой асинхронных запросов.
- Управление DAG (Directed Acyclic Graph) в Memgraph для подтверждения транзакций.
- RESTful API (FastAPI) и GraphQL интерфейс (Strawberry).
- Утилиты для генерации ключей, тестовых транзакций и ботов для нагрузочного тестирования.
- Кэширование балансов в Redis для оптимизации производительности.
- Поддержка весов транзакций для консенсуса на основе DAG.

## Требования
- **Python**: 3.12+
- **Docker**: Для запуска MongoDB, RabbitMQ, Redis, Memgraph и InfluxDB
- **Poetry**: Для управления зависимостями

## Установка

### 1. Клонирование репозитория
```bash
git clone https://github.com/feonyxlab/l0-python.git
cd l0-python
```

### 2. Установка зависимостей
Установите Poetry, если его нет:
```bash
pipx install poetry
```

Активируйте виртуальное окружение:
```bash
python3.12 -m venv env
source env/bin/activate
```

Установите зависимости проекта:
```bash
poetry install
```

## Структура проекта
```
l0-python/
│
├── src/                    # Основной исходный код
│   ├── api/                # API и GraphQL сервер
│   │   ├── schema.py       # GraphQL схема для запросов транзакций
│   │   ├── pool.py         # Эндпоинт для получения неподтверждённых транзакций
│   │   └── transactions.py # Эндпоинт для отправки транзакций
│   ├── database/           # Логика работы с базами данных
│   │   ├── mongodb.py      # Асинхронные операции с MongoDB
│   │   ├── redis.py        # Кэширование в Redis
│   │   ├── graph.py        # Работа с Memgraph (DAG)
│   │   └── queues.py       # Публикация и потребление сообщений в RabbitMQ
│   ├── models/             # Модели данных
│   │   └── tx.py           # Модели транзакций (Tx, TxRead, TxStore, TxWrite, TxSend)
│   ├── utils/              # Утилиты
│   │   ├── crypto.py       # Криптография (генерация ключей, подписи, верификация)
│   │   ├── data.py         # Хэширование и сериализация данных
│   └── └── profile.py      # Получение балансов
├── service/                # Микросервисы
│   ├── consumer.py         # Consumer для RabbitMQ, добавление транзакций в граф
│   ├── writer.py           # Подтверждение транзакций и запись в MongoDB
│   └── confirmer.py        # Пересчёт весов транзакций в DAG
├── api.py                  # Точка входа в FastAPI
├── config/                 # Конфигурации для docker
├── tests/                  # Тесты
├── pyproject.toml          # Конфигурация Poetry
├── docker-compose.yml      # Docker-сервисы (MongoDB, RabbitMQ, Redis, Memgraph, InfluxDB)
├── genesis.py              # Генерация начальных транзакций
├── keys.py                 # Тестовые ключи
├── tx_bot.py               # Генерация тестовых транзакций
├── run.py                  # Продакшн раннер
├── run.d.py                # Раннер для разработки (не работает:)
└── README.md               # Документация
```

## Запуск проекта

### 1. Запуск сервисов
Запустите MongoDB, RabbitMQ, Redis, Memgraph и InfluxDB через Docker:
```bash
docker compose up -d
```

### 2. Генезис
Сгенерируйте начальные транзакции:
```bash
python genesis.py
```

### 2. Запуск API
Запустите FastAPI сервер:
```bash
fastapi run api.py --workers=3 --reload
```

- REST API: `http://127.0.0.1:8000/docs`
- GraphQL: `http://127.0.0.1:8000/graphql`

### 3. Запуск Service
В отдельных терминалах запустите сервисы:
```bash
python run.py SERVICE N-processes
```

## Тестирование
Бот:
```bash
python tx_bot.py N-tx N-coroutines N-processes
```
## Сервисы
- Memgraph Lab: `http://localhost:3000` (host: `memgraph`)
- RabbitMQ: `http://localhost:15672` (логин/пароль: `guest`/`guest`)
- InfluxDB: `http://localhost:8086` (логин/пароль: `admin`/`00000000`)
- MongoDB: [Compass](https://www.mongodb.com/products/tools/compass)
- Redis: [Insight](https://redis.io/insight/)

## Пример GraphQL запроса
Запросите транзакции через GraphQL:
```graphql
query {
  getTx(addr: "EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1", limit: 10) {
    credit
    debit
    amount
    uniq
    msg
  }
}
```

## Зависимости
- `fastapi`: RESTful API.
- `pynacl`: Криптография (Ed25519).
- `base58`: Кодирование ключей.
- `strawberry-graphql`: GraphQL сервер.
- `aio-pika`: Асинхронная работа с RabbitMQ.
- `pymongo`: Асинхронные операции с MongoDB.
- `redis`: Кэширование.
- `neo4j`: Работа с Memgraph.

Полный список см. в `pyproject.toml`.

## Алгоритм работы
1. **Отправка транзакции** (`/send/`):
   - Проверка подписи (Ed25519).
   - Публикация в RabbitMQ.
2. **Consumer**:
   - Проверяет дубликаты, баланс, наличие родителей.
   - Добавляет транзакцию в DAG (Memgraph).
   - Проверяет отсутствие циклов.
3. **Confirmer**:
   - Пересчитывает веса транзакций в DAG на основе глубины и затухания.
4. **Writer**:
   - Подтверждает транзакции с весом ≥ 10.
   - Записывает в MongoDB, обновляет балансы в Redis, удаляет из DAG.

## Лицензия
MIT
