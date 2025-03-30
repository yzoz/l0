# l0-python

![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Version](https://img.shields.io/badge/Version-0.0.12-orange.svg)

`l0-python` — это экспериментальный проект блокчейна уровня 0, реализованный на Python. Он предоставляет инфраструктуру для обработки транзакций, токенов и NFT с использованием криптографических подписей, асинхронной обработки через RabbitMQ и хранения данных в MongoDB.

## Основные возможности
- Создание и валидация транзакций с использованием библиотеки NaCl.
- Поддержка токенов и NFT с метаданными (изображения, описания).
- Асинхронная обработка транзакций через RabbitMQ.
- Хранение данных в MongoDB с поддержкой асинхронных запросов.
- RESTful API (FastAPI) и GraphQL интерфейс (Strawberry).
- Утилиты для генерации ключей и тестовых транзакций.

## Требования
- **Python**: 3.12+
- **Docker**: Для запуска MongoDB, RabbitMQ, Redis и InfluxDB
- **Poetry**: Для управления зависимостями

## Установка

### 1. Клонирование репозитория
```bash
git clone https://github.com/yourusername/l0-python.git
cd l0-python
```

### 2. Установка зависимостей
Установите Poetry, если его нет:
```bash
pip install poetry
```

Установите зависимости проекта:
```bash
poetry install
```

Активируйте виртуальное окружение:
```bash
poetry shell
```

### 3. Настройка секретов для InfluxDB
Создайте файлы секретов в корневой директории:
- `.env.influxdb2-admin-username`:
  ```
  admin
  ```
- `.env.influxdb2-admin-password`:
  ```
  00000000
  ```
- `.env.influxdb2-admin-token`:
  ```
  MyInitialAdminToken0==
  ```

## Структура проекта
```
l0-python/
│
├── src/                    # Основной исходный код
│   ├── __init__.py         # Пустой файл для обозначения модуля
│   ├── models/             # Модели данных
│   │   ├── __init__.py     # Пустой файл для модуля
│   │   ├── tx.py           # Модель транзакции (Tx) с полями credit, debit, amount и др.
│   │   ├── token.py        # Модель токена (Token) для создания токенов с тикером и количеством
│   │   └── nft.py          # Модель NFT с метаданными (изображение, описание)
│   ├── utils/              # Утилиты и вспомогательные функции
│   │   ├── __init__.py     # Пустой файл для модуля
│   │   ├── crypto.py       # Функции для генерации ключей, подписи и верификации (NaCl)
│   │   └── hash.py         # Хэширование данных для создания уникальных идентификаторов
│   ├── database/           # Логика работы с базами данных
│   │   ├── __init__.py     # Пустой файл для модуля
│   │   ├── mongodb.py      # Подключение и операции с MongoDB (асинхронные и синхронные)
│   │   └── queues.py       # Работа с RabbitMQ (публикация и потребление сообщений)
│   ├── api/                # API и GraphQL сервер
│   │   ├── __init__.py     # Пустой файл для модуля
│   │   ├── main.py         # Основной FastAPI сервер с эндпоинтами /send/ и GraphQL
│   │   └── schema.py       # GraphQL схема для запросов транзакций
│   ├── scripts/            # Скрипты для генерации данных и тестирования
│   │   ├── __init__.py     # Пустой файл для модуля
│   │   ├── tx_generator.py # Генерация тестовых транзакций с случайными данными
│   │   └── key_generator.py# Генерация публичных и приватных ключей
│   └── worker/             # Обработчик сообщений
│       ├── __init__.py     # Пустой файл для модуля
│       └── consumer.py     # Consumer для RabbitMQ, записывающий транзакции в MongoDB
│
├── pyproject.toml          # Конфигурация проекта и зависимостей (Poetry)
├── docker-compose.yml      # Конфигурация сервисов (RabbitMQ, MongoDB, Redis, InfluxDB)
├── README.md               # Документация проекта
```

## Запуск проекта

### 1. Запуск сервисов
Запустите MongoDB, RabbitMQ, Redis и InfluxDB через Docker:
```bash
docker-compose up -d
```

Проверьте состояние:
```bash
docker ps
```

### 2. Запуск API
Запустите FastAPI сервер:
```bash
poetry run fast
```

- REST API: `http://127.0.0.1:8000`
- GraphQL: `http://127.0.0.1:8000/graphql`

### 3. Запуск Worker
В отдельном терминале запустите обработчик RabbitMQ:
```bash
python src/worker/consumer.py
```

### 4. Тестирование
Отправьте тестовую транзакцию:
```bash
curl -X POST "http://127.0.0.1:8000/send/" \
     -H "Content-Type: application/json" \
     -d '{"credit": "EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1", "debit": "A9huRv7vtELAYQ4nQYtEWfn5NEdqXBUs722TdsiZR3ST", "asset": "0", "amount": 100.0, "uniq": "12345", "sign": "ZAjPpYqRToeE6dkXFNdA8XMEZHC8R3Wo12255ZFDWDAiqkWZrNVt2Y3FoNqBhv2VYHDc98J94yyVTuedFw9X2i8SDpju2hbxb", "msg": "test"}'
```

Проверьте данные в MongoDB:
```bash
docker exec -it mongo mongosh
```
```javascript
use feo
db.tx.find()
```

Проверьте RabbitMQ: `http://localhost:15672` (логин/пароль: `guest`/`guest`).

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

## Генерация данных
- **Ключи**: 
  ```bash
  python src/scripts/key_generator.py
  ```
- **Транзакции**: 
  ```bash
  python src/scripts/tx_generator.py
  ```

## Зависимости
- `fastapi`: RESTful API
- `pynacl`: Криптография
- `base58`: Кодирование ключей
- `strawberry-graphql`: GraphQL
- `motor`: MongoDB (асинхронный)
- `aio-pika` и `pika`: RabbitMQ

См. `pyproject.toml` для полного списка.

## Лицензия
MIT (см. `pyproject.toml`).

## Авторы
- **YZoZ** (mail@yzoz.com)
- **GendByteMaster** ([GendByteMaster](https://github.com/GendByteMaster/))