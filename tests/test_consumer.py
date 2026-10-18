"""Тесты для consumer.py
└─$ poetry run pytest tests/ -v
"""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika import IncomingMessage

from service.consumer import ConsumerProcessor, ConsumerService
from src.database.graph import Memgraph
from src.database.mongodb import MongoDB
from src.database.queues import RabbitMQ
from src.database.redis import Redis
from src.models.tx import TxStore


# Фикстура для создания тестового объекта TxStore
@pytest.fixture
def tx_store():
    return TxStore(
        credit="EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1",
        debit="EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU2",
        amount=100,
        uniq="tx123",
        parents=["parent1", "parent2", "parent3"],
        msg="test",
        sign="signature",
    )


# Фикстура для создания мока сообщения
@pytest.fixture
def message(tx_store):
    message_data = {
        "content": {
            "credit": tx_store.credit,
            "debit": tx_store.debit,
            "amount": tx_store.amount,
            "uniq": tx_store.uniq,
            "parents": tx_store.parents,
            "msg": tx_store.msg,
        },
        "sign": tx_store.sign,
    }
    message = MagicMock(spec=IncomingMessage)
    message.body = json.dumps(message_data).encode("utf-8")
    message.nack = AsyncMock()
    message.ack = AsyncMock()
    return message


# Фикстура для создания ConsumerProcessor
@pytest.fixture
def processor():
    db = MagicMock(spec=MongoDB)
    rabbitmq = MagicMock(spec=RabbitMQ)
    redis = MagicMock(spec=Redis)
    dag = MagicMock(spec=Memgraph)
    redis.client = MagicMock()

    @asynccontextmanager
    async def lock(_key):
        yield

    redis.client.lock = lock
    rabbitmq.publish_to_dlq = AsyncMock()
    redis.client.set = AsyncMock(return_value=True)
    redis.client.delete = AsyncMock()
    return ConsumerProcessor(rabbitmq, db, redis, dag)


# Тесты для ConsumerProcessor
@pytest.mark.asyncio
async def test_parse_message(processor, message, tx_store):
    """Тест парсинга сообщения в TxStore"""
    result = processor.parse_message(message)
    assert isinstance(result, TxStore)
    assert result.credit == tx_store.credit
    assert result.uniq == tx_store.uniq
    assert result.sign == tx_store.sign


@pytest.mark.asyncio
async def test_parse_message_invalid_json(processor, message):
    """Тест обработки некорректного JSON"""
    message.body = b"invalid_json"
    with pytest.raises(json.JSONDecodeError):
        processor.parse_message(message)


@pytest.mark.asyncio
async def test_check_duplicate_exists(processor, tx_store):
    """Тест проверки дубликатов (транзакция существует в реестре)"""
    processor.redis.client.set = AsyncMock(return_value=True)
    processor.redis.client.delete = AsyncMock()
    processor.db.find_one = AsyncMock(return_value={"credit": tx_store.credit, "uniq": tx_store.uniq})
    result = await processor.check_duplicate(tx_store)
    assert result is True


@pytest.mark.asyncio
async def test_check_duplicate_not_exists(processor, tx_store):
    """Тест проверки дубликатов (транзакция не существует)"""
    processor.redis.client.set = AsyncMock(return_value=True)
    processor.redis.client.delete = AsyncMock()
    processor.db.find_one = AsyncMock(return_value=None)
    result = await processor.check_duplicate(tx_store)
    assert result is False


@pytest.mark.asyncio
async def test_check_duplicate_uniq_already_reserved(processor, tx_store):
    """Тест проверки дубликатов (uniq уже зарезервирован в Redis)"""
    processor.redis.client.set = AsyncMock(return_value=None)
    result = await processor.check_duplicate(tx_store)
    assert result is True


@pytest.mark.asyncio
async def test_verify_balance_sufficient(processor, tx_store):
    """Тест проверки достаточного баланса"""
    with patch("service.consumer.get_balance", AsyncMock(return_value=200)):
        result = await processor.verify_balance(tx_store)
        assert result is True


@pytest.mark.asyncio
async def test_verify_balance_insufficient(processor, tx_store):
    """Тест проверки недостаточного баланса"""
    with patch("service.consumer.get_balance", AsyncMock(return_value=50)):
        result = await processor.verify_balance(tx_store)
        assert result is False


@pytest.mark.asyncio
async def test_verify_parents_all_exist(processor, tx_store):
    """Тест проверки наличия всех родителей"""
    processor.verify_parents = AsyncMock(return_value=True)
    result = await processor.verify_parents("hash", tx_store)
    assert result is True


@pytest.mark.asyncio
async def test_verify_parents_missing(processor, tx_store):
    """Тест проверки отсутствия родителей"""
    processor.verify_parents = AsyncMock(return_value=False)
    processor.verify_parents = AsyncMock(return_value=False)
    result = await processor.verify_parents("hash", tx_store)
    assert result is False


@pytest.mark.asyncio
async def test_add_to_graph_no_cycle(processor, tx_store):
    """Тест добавления транзакции в граф без цикла"""
    processor.add_to_graph = AsyncMock(return_value=True)
    tx_hash = "tx_hash_123"
    result = await processor.add_to_graph(tx_hash, tx_store)
    assert result is True
    assert result is True


@pytest.mark.asyncio
async def test_add_to_graph_cycle_detected(processor, tx_store):
    """Тест обнаружения цикла при добавлении в граф"""
    processor.add_to_graph = AsyncMock(return_value=False)
    result = await processor.add_to_graph("tx_hash_123", tx_store)
    assert result is False


@pytest.mark.asyncio
async def test_process_transaction_duplicate(processor, message, tx_store):
    """Тест обработки транзакции с дубликатом"""
    processor._fetch_graph_data = AsyncMock(return_value=None)
    processor.check_duplicate = AsyncMock(return_value=True)
    await processor.process_transaction(tx_store, message)
    message.nack.assert_called_once_with(requeue=False)
    message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_process_transaction_insufficient_balance(processor, message, tx_store):
    """Тест обработки транзакции с недостаточным балансом"""
    processor._fetch_graph_data = AsyncMock(return_value=None)
    processor.check_duplicate = AsyncMock(return_value=False)
    processor.verify_balance = AsyncMock(return_value=False)
    await processor.process_transaction(tx_store, message)
    message.nack.assert_called_once_with(requeue=False)
    message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_process_transaction_missing_parents(processor, message, tx_store):
    """Тест обработки транзакции с отсутствующими родителями"""
    processor._fetch_graph_data = AsyncMock(return_value=None)
    processor.check_duplicate = AsyncMock(return_value=False)
    processor.verify_balance = AsyncMock(return_value=True)
    processor.verify_parents = AsyncMock(return_value=False)
    await processor.process_transaction(tx_store, message)
    message.nack.assert_called_once_with(requeue=False)
    message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_process_transaction_success(processor, message, tx_store):
    """Тест успешной обработки транзакции: запись в граф → mid → ack"""
    processor._fetch_graph_data = AsyncMock(return_value=None)
    processor.check_duplicate = AsyncMock(return_value=False)
    processor.verify_balance = AsyncMock(return_value=True)
    processor.verify_parents = AsyncMock(return_value=True)
    processor.add_to_graph = AsyncMock(return_value=True)
    processor._publish_mid = AsyncMock(return_value=True)
    await processor.process_transaction(tx_store, message)
    message.ack.assert_called_once()
    message.nack.assert_not_called()
    # mid публикуется ДО ack — это гарантия no-loss при сбое публикации.
    processor._publish_mid.assert_called_once()


@pytest.mark.asyncio
async def test_process_transaction_mid_publish_failure_requeues(processor, message, tx_store):
    """Сбой публикации mid: сообщение НЕ подтверждается, возвращается в очередь.

    Узел уже записан в граф — uniq-резерв остаётся guard'ом от повторной вставки.
    """
    processor._fetch_graph_data = AsyncMock(return_value=None)
    processor.check_duplicate = AsyncMock(return_value=False)
    processor.verify_balance = AsyncMock(return_value=True)
    processor.verify_parents = AsyncMock(return_value=True)
    processor.add_to_graph = AsyncMock(return_value=True)
    processor._publish_mid = AsyncMock(return_value=False)
    processor.release_uniq = AsyncMock()
    await processor.process_transaction(tx_store, message)
    message.nack.assert_called_once_with(requeue=True)
    message.ack.assert_not_called()
    # Узел записан — резерв uniq снимать НЕЛЬЗЯ.
    processor.release_uniq.assert_not_called()


@pytest.mark.asyncio
async def test_process_transaction_retry_after_insert(processor, message, tx_store):
    """Recovery-путь: транзакция уже в графе — перепубликуем mid и ack, без второго узла."""
    existing = dict(tx_store, time=12345)
    processor._fetch_graph_data = AsyncMock(return_value=existing)
    processor._retry_inserted = AsyncMock()
    await processor.process_transaction(tx_store, message)
    processor._retry_inserted.assert_called_once()
    message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_retry_inserted_mismatch_goes_to_dlq(processor, message, tx_store):
    """Конфликт данных при recovery: хэш совпал, но данные не те — в DLQ, второй узел не создаём."""
    existing = dict(tx_store, time=12345)
    existing["amount"] = 999  # расхождение данных при том же хэше
    processor._reject = AsyncMock()
    await processor._retry_inserted("a" * 128, tx_store, existing, message)
    processor._reject.assert_called_once()
    message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_retry_inserted_success(processor, message, tx_store):
    """Recovery: данные совпали, рёбра целы, mid перепубликован — ack без дубля узла."""
    existing = dict(tx_store, time=12345)
    processor._ensure_edges = AsyncMock(return_value=True)
    processor._publish_mid = AsyncMock(return_value=True)
    processor.add_to_graph = AsyncMock()
    await processor._retry_inserted("a" * 128, tx_store, existing, message)
    message.ack.assert_called_once()
    message.nack.assert_not_called()
    # Новый узел не создаётся.
    processor.add_to_graph.assert_not_called()


# Тесты для ConsumerService
@pytest.mark.asyncio
async def test_consumer_service_run():
    """Тест запуска и завершения сервиса"""
    service = ConsumerService()
    service.redis.connect = AsyncMock()
    service.rabbit.connect = AsyncMock()
    service.rabbit.connection = MagicMock(is_closed=False)
    service.db.connect = AsyncMock()
    service.rabbit.consume_messages = AsyncMock()
    service.redis.close = AsyncMock()
    service.rabbit.close = AsyncMock()
    service.db.close = AsyncMock()

    async def run_with_exception():
        await service.startup()
        raise Exception("Simulated error")

    with pytest.raises(Exception, match="Simulated error"):
        await run_with_exception()

    await service.shutdown()
    service.redis.connect.assert_called_once()
    service.rabbit.connect.assert_called_once()
    service.db.connect.assert_called_once()
    service.redis.close.assert_called_once()
    service.rabbit.close.assert_called_once()
    service.db.close.assert_called_once()


@pytest.mark.asyncio
async def test_consumer_service_process_message_error(processor, message):
    """Тест обработки сообщения с ошибкой"""
    message.body = b"invalid_json"
    await processor.process_message(message)
    message.nack.assert_called_once_with(requeue=False)
