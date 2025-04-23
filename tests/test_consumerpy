"""Тесты для consumer.py
└─$ poetry run pytest tests/ -v
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from aio_pika import IncomingMessage
import networkx as nx
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.utils.graph import Dag
from src.models.tx import TxStore
from service.consumer import ConsumerProcessor, ConsumerService

# Фикстура для создания тестового объекта TxStore
@pytest.fixture
def tx_store():
    return TxStore(
        credit="user1",
        debit="user2",
        amount=100,
        uniq="tx123",
        parents=["parent1", "parent2"],
        msg="test_message",
        sign="signature"
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
            "msg": tx_store.msg
        },
        "sign": tx_store.sign
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
    dag = Dag()
    dag.G = nx.DiGraph()
    return ConsumerProcessor(db, rabbitmq, redis, dag)

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
    """Тест проверки дубликатов (транзакция существует)"""
    processor.db.find_one = AsyncMock(return_value={"credit": tx_store.credit, "uniq": tx_store.uniq})
    result = await processor.check_duplicate(tx_store)
    assert result is True

@pytest.mark.asyncio
async def test_check_duplicate_not_exists(processor, tx_store):
    """Тест проверки дубликатов (транзакция не существует)"""
    processor.db.find_one = AsyncMock(return_value=None)
    result = await processor.check_duplicate(tx_store)
    assert result is False

@pytest.mark.asyncio
async def test_verify_balance_sufficient(processor, tx_store):
    """Тест проверки достаточного баланса"""
    with patch("consumer.get_balance", AsyncMock(return_value=200)):
        result = await processor.verify_balance(tx_store)
        assert result is True

@pytest.mark.asyncio
async def test_verify_balance_insufficient(processor, tx_store):
    """Тест проверки недостаточного баланса"""
    with patch("consumer.get_balance", AsyncMock(return_value=50)):
        result = await processor.verify_balance(tx_store)
        assert result is False

@pytest.mark.asyncio
async def test_verify_parents_all_exist(processor, tx_store):
    """Тест проверки наличия всех родителей"""
    processor.dag.G.add_nodes_from(tx_store.parents)
    result = await processor.verify_parents(tx_store)
    assert result is True

@pytest.mark.asyncio
async def test_verify_parents_missing(processor, tx_store):
    """Тест проверки отсутствия родителей"""
    result = await processor.verify_parents(tx_store)
    assert result is False

@pytest.mark.asyncio
async def test_add_to_graph_no_cycle(processor, tx_store):
    """Тест добавления транзакции в граф без цикла"""
    processor.dag.G.add_nodes_from(tx_store.parents)
    tx_hash = "tx_hash_123"
    result = await processor.add_to_graph(tx_hash, tx_store)
    assert result is True
    assert tx_hash in processor.dag.G.nodes
    assert all(processor.dag.G.has_edge(parent, tx_hash) for parent in tx_store.parents)

@pytest.mark.asyncio
async def test_add_to_graph_cycle_detected(processor, tx_store):
    """Тест обнаружения цикла при добавлении в граф"""
    tx_hash = "tx_hash_123"
    processor.dag.G.add_node(tx_hash)
    processor.dag.G.add_edge(tx_hash, tx_store.parents[0])  # Создаём потенциальный цикл
    result = await processor.add_to_graph(tx_hash, tx_store)
    assert result is False

@pytest.mark.asyncio
async def test_process_transaction_duplicate(processor, message, tx_store):
    """Тест обработки транзакции с дубликатом"""
    processor.check_duplicate = AsyncMock(return_value=True)
    await processor.process_transaction(tx_store, message)
    message.nack.assert_called_once_with(requeue=False)
    message.ack.assert_not_called()

@pytest.mark.asyncio
async def test_process_transaction_insufficient_balance(processor, message, tx_store):
    """Тест обработки транзакции с недостаточным балансом"""
    processor.check_duplicate = AsyncMock(return_value=False)
    processor.verify_balance = AsyncMock(return_value=False)
    await processor.process_transaction(tx_store, message)
    message.nack.assert_called_once_with(requeue=False)
    message.ack.assert_not_called()

@pytest.mark.asyncio
async def test_process_transaction_missing_parents(processor, message, tx_store):
    """Тест обработки транзакции с отсутствующими родителями"""
    processor.check_duplicate = AsyncMock(return_value=False)
    processor.verify_balance = AsyncMock(return_value=True)
    processor.verify_parents = AsyncMock(return_value=False)
    await processor.process_transaction(tx_store, message)
    message.nack.assert_called_once_with(requeue=False)
    message.ack.assert_not_called()

@pytest.mark.asyncio
async def test_process_transaction_success(processor, message, tx_store):
    """Тест успешной обработки транзакции"""
    processor.check_duplicate = AsyncMock(return_value=False)
    processor.verify_balance = AsyncMock(return_value=True)
    processor.verify_parents = AsyncMock(return_value=True)
    processor.add_to_graph = AsyncMock(return_value=True)
    await processor.process_transaction(tx_store, message)
    message.ack.assert_called_once()
    message.nack.assert_not_called()

# Тесты для ConsumerService
@pytest.mark.asyncio
async def test_consumer_service_run():
    """Тест запуска и завершения сервиса"""
    service = ConsumerService()
    service.redis.connect = AsyncMock()
    service.rabbitmq.connect = AsyncMock()
    service.db.connect = AsyncMock()
    service.dag.load = AsyncMock()
    service.rabbitmq.consume_messages = AsyncMock()
    service.redis.close = AsyncMock()
    service.rabbitmq.close = AsyncMock()
    service.db.close = AsyncMock()

    async def run_with_exception():
        await service.startup()
        raise Exception("Simulated error")

    with pytest.raises(Exception, match="Simulated error"):
        await run_with_exception()

    await service.shutdown()
    service.redis.connect.assert_called_once()
    service.rabbitmq.connect.assert_called_once()
    service.db.connect.assert_called_once()
    service.dag.load.assert_called_once()
    service.redis.close.assert_called_once()
    service.rabbitmq.close.assert_called_once()
    service.db.close.assert_called_once()

@pytest.mark.asyncio
async def test_consumer_service_process_message_error(processor, message):
    """Тест обработки сообщения с ошибкой"""
    message.body = b"invalid_json"
    await processor.process_message(message)
    message.nack.assert_called_once_with(requeue=False)
