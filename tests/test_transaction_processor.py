"""Тесты для consumer_v2.py
└─$ poetry run pytest tests/ -v
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import asyncio
import json
import pytest
import pytest_asyncio
import fakeredis.aioredis
from unittest.mock import AsyncMock, patch
from aio_pika import Message, DeliveryMode
from aio_pika.abc import AbstractIncomingMessage
from src.models.tx import TxStore, TxWrite
from consumer_v2 import TransactionProcessor, HOME_TIME, MAX_ATTEMPTS

# Фикстура для создания тестового процессора
@pytest_asyncio.fixture
async def processor():
    proc = TransactionProcessor()
    # Мокаем зависимости
    proc.db = AsyncMock()
    proc.rabbitmq = AsyncMock()
    proc.rabbitmq.channel = AsyncMock()
    proc.rabbitmq.channel.default_exchange = AsyncMock()
    proc.mom = fakeredis.aioredis.FakeRedis(db=1)
    proc.dad = fakeredis.aioredis.FakeRedis(db=2)
    proc.home = fakeredis.aioredis.FakeRedis(db=3)
    try:
        yield proc
    finally:
        # Очистка после теста
        await proc.mom.flushdb()
        await proc.dad.flushdb()
        await proc.home.flushdb()
        await proc.mom.aclose()
        await proc.dad.aclose()
        await proc.home.aclose()

# Фикстура для создания тестового сообщения
@pytest_asyncio.fixture
def message():
    tx_data = {
        "content": {
            "mom": "mom_hash",
            "dad": "dad_hash",
            "credit": "credit_addr",
            "debit": "debit_addr",
            "amount": 100,
            "uniq": "unique_id",
            "msg": "test"
        },
        "sign": "signature"
    }
    msg = Message(
        body=json.dumps(tx_data).encode("utf-8"),
        headers={},
        delivery_mode=DeliveryMode.PERSISTENT
    )
    incoming_msg = AsyncMock(spec=AbstractIncomingMessage)
    incoming_msg.body = msg.body
    incoming_msg.headers = msg.headers
    incoming_msg.routing_key = "tx"
    incoming_msg.delivery_mode = msg.delivery_mode
    incoming_msg.ack = AsyncMock()
    incoming_msg.reject = AsyncMock()
    return incoming_msg

@pytest.mark.asyncio
async def test_is_duplicate_true(processor):
    processor.db.find_one.return_value = {"credit": "credit_addr", "uniq": "unique_id"}
    tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                 amount=100, uniq="unique_id", msg="test", sign="signature")
    assert await processor.is_duplicate(tx) is True

@pytest.mark.asyncio
async def test_is_duplicate_false(processor):
    processor.db.find_one.return_value = None
    tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                 amount=100, uniq="unique_id", msg="test", sign="signature")
    assert await processor.is_duplicate(tx) is False

@pytest.mark.asyncio
async def test_has_sufficient_balance_true(processor):
    # Патчим функцию, используемую в consumer_v2, а не в src.utils.balance
    with patch("consumer_v2.get_balance", new_callable=AsyncMock, return_value=200):
        tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                     amount=100, uniq="unique_id", msg="test", sign="signature")
        assert await processor.has_sufficient_balance(tx) is True

@pytest.mark.asyncio
async def test_has_sufficient_balance_false(processor):
    with patch("consumer_v2.get_balance", new_callable=AsyncMock, return_value=50):
        tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                     amount=100, uniq="unique_id", msg="test", sign="signature")
        assert await processor.has_sufficient_balance(tx) is False

@pytest.mark.asyncio
async def test_check_parents_both_exist_in_redis(processor):
    await processor.mom.set("mom_hash", "{}")
    await processor.dad.set("dad_hash", "{}")
    tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                 amount=100, uniq="unique_id", msg="test", sign="signature")
    mom_exists, dad_exists = await processor.check_parents(tx)
    assert mom_exists is True
    assert dad_exists is True

@pytest.mark.asyncio
async def test_check_parents_only_mom_in_mongo(processor):
    processor.db.find_one.side_effect = [{"hash": "mom_hash"}, None]
    tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                 amount=100, uniq="unique_id", msg="test", sign="signature")
    mom_exists, dad_exists = await processor.check_parents(tx)
    assert mom_exists is True
    assert dad_exists is False

@pytest.mark.asyncio
async def test_check_parents_none_exist(processor):
    processor.db.find_one.return_value = None
    tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                 amount=100, uniq="unique_id", msg="test", sign="signature")
    mom_exists, dad_exists = await processor.check_parents(tx)
    assert mom_exists is False
    assert dad_exists is False

@pytest.mark.asyncio
async def test_move_to_dad(processor):
    tx_hash = "mom_hash"
    pool_mom = json.dumps({"key": "value"}).encode("utf-8")
    await processor.mom.set(tx_hash, pool_mom)
    await processor.move_to_dad(tx_hash, pool_mom)
    assert await processor.mom.get(tx_hash) is None
    assert await processor.dad.get(tx_hash) == pool_mom
    assert await processor.dad.get("last") == tx_hash.encode("utf-8")

@pytest.mark.asyncio
async def test_move_to_home(processor):
    tx_hash = "mom_hash"
    pool_data = json.dumps({
        "mom": "parent_mom",
        "dad": "parent_dad",
        "credit": "credit_addr",
        "debit": "debit_addr",
        "amount": 100,
        "uniq": "unique_id",
        "time": 123,
        "sign": "signature"
    }).encode("utf-8")
    processor.db.insert_tx = AsyncMock(return_value=True)
    await processor.dad.set(tx_hash, pool_data)
    await processor.move_to_home(tx_hash, pool_data, is_mom=True)
    assert await processor.dad.get(tx_hash) is None
    assert await processor.home.get(tx_hash) == pool_data
    processor.db.insert_tx.assert_called_once()

@pytest.mark.asyncio
async def test_cleanup_home(processor):
    tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                 amount=100, uniq="unique_id", msg="test", sign="signature")
    old_time = int(time.time_ns() / 1000) - HOME_TIME - 1
    await processor.home.set("mom_hash", json.dumps({"time": old_time, "sign": "signature"}).encode("utf-8"))
    await processor.home.set("dad_hash", json.dumps({"time": int(time.time_ns() / 1000), "sign": "signature"}).encode("utf-8"))
    await processor.cleanup_home(tx, int(time.time_ns() / 1000))
    assert await processor.home.get("mom_hash") is None
    assert await processor.home.get("dad_hash") is not None

@pytest.mark.asyncio
async def test_process_transaction(processor):
    tx = TxStore(mom="mom_hash", dad="dad_hash", credit="credit_addr", debit="debit_addr",
                 amount=100, uniq="unique_id", msg="test", sign="signature")
    tx_hash = "tx_hash"
    await processor.process_transaction(tx, tx_hash)
    stored_data = await processor.mom.get(tx_hash)
    assert stored_data is not None
    assert json.loads(stored_data.decode("utf-8"))["uniq"] == "unique_id"
    assert await processor.mom.get("last") == tx_hash.encode("utf-8")

@pytest.mark.asyncio
async def test_requeue_message(processor, message):
    await processor.requeue_message(message, 0)
    processor.rabbitmq.channel.default_exchange.publish.assert_called_once()
    call_args = processor.rabbitmq.channel.default_exchange.publish.call_args
    assert call_args[0][0].headers["x-attempts"] == 1

@pytest.mark.asyncio
async def test_on_message_duplicate(processor, message):
    with patch.object(processor, "is_duplicate", AsyncMock(return_value=True)):
        await processor.on_message(message)
    message.ack.assert_called_once()

@pytest.mark.asyncio
async def test_on_message_insufficient_balance(processor, message):
    with patch.object(processor, "is_duplicate", AsyncMock(return_value=False)):
        with patch.object(processor, "has_sufficient_balance", AsyncMock(return_value=False)):
            await processor.on_message(message)
    message.reject.assert_called_once_with(requeue=False)

@pytest.mark.asyncio
async def test_on_message_no_parents(processor, message):
    with patch.object(processor, "is_duplicate", AsyncMock(return_value=False)):
        with patch.object(processor, "has_sufficient_balance", AsyncMock(return_value=True)):
            with patch.object(processor, "check_parents", AsyncMock(return_value=(False, False))):
                await processor.on_message(message)
    message.reject.assert_called_once_with(requeue=False)
    processor.rabbitmq.channel.default_exchange.publish.assert_called_once()

@pytest.mark.asyncio
async def test_on_message_max_attempts(processor, message):
    message.headers = {"x-attempts": MAX_ATTEMPTS}
    await processor.on_message(message)
    message.reject.assert_called_once_with(requeue=False)
    processor.rabbitmq.channel.default_exchange.publish.assert_not_called()

@pytest.mark.asyncio
async def test_on_message_success(processor, message):
    pool_data = json.dumps({
        "mom": "mom_hash",
        "dad": "dad_hash",
        "credit": "credit_addr",
        "debit": "debit_addr",
        "amount": 100,
        "uniq": "unique_id",
        "sign": "signature",
        "time": 123
    }).encode("utf-8")
    with patch.object(processor, "is_duplicate", AsyncMock(return_value=False)):
        with patch.object(processor, "has_sufficient_balance", AsyncMock(return_value=True)):
            with patch.object(processor, "check_parents", AsyncMock(return_value=(True, True))):
                await processor.mom.set("mom_hash", pool_data)
                await processor.dad.set("dad_hash", pool_data)
                await processor.on_message(message)
    message.ack.assert_called_once()
    assert await processor.mom.get("last") is not None

@pytest.mark.asyncio
async def test_run_exception_handling(processor):
    processor.rabbitmq.consume_messages = AsyncMock(side_effect=Exception("Test error"))
    # Замокаем методы aclose, чтобы можно было проверить вызовы
    processor.mom.aclose = AsyncMock()
    processor.dad.aclose = AsyncMock()
    processor.home.aclose = AsyncMock()
    with pytest.raises(Exception, match="Test error"):
        await processor.run()
    processor.rabbitmq.close.assert_called_once()
    processor.db.close.assert_called_once()
    processor.mom.aclose.assert_called_once()
    processor.dad.aclose.assert_called_once()
    processor.home.aclose.assert_called_once()
