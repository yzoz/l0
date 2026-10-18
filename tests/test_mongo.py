from unittest.mock import AsyncMock, MagicMock

import pytest
from pymongo.results import InsertManyResult, InsertOneResult

from src.database.mongodb import MongoDB
from src.models.tx import TxWrite


# Фикстура для TxWrite
@pytest.fixture
def tx_write():
    return TxWrite(
        credit="EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1",
        debit="EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU2",
        amount=100,
        uniq="tx123",
        parents=["parent1", "parent2", "parent3"],
        msg="test",
        sign="signature",
        hash="tx_hash_123",
        time=1234567890,
    )


# Фикстура для MongoDB
@pytest.fixture
def mongodb():
    db = MongoDB()
    db.client = MagicMock()
    db.db = MagicMock()
    db.tx = MagicMock()
    return db


@pytest.mark.asyncio
async def test_connect(mongodb, monkeypatch):
    """Тест подключения к MongoDB"""
    mock_client = MagicMock()
    monkeypatch.setattr("pymongo.AsyncMongoClient", mock_client)
    await mongodb.connect()
    assert mongodb.client is not None
    assert mongodb.db is not None
    assert mongodb.tx is not None


@pytest.mark.asyncio
async def test_insert_tx_success(mongodb, tx_write):
    """Тест успешной вставки одной транзакции"""
    mock_result = InsertOneResult(inserted_id="123", acknowledged=True)
    mongodb.tx.insert_one = AsyncMock(return_value=mock_result)
    result = await mongodb.insert_tx(tx_write)
    assert result is True
    mongodb.tx.insert_one.assert_called_once_with(dict(tx_write))


@pytest.mark.asyncio
async def test_insert_tx_failure(mongodb, tx_write):
    """Тест ошибки при вставке одной транзакции"""
    mongodb.tx.insert_one = AsyncMock(side_effect=Exception("MongoDB error"))
    with pytest.raises(Exception, match="MongoDB error"):
        await mongodb.insert_tx(tx_write)


@pytest.mark.asyncio
async def test_insert_many_tx_success(mongodb, tx_write):
    """Тест успешной вставки нескольких транзакций"""
    mock_result = InsertManyResult(inserted_ids=["123", "124"], acknowledged=True)
    mongodb.tx.insert_many = AsyncMock(return_value=mock_result)
    result = await mongodb.insert_many_tx([tx_write, tx_write])
    assert result is True
    mongodb.tx.insert_many.assert_called_once_with([dict(tx_write), dict(tx_write)])


@pytest.mark.asyncio
async def test_insert_many_tx_empty(mongodb):
    """Тест вставки пустого списка транзакций"""
    result = await mongodb.insert_many_tx([])
    assert result is True
    mongodb.tx.insert_many.assert_not_called()


@pytest.mark.asyncio
async def test_find_one_success(mongodb):
    """Тест успешного поиска одной транзакции"""
    query = {"uniq": "tx123"}
    mock_result = {"uniq": "tx123", "amount": 100}
    mongodb.tx.find_one = AsyncMock(return_value=mock_result)
    result = await mongodb.find_one(query)
    assert result == mock_result
    mongodb.tx.find_one.assert_called_once_with(query)


@pytest.mark.asyncio
async def test_aggregate_success(mongodb):
    """Тест успешной агрегации"""
    pipeline = [{"$match": {"uniq": "tx123"}}]
    mock_result = [{"uniq": "tx123", "amount": 100}]

    async def cursor():
        for item in mock_result:
            yield item

    mongodb.tx.aggregate = AsyncMock(return_value=cursor())
    result = await mongodb.aggregate(pipeline)
    assert result == mock_result
    mongodb.tx.aggregate.assert_called_once_with(pipeline)


@pytest.mark.asyncio
async def test_aggregate_empty(mongodb):
    """Тест пустого результата агрегации"""
    pipeline = [{"$match": {"uniq": "tx123"}}]

    async def cursor():
        if False:
            yield None

    mongodb.tx.aggregate = AsyncMock(return_value=cursor())
    result = await mongodb.aggregate(pipeline)
    assert result is None
    mongodb.tx.aggregate.assert_called_once_with(pipeline)


@pytest.mark.asyncio
async def test_find_success(mongodb):
    """Тест успешного поиска с пропуском и лимитом"""
    query = {"amount": {"$gt": 50}}
    skip, limit = 10, 20
    mock_result = [{"uniq": "tx123"}, {"uniq": "tx124"}]
    mock_cursor = MagicMock()
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor
    mock_cursor.to_list = AsyncMock(return_value=mock_result)
    mongodb.tx.find = MagicMock(return_value=mock_cursor)
    result = await mongodb.find(query, skip, limit)
    assert result == mock_result
    mongodb.tx.find.assert_called_once_with(query)


@pytest.mark.asyncio
async def test_close(mongodb):
    """Тест закрытия соединения"""
    mongodb.client = MagicMock()
    client = mongodb.client
    client.close = AsyncMock()
    await mongodb.close()
    client.close.assert_called_once()
    assert mongodb.client is None
    assert mongodb.db is None
    assert mongodb.tx is None
