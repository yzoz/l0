"""Сквозной (E2E) инвариант конвейера L0.

Проверяет согласованность хранилищ ПОСЛЕ того, как конвейер осел
(genesis подтверждён, транзакции обработаны consumer→confirmer→writer):

    consumer → Memgraph → confirmer (weight) → writer → MongoDB + Redis

Инварианты (проверяются на живых данных MongoDB/Redis):

1. Согласованность ПО АДРЕСУ: для каждого ``balance:{addr}`` в Redis
   ``balance_from_ledger(addr)`` (Mongo) даёт то же значение. Деньги в кэше
   и в журнале не расходятся.

2. Сохранение суммы / ОТСУТСТВИЕ ДВОЙНОГО КРЕДИТА (главная проверка фикса
   минта): сумма всех балансов Redis == сумма всех балансов Mongo == сумма
   genesis-минтов. Минт создаёт деньги ТОЛЬКО абсолютным pre-load в genesis;
   подтверждение минта НЕ делает ``INCRBY`` (иначе сумма удвоилась бы).
   Переводы zero-sum, поэтому общий объём == сумме минтов, не более.

3. Минт записан РОВНО ОДИН РАЗ: в Mongo три ``credit == ""`` записи, их
   (msg, amount, debit) совпадают с ожидаемым genesis-набором, без дублей.

Запуск (только интеграционные, требует поднятых MongoDB/Redis и осевшего
конвейера; см. ``make e2e``):

    poetry run pytest -m integration tests/test_e2e_pipeline.py -v
"""

import pytest

from src.config import DEV_ADDR
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.utils.profile import balance_from_ledger

# Ожидаемый genesis-набор (соответствует genesis.py). Закрепляет, что минты
# не задваиваются и не теряются. TOTAL — сумма всех созданных денег.
EXPECTED_MINTS = {
    ("genesis", 500000000000000),
    ("genesis2", 330000000000000),
    ("genesis3", 250000000000000),
}
EXPECTED_TOTAL_MINT = sum(amount for _, amount in EXPECTED_MINTS)  # 1_080_000_000_000_000


async def _load_balances(mongodb: MongoDB, redis: Redis):
    """Вернёт (по_адресу: {addr: (redis_bal, mongo_bal)}, сумма_redis, сумма_mongo)."""
    client = redis.client
    per_addr = {}
    total_redis = 0
    total_mongo = 0
    async for key in client.scan_iter("balance:*"):
        addr = key.split(":", 1)[1]
        redis_bal = int(await client.get(key) or 0)
        mongo_bal = int(await balance_from_ledger(addr, mongodb) or 0)
        per_addr[addr] = (redis_bal, mongo_bal)
        total_redis += redis_bal
        total_mongo += mongo_bal
    return per_addr, total_redis, total_mongo


@pytest.mark.integration
@pytest.mark.asyncio
async def test_e2e_pipeline_invariants():
    mongodb = MongoDB()
    redis = Redis()

    try:
        await mongodb.connect()
        await redis.connect()

        # --- 1. Согласованность по адресу (все адреса, не только genesis) ---
        per_addr, total_redis, total_mongo = await _load_balances(mongodb, redis)
        assert per_addr, "В Redis нет ни одного balance:* — конвейер не создал состояния"

        mismatches = {
            addr: (r, m) for addr, (r, m) in per_addr.items() if r != m
        }
        assert not mismatches, (
            "Расхождение Redis vs MongoDB по адресам (balance:{addr} != balance_from_ledger): "
            f"{mismatches}"
        )

        # --- 2. Сохранение суммы / отсутствие двойного кредита ---
        assert total_redis == total_mongo, (
            f"Суммарные балансы разошлись: Redis={total_redis}, MongoDB={total_mongo}"
        )
        assert total_redis == EXPECTED_TOTAL_MINT, (
            "Сумма балансов != сумме genesis-минтов — признак двойного кредита "
            f"(или потери/создания денег): Redis={total_redis}, ожидалось={EXPECTED_TOTAL_MINT}. "
            f"Удвоение minта дало бы ~{2 * EXPECTED_TOTAL_MINT}."
        )

        # --- 3. Минт записан ровно один раз ---
        txs = mongodb.db["tx"]
        mint_docs = [
            {"msg": d.get("msg"), "amount": int(d.get("amount")), "debit": d.get("debit")}
            async for d in txs.find({"credit": ""}, {"msg": 1, "amount": 1, "debit": 1, "_id": 0})
        ]
        mint_keys = [(d["msg"], d["amount"]) for d in mint_docs]
        assert len(mint_docs) == len(EXPECTED_MINTS), (
            f"Ожидалось {len(EXPECTED_MINTS)} минт-записей, найдено {len(mint_docs)}: {mint_keys}"
        )
        assert set(mint_keys) == EXPECTED_MINTS, (
            f"Genesis-минты не совпадают с ожидаемым набором: {set(mint_keys)}"
        )
        # Каждый минт относится к своему genesis-адресу, без дублей.
        assert sorted(d["debit"] for d in mint_docs) == sorted(DEV_ADDR), (
            f"Получатели минтов != DEV_ADDR: {sorted(d['debit'] for d in mint_docs)}"
        )

    finally:
        await mongodb.close()
        await redis.close()


if __name__ == "__main__":
    pytest.main(["-v", __file__])
