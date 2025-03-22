from fastapi import APIRouter, Depends, Request, HTTPException
from src.models.tx import Tx
from src.database.mongodb import MongoDB
from src.utils.crypto import verify_message
from fastapi.encoders import jsonable_encoder
import json
import base58
import logging
import asyncio
from hashlib import sha256
from typing import Optional

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

router = APIRouter()

# Константы
RABBITMQ_TIMEOUT = 5  # Тайм-аут для RabbitMQ в секундах
MAX_AMOUNT = 10**18  # Максимальная сумма транзакции (например, 1e18)
MAX_MSG_LENGTH = 256  # Максимальная длина сообщения

async def get_db(request: Request):
    return request.app.state.db

async def get_rabbit(request: Request):
    return request.app.state.rabbit

def calculate_tx_hash(tx: Tx) -> str:
    """Вычисляет хэш транзакции для обеспечения неизменяемости."""
    data = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + (tx.msg or "")
    return sha256(data.encode('utf-8')).hexdigest()


@router.post("/send/")
async def send(
    tx: Tx,
    db: MongoDB = Depends(get_db),
    rabbit = Depends(get_rabbit)
):
    try:
        logger.info(f"Processing transaction: uniq={tx.uniq}, credit={tx.credit}, debit={tx.debit}")

        # Валидация данных
        await validate_tx(tx, db)

        # Проверка уникальности в БД
        if await db.find_one({"uniq": tx.uniq}):
            logger.warning(f"Duplicate transaction detected: uniq={tx.uniq}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"not uniq ({tx.uniq})"}])

        # Проверка отправки самому себе
        if tx.credit == tx.debit:
            logger.warning(f"Self-transfer detected: credit={tx.credit}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "not for yourself"}])

        # Проверка подписи
        message_to_verify = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + (tx.msg or "")
        logger.debug(f"Message to verify: {message_to_verify}")
        verified_message = verify_message(tx.credit, tx.sign)
        if verified_message != message_to_verify:
            logger.warning(f"Signature verification failed: credit={tx.credit}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "bad sign"}])

        # Проверка баланса
        balance = await get_balance(tx.credit, db)
        logger.info(f"Balance for {tx.credit}: {balance}")
        if balance < tx.amount:
            logger.warning(f"Insufficient balance: credit={tx.credit}, balance={balance}, amount={tx.amount}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "bad balance"}])

        # Вычисление хэша транзакции
        tx_hash = calculate_tx_hash(tx)
        logger.info(f"Transaction hash: {tx_hash}")

        # Сериализация с хэшем
        tx_dict = jsonable_encoder(tx)
        tx_dict["hash"] = tx_hash
        message = json.dumps(tx_dict)
        logger.debug(f"Publishing to RabbitMQ: {message}")

        # Отправка в RabbitMQ с тайм-аутом
        try:
            await asyncio.wait_for(rabbit.publish_message(message), timeout=RABBITMQ_TIMEOUT)
            logger.info(f"Transaction successfully published: uniq={tx.uniq}, hash={tx_hash}")
        except asyncio.TimeoutError:
            logger.error(f"RabbitMQ timeout after {RABBITMQ_TIMEOUT}s: uniq={tx.uniq}")
            raise HTTPException(status_code=503, detail=[{"success": 0, "msg": "RabbitMQ timeout"}])

        return {"detail": [{"success": 1, "msg": "ok"}]}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in send: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=[{"success": 0, "msg": f"server error: {str(e)}"}])

""" Эта функция оптимизированная для получения баланса """
async def get_balance(addr: str, db: MongoDB) -> int:
    """Оптимизированная функция получения баланса."""
    pipeline = [
        {"$facet": {
            "income": [
                {"$match": {"debit": addr}},
                {"$group": {"_id": None, "sum": {"$sum": "$amount"}}}
            ],
            "outcome": [
                {"$match": {"credit": addr}},
                {"$group": {"_id": None, "sum": {"$sum": "$amount"}}}
            ]
        }}
    ]
    result = await db.aggregate(pipeline)
    income = result[0]["income"][0]["sum"] if result[0]["income"] else 0
    outcome = result[0]["outcome"][0]["sum"] if result[0]["outcome"] else 0
    return income - outcome

async def validate_tx(tx: Tx, db: MongoDB) -> None:
    """Асинхронная проверка валидности и правильности данных транзакции."""
    # Проверка адресов
    for addr, name in [(tx.credit, "credit"), (tx.debit, "debit")]:
        try:
            addr_bytes = base58.b58decode(addr)
            if len(addr_bytes) != 32:
                logger.warning(f"Invalid {name} address size: {addr}, length={len(addr_bytes)}")
                raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"invalid {name} address size"}])
        except Exception as e:
            logger.error(f"Failed to decode {name} address: {addr}, error={str(e)}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"invalid {name} address"}])

        # Пример асинхронной проверки адреса в БД (опционально, закомментировано)
        # if not await db.find_one({"address": addr}):
        #     logger.warning(f"Address not found in DB: {addr}")
        #     raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"{name} address not registered"}])

    # Проверка суммы
    if not isinstance(tx.amount, (int, float)) or tx.amount <= 0 or tx.amount > MAX_AMOUNT:
        logger.warning(f"Invalid amount: {tx.amount}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "amount must be positive and within limits"}])

    # Проверка уникального идентификатора
    if not tx.uniq or len(tx.uniq) < 1:
        logger.warning(f"Invalid uniq: {tx.uniq}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "uniq must be non-empty"}])

    # Проверка сообщения
    if tx.msg and len(tx.msg) > MAX_MSG_LENGTH:
        logger.warning(f"Message too long: length={len(tx.msg)}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"message exceeds {MAX_MSG_LENGTH} chars"}])

    # Проверка подписи (формальная)
    if not tx.sign or len(tx.sign) < 1:
        logger.warning(f"Invalid signature: {tx.sign}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "signature must be non-empty"}])
