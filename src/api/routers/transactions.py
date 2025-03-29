from fastapi import APIRouter, Depends, Request, HTTPException
from src.models.tx import TxRead
from src.utils.balance import get_balance
from src.database.mongodb import MongoDB
from src.utils.crypto import verify_message
from src.utils.data import ordered_sum
from fastapi.encoders import jsonable_encoder
import json
import base58
import logging
import asyncio

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

@router.post("/send/")
async def send(
    tx: TxRead,
    db: MongoDB = Depends(get_db),
    rabbit = Depends(get_rabbit)
):
    try:
        logger.info(f"Processing transaction: uniq={tx.content.uniq}, credit={tx.content.credit}, debit={tx.content.debit}")

        # Валидация данных
        await validate_tx(tx, db)

        # Проверка уникальности в БД
        if await db.find_one({"uniq": tx.content.uniq}):
            logger.warning(f"Duplicate transaction detected: uniq={tx.content.uniq}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"not uniq ({tx.content.uniq})"}])

        # Проверка отправки самому себе
        if tx.content.credit == tx.content.debit:
            logger.warning(f"Self-transfer detected: credit={tx.content.credit}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "not for yourself"}])

        # Проверка подписи
        message_to_verify = ordered_sum(dict(tx.content))
        #message_to_verify = tx.content.credit + tx.content.debit + str(tx.content.amount) + tx.content.asset + tx.content.uniq + (tx.content.msg or "")
        logger.info(f"Message to verify: {message_to_verify}")
        verified_message = verify_message(tx.content.credit, tx.sign)
        if verified_message != message_to_verify:
            logger.warning(f"Signature verification failed: credit={tx.content.credit}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "bad sign"}])

        # Проверка баланса
        balance = await get_balance(tx.content.credit, db)
        logger.info(f"Balance for {tx.content.credit}: {balance}")
        if balance < tx.content.amount:
            logger.warning(f"Insufficient balance: credit={tx.content.credit}, balance={balance}, amount={tx.content.amount}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "bad balance"}])

        # Сериализация с хэшем
        tx_dict = jsonable_encoder(tx)
        message = json.dumps(tx_dict)
        logger.info(f"Publishing to RabbitMQ: {message}")

        # Отправка в RabbitMQ с тайм-аутом
        try:
            await asyncio.wait_for(rabbit.publish_message(message), timeout=RABBITMQ_TIMEOUT)
            logger.info(f"Transaction successfully published: uniq={tx.content.uniq}")
        except asyncio.TimeoutError:
            logger.error(f"RabbitMQ timeout after {RABBITMQ_TIMEOUT}s: uniq={tx.content.uniq}")
            raise HTTPException(status_code=503, detail=[{"success": 0, "msg": "RabbitMQ timeout"}])

        return {"detail": [{"success": 1, "msg": "ok"}]}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in send: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=[{"success": 0, "msg": f"server error: {str(e)}"}])

async def validate_tx(tx: TxRead, db: MongoDB) -> None:
    """Асинхронная проверка валидности и правильности данных транзакции."""
    # Проверка адресов
    for addr, name in [(tx.content.credit, "credit"), (tx.content.debit, "debit")]:
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
    if not isinstance(tx.content.amount, (int, float)) or tx.content.amount <= 0 or tx.content.amount > MAX_AMOUNT:
        logger.warning(f"Invalid amount: {tx.content.amount}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "amount must be positive and within limits"}])

    # Проверка уникального идентификатора
    if not tx.content.uniq or len(tx.content.uniq) < 1:
        logger.warning(f"Invalid uniq: {tx.content.uniq}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "uniq must be non-empty"}])

    # Проверка сообщения
    if tx.content.msg and len(tx.content.msg) > MAX_MSG_LENGTH:
        logger.warning(f"Message too long: length={len(tx.content.msg)}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"message exceeds {MAX_MSG_LENGTH} chars"}])

    # Проверка подписи (формальная)
    if not tx.sign or len(tx.sign) < 1:
        logger.warning(f"Invalid signature: {tx.content.sign}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "signature must be non-empty"}])
