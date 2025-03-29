from fastapi import APIRouter, Depends, Request, HTTPException
from src.models.tx import TxRead
from src.utils.crypto import verify_message
from src.utils.data import ordered_sum
from fastapi.encoders import jsonable_encoder
import json
import base58
import logging
import asyncio
from hashlib import sha512

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

router = APIRouter()

# Константы
RABBITMQ_TIMEOUT = 5  # Тайм-аут для RabbitMQ в секундах
MAX_MSG_LENGTH = 256  # Максимальная длина сообщения

async def get_rabbit(request: Request):
    return request.app.state.rabbit

@router.post("/send/")
async def send(
    tx: TxRead,
    rabbit = Depends(get_rabbit)
):
    try:
        logger.info(f"Processing transaction: uniq={tx.content.uniq}, credit={tx.content.credit}, debit={tx.content.debit}")

        # Валидация данных
        validate_tx(tx)

        # Проверка подписи
        message_to_verify = sha512(ordered_sum(dict(tx.content)).encode('utf-8')).digest()
        verified_message = verify_message(tx.content.credit, tx.sign)
        logger.info(f"Message to verify: {message_to_verify} | Verified message: {verified_message}")
        if verified_message != message_to_verify:
            logger.warning(f"Signature verification failed: credit={tx.content.credit}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "Bad sign!"}])

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

def validate_tx(tx: TxRead) -> None:
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

    # Проверка отправки самому себе
    if tx.content.credit == tx.content.debit:
        logger.warning(f"Self-transfer detected: credit={tx.content.credit}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "not for yourself"}])

    # Проверка сообщения
    if tx.content.msg and len(tx.content.msg) > MAX_MSG_LENGTH:
        logger.warning(f"Message too long: length={len(tx.content.msg)}")
        raise HTTPException(status_code=400, detail=[{"success": 0, "msg": f"message exceeds {MAX_MSG_LENGTH} chars"}])
