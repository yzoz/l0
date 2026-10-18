import asyncio
import json
import logging
from hashlib import sha512

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from nacl.exceptions import BadSignatureError

from src.config import IN_QUEUE, RABBITMQ_TIMEOUT
from src.models.tx import TxRead
from src.utils.crypto import verify_message
from src.utils.data import hash_hex, ordered_sum

logger = logging.getLogger(__name__)

router = APIRouter()


async def get_rabbit(request: Request):
    return request.app.state.rabbit


@router.post("/send/")
async def send(tx: TxRead, rabbit=Depends(get_rabbit)):
    logger.debug("Ordered sum: %s", ordered_sum(dict(tx.content)))
    try:
        logger.info(
            f"Processing transaction: uniq={tx.content.uniq}, credit={tx.content.credit}, debit={tx.content.debit}"
        )

        # Проверка подписи
        message_to_verify = sha512(ordered_sum(dict(tx.content)).encode("utf-8")).digest()
        try:
            verified_message = verify_message(tx.content.credit, tx.sign, message_to_verify)
        except (BadSignatureError, ValueError, TypeError):
            verified_message = False
        if verified_message != message_to_verify:
            logger.warning(f"Signature verification failed: credit={tx.content.credit}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "Bad sign!"}])

        # tx_hash — идентификатор транзакции. Считается ТЕМ ЖЕ способом и по ТЕМ ЖЕ
        # полям (parents/credit/debit/amount/uniq/msg/sign), что и на стороне consumer
        # (hash_hex(ordered_sum(dict(TxStore)))), чтобы клиент и пайплайн давали
        # одинаковый хэш. Это идентификатор для отслеживания статуса, а не гарантия
        # подтверждения.
        tx_hash = hash_hex(ordered_sum({**dict(tx.content), "sign": tx.sign}))

        message = json.dumps(jsonable_encoder(tx))
        logger.info(f"Publishing to RabbitMQ: {message}")

        # Отправка в очередь in с тайм-аутом
        try:
            await asyncio.wait_for(rabbit.publish_message(IN_QUEUE, message), timeout=RABBITMQ_TIMEOUT)
            logger.info(f"Transaction successfully published: uniq={tx.content.uniq} hash={tx_hash}")
        except asyncio.TimeoutError:
            logger.error(f"RabbitMQ timeout after {RABBITMQ_TIMEOUT}s: uniq={tx.content.uniq}")
            raise HTTPException(status_code=503, detail=[{"success": 0, "msg": "RabbitMQ timeout"}]) from None

        return {"detail": [{"success": 1, "msg": "ok"}], "tx_hash": tx_hash}

    except HTTPException:
        raise
    except Exception:
        logger.error("Unexpected error in send", exc_info=True)
        raise HTTPException(status_code=500, detail=[{"success": 0, "msg": "Internal server error"}]) from None
