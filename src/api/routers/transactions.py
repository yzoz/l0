from fastapi import APIRouter, Depends, Request, HTTPException
from src.models.tx import TxRead
from src.utils.crypto import verify_message
from src.utils.data import ordered_sum
import json
import logging
import asyncio
from hashlib import sha512
from fastapi.encoders import jsonable_encoder

logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

router = APIRouter()

RABBITMQ_TIMEOUT = 5

async def get_rabbit(request: Request):
    return request.app.state.rabbit

@router.post("/send/")
async def send(
    tx: TxRead,
    rabbit = Depends(get_rabbit)
):
    try:
        logger.info(f"Processing transaction: uniq={tx.content.uniq}, credit={tx.content.credit}, debit={tx.content.debit}")

        # Проверка подписи
        message_to_verify = sha512(ordered_sum(dict(tx.content)).encode('utf-8')).digest()
        verified_message = verify_message(tx.content.credit, tx.sign)
        logger.info(f"Message to verify: {message_to_verify} | Verified message: {verified_message}")
        if verified_message != message_to_verify:
            logger.warning(f"Signature verification failed: credit={tx.content.credit}")
            raise HTTPException(status_code=400, detail=[{"success": 0, "msg": "Bad sign!"}])
        
        message = json.dumps(jsonable_encoder(tx))
        logger.info(f"Publishing to RabbitMQ: {message}")

        # Отправка в RabbitMQ с тайм-аутом
        try:
            await asyncio.wait_for(rabbit.publish_message("tx", message), timeout=RABBITMQ_TIMEOUT)
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
