import asyncio
import json
import time
import random
import logging
import httpx
from nacl.signing import SigningKey
import base58
import uuid
from src.utils.data import ordered_sum
from hashlib import sha512

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Асинхронный HTTP-клиент
client = httpx.AsyncClient()

# Список ключей (приватные и публичные)
sk = [
    'DF3998wbqyBn9uSA4BcMJQAkpnZb21a1vRLo9Nw8GwWy',
    '8no9RFLqKkC5HSLmSqkTnrX6pD2Fn5MNNbJmdGP1GivQ',
    '6savWyJQgcZEfDBt9N1u2dhPg68cng43zQFKgyM9qKDW',
    'C8xjbHktooH8Pm1n57hHprfBSb4kxSfTcDS3ZZKBCTCR',
    'GtTXBxARCLoRUFcmv5QtU6L1neQkRdivRXCEMuigKba5'
]
pk = [
    'EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1',
    'A9huRv7vtELAYQ4nQYtEWfn5NEdqXBUs722TdsiZR3ST',
    '2RAHCZg7SfyLY6x5uLmtDh1tNbZr4w5NTPMWXpHrqdyc',
    '9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy',
    'RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR'
]

async def arange(count):
    """Генератор для асинхронного перебора чисел."""
    for i in range(count):
        yield i
        await asyncio.sleep(0.0)

async def send(in_set):
    """Отправка случайных транзакций."""
    async for _ in arange(int(in_set)):
        i = random.randint(0, 4)
        c = pk[i]  # credit (отправитель)
        priv = sk[i]  # приватный ключ
        d = pk[random.randint(0, 4)]  # debit (получатель)
        a = random.randint(10, 1000)  # сумма
        u = str(uuid.uuid4())  # уникальный идентификатор
        m = str(random.randint(0, 100000))  # сообщение
        # Формируем транзакцию
        c = {
            'credit': c,
            'debit': d,
            'amount': a,
            'asset': "0",
            'uniq': u,
            'msg': m          
        }
        h = ordered_sum(c)  # строка для подписи
        # Подпись транзакции
        priv = base58.b58decode(priv)
        signing_key = SigningKey(priv)
        message_bytes = sha512(h.encode("utf-8")).digest()
        signed = base58.b58encode(signing_key.sign(message_bytes))
        s = signed.decode("utf-8")
        tx = {
            "content": c,
            "sign": s
        }
        logger.info(f"Sending transaction: {json.dumps(tx, indent=2)}")
        logger.info("_" * 80)
        r = await client.post('http://127.0.0.1:8000/send/', data=json.dumps(tx))
        logger.info("_" * 80)
        logger.info(f"Response: {r.text}")

async def main():
    """Основная функция для запуска генератора транзакций."""
    in_set = input("Enter the number of transactions to send: ")
    await asyncio.gather(*[send(in_set) for _ in range(50)])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        asyncio.run(client.aclose())  # Закрываем HTTP-клиент