import asyncio
import httpx
from src.models.tx import Tx
import uuid
import random
from src.utils.data import ordered_sum
from src.utils.crypto import sign_message
import json
import logging
from multiprocessing import Process
import sys
import time

# Настройка логирования
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

start = time.time()

async def arange(count):
    """Генератор для асинхронного перебора чисел."""
    for i in range(count):
        yield i
        await asyncio.sleep(0.1)
        if i % 5 == 0: await asyncio.sleep(round(random.uniform(0, 0.25), 3))
        if i % 10 == 0: await asyncio.sleep(round(random.uniform(0, 0.5), 3))
        #await asyncio.sleep(round(random.uniform(0.01, 0.05), 3))
        """await asyncio.sleep(random.randint(0, 30))
        if i % 2 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 3 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 5 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 7 == 0: await asyncio.sleep(random.randint(0, 30))
        if i % 10 == 0: await asyncio.sleep(random.randint(0, 30))"""

async def send(in_set):
    #await asyncio.sleep(random.randint(0, 30))
    """Отправка случайных транзакций в асинхронном режиме."""
    async for _ in arange(int(in_set)):
        i = random.randint(0, 4)
        c = pk[i]  # credit (отправитель)
        priv = sk[i]  # приватный ключ
        d = pk[random.randint(0, 4)]  # debit (получатель)
        a = random.randint(1, 1000000000)  # сумма
        u = str(uuid.uuid4())  # уникальный идентификатор
        m = str(random.randint(0, 100000))  # сообщение
        try:
            p = await client.get('http://192.168.1.8:8000/pool')
            r = p.json()
            # Формируем транзакцию
            tx = Tx(
                parents=r,
                debit=d,
                credit=c,
                amount=a,
                uniq=u,
                msg=m
            )
            m = ordered_sum(dict(tx))  # строка для подписи
            # Подпись транзакции
            s = sign_message(priv, m)
            tx_data = {
                "content": dict(tx),
                "sign": s
            }
            #logger.info(f"Sending transaction: {json.dumps(tx_data, indent=2)}")
            r = await client.post('http://192.168.1.8:8000/send/', data=json.dumps(tx_data))
            #logger.info(f"Response: {r.text}")
        except Exception as e:
            logger.error(f"Error in send: {e}")

async def run_sender(in_set):
    """Запуск 10 параллельных корутин в одном процессе."""
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[send(in_set) for _ in range(25)]) #250

def process_target(in_set):
    """Функция-цель для каждого процесса."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_sender(in_set))
    except KeyboardInterrupt:
        logger.info(f"Process interrupted")
    finally:
        loop.close()

def main():
    """Основная функция для запуска многопроцессного генератора транзакций."""
    in_set = input("Enter the number of transactions to send per coroutine: ")
    num_processes = 25  # Количество процессов (можно настроить)

    processes = []
    for _ in range(num_processes):
        p = Process(target=process_target, args=(in_set,))
        processes.append(p)
        p.start()

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Main process interrupted, terminating all processes...")
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
        sys.exit(0)

if __name__ == "__main__":
    main()
    print(int(time.time() - start))