import asyncio
import json
import time
import logging
from aio_pika import IncomingMessage
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite, TxStore
from src.utils.data import hash_hex, ordered_sum
from src.utils.balance import get_balance
#import redis.asyncio as redis
import redis

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HOME_TIME = 300 * 1_000_000

# Инициализация клиентов
db = MongoDB()
rabbitmq = RabbitMQ(host='localhost', queue_name='tx')
pool = redis.Redis(host='localhost', db=0)
home = redis.Redis(host='localhost', db=1)

async def on_message(message: IncomingMessage):
    logger.info("_" * 80)
    """Обработка входящего сообщения."""
    now = int(time.time_ns() / 1000)
    try:
        tx_data = message.body.decode("utf-8")
        tx_dict = json.loads(tx_data)
        c = tx_dict["content"]
        c["sign"] = tx_dict["sign"]
        m = ordered_sum(c)
        hash = hash_hex(m)
        tx = TxStore(**c)

        if await db.find_one({"credit": tx.credit, "uniq": tx.uniq}):
            logger.info(f"Duplicate transaction detected: {tx.uniq}")
            await message.nack(requeue=False)
            return
        
        balance = await get_balance(tx.credit, db)
        if balance < tx.amount and tx.credit != "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy": # DEV
            logger.error(f"Insufficient balance for {tx.credit}: {balance} < {tx.amount}")
            await message.nack(requeue=False) # False если баланс недостаточен, отклоняем сообщение. True если баланс не отклоняем сообщение и повторяем попытку
            return
        
        pool_mom = pool.get(tx.mom)
        pool_dad = pool.get(tx.dad)
        pool_bro = pool.get(tx.bro)
        pool_home_mom = home.get(tx.mom)
        pool_home_dad = home.get(tx.dad)
        pool_home_bro = home.get(tx.bro)
        MOM = False
        DAD = False
        BRO = False
        if pool_mom or pool_home_mom: MOM = True
        if pool_dad or pool_home_dad: DAD = True
        if pool_bro or pool_home_bro: BRO = True
        """Если мама и папа найдены добавляем нашу транзу в пул"""
        if MOM and DAD and BRO:
            p = dict(tx)
            p["time"] = int(time.time_ns() / 1000)
            #tx.time = int(time.time() * 1e9)
            pool.set(hash, json.dumps(p))
            logger.info(f"Pooled transaction: {p}")
            """Если мама всё ещё в пуле, перемещаем её в дом и публикуем"""
            if pool_mom:
                pool.delete(tx.mom)
                home.set(tx.mom, pool_mom)
                logger.info(f"Move pool_mom {tx.mom}")
                t = json.loads(pool_mom.decode("utf-8"))
                t["hash"] = tx.mom
                t = TxWrite(**t)
                result = await db.insert_tx(t)
                if result:
                    logger.info(f"Saved transaction: {dict(t)}")
                else:
                    logger.error("Failed to save transaction")
            """Если папа всё ещё в пуле, перемещаем его в дом и публикуем"""
            if pool_dad:
                pool.delete(tx.dad)
                home.set(tx.dad, pool_dad)
                logger.info(f"Move pool_dad {tx.dad}")
                t = json.loads(pool_dad.decode("utf-8"))
                t["hash"] = tx.dad
                t = TxWrite(**t)
                result = await db.insert_tx(t)
                if result:
                    logger.info(f"Saved transaction: {dict(t)}")
                else:
                    logger.error("Failed to save transaction")
            if pool_bro:
                pool.delete(tx.bro)
                home.set(tx.bro, pool_bro)
                logger.info(f"Move pool_bro {tx.bro}")
                t = json.loads(pool_bro.decode("utf-8"))
                t["hash"] = tx.bro
                t = TxWrite(**t)
                result = await db.insert_tx(t)
                if result:
                    logger.info(f"Saved transaction: {dict(t)}")
                else:
                    logger.error("Failed to save transaction")
            """Если родители дома дольше чем N -> удаляем"""
            """if pool_home_mom:
                t = json.loads(pool_home_mom.decode("utf-8"))["time"]
                if now - t > HOME_TIME: home.delete(tx.mom); logger.info(f"Remove pool_home_mom: {pool_home_mom}")
            if pool_home_dad:
                t = json.loads(pool_home_dad.decode("utf-8"))["time"]
                if now - t > HOME_TIME: home.delete(tx.dad); logger.info(f"Remove pool_home_dad: {pool_home_dad}")"""
            await message.ack()
        else:
            logger.error(f"No mom & dad in Pool: {dict(t)}")
            await message.nack(requeue=False)

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await message.nack(requeue=False) #requeue=True

async def main():
    """Основная функция для запуска consumer'а."""
    try:
        await rabbitmq.connect()
        logger.info("Connected to RabbitMQ. Waiting for messages...")
        await rabbitmq.consume_messages(on_message)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        if rabbitmq.connection and not rabbitmq.connection.is_closed:
            await rabbitmq.close()
            logger.info("RabbitMQ connection closed")
        else:
            logger.warning("RabbitMQ connection was not open or already closed")
        await db.close()
        logger.info("MongoDB connection closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")