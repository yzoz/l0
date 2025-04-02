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
import redis.asyncio as redis

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация клиентов
db = MongoDB()
rabbitmq = RabbitMQ(host='localhost', queue_name='tx')
pool = redis.Redis(host='localhost', db=0)
home = redis.Redis(host='localhost', db=1)
balance = redis.Redis(host='localhost', db=2)

async def on_message(message: IncomingMessage):
    logger.info("_" * 80)
    """Обработка входящего сообщения."""
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
        
        b = await balance.get(tx.credit)
        if not b:
            b = await get_balance(tx.credit, db)
            if b: await balance.set(tx.credit, b)
        else:
            b = int(b.decode("utf-8"))
        if b < tx.amount and tx.credit != "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy": # DEV
            logger.error(f"Insufficient balance for {tx.credit}: {b} < {tx.amount}")
            await message.nack(requeue=False) # False если баланс недостаточен, отклоняем сообщение. True если баланс не отклоняем сообщение и повторяем попытку
            return
        
        pool_mom = await pool.get(tx.mom)
        pool_dad = await pool.get(tx.dad)
        pool_home_mom = await home.get(tx.mom)
        pool_home_dad = await home.get(tx.dad)
        MOM = False
        DAD = False
        if pool_mom or pool_home_mom: MOM = True
        if pool_dad or pool_home_dad: DAD = True
        """Если мама и папа найдены добавляем нашу транзу в пул"""
        if MOM and DAD:
            p = dict(tx)
            p["time"] = int(time.time_ns() / 1000)
            await pool.set(hash, json.dumps(p))
            await balance.incr(tx.debit, amount=tx.amount)
            await balance.decr(tx.credit, amount=tx.amount)
            logger.info(f"Pooled transaction: {p}")
            """Если мама всё ещё в пуле, перемещаем её в дом и публикуем"""
            if pool_mom:
                await pool.delete(tx.mom)
                await home.set(tx.mom, pool_mom)
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
                await pool.delete(tx.dad)
                await home.set(tx.dad, pool_dad)
                logger.info(f"Move pool_dad {tx.dad}")
                t = json.loads(pool_dad.decode("utf-8"))
                t["hash"] = tx.dad
                t = TxWrite(**t)
                result = await db.insert_tx(t)
                if result:
                    logger.info(f"Saved transaction: {dict(t)}")
                else:
                    logger.error("Failed to save transaction")
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