import asyncio
import json
import time
import logging
from aio_pika import IncomingMessage
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.models.tx import TxWrite
from src.utils.data import hash_hex, ordered_sum
from src.utils.balance import get_balance  # Импортируем функцию

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация клиентов
db = MongoDB()
rabbitmq = RabbitMQ(host='localhost', queue_name='tx')

async def on_message(message: IncomingMessage):
    """Обработка входящего сообщения."""
    try:
        tx_data = message.body.decode("utf-8")
        tx_dict = json.loads(tx_data)
        tx = tx_dict["content"]
        tx['sign'] = tx_dict["sign"]
        m = ordered_sum(tx)
        this_hash = hash_hex(m)
        tx = TxWrite(**tx)
        #tx.time = int(time.time() * 1e9)
        tx.time = time.time_ns()
        tx.v = 1

        if await db.find_one({"uniq": tx.uniq}):
            logger.info(f"Duplicate transaction detected: {tx.uniq}")
            await message.ack()
            return

        # Проверка баланса отправителя
        balance = await get_balance(tx.credit, db)
        if balance < tx.amount:
            logger.error(f"Insufficient balance for {tx.credit}: {balance} < {tx.amount}")
            await message.nack(requeue=False) # False если баланс недостаточен, отклоняем сообщение. True если баланс не отклоняем сообщение и повторяем попытку
            return

        last = await db.find_one({'$query': {}, '$orderby': {'_id': -1}})
        last_hash = last['hash'] if last else "0"
        logger.info(f"Last hash: {last_hash}, This hash: {this_hash}")

        tx.hash = hash_hex(last_hash + this_hash)

        result = await db.insert_tx(tx)
        if result:
            logger.info(f"Saved transaction: {dict(tx)}")
            await message.ack()
        else:
            logger.error("Failed to save transaction")
            await message.nack(requeue=True)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await message.nack(requeue=True)

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