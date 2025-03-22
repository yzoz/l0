import asyncio
import json
import time
import logging
from aio_pika import IncomingMessage
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.models.tx import Tx
from src.utils.hash import tob2b

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
        tx = Tx(**tx_dict)

        if await db.find_one({"uniq": tx.uniq}):
            logger.info(f"Duplicate transaction detected: {tx.uniq}")
            await message.ack()
            return

        tx.time = int(time.time() * 1e9)
        tx.v = 1

        m = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + (tx.msg or "")
        this_hash = tob2b(m)

        last = await db.find_one({'$query': {}, '$orderby': {'_id': -1}})
        last_hash = last['hash'] if last else "0"
        logger.info(f"Last hash: {last_hash}, This hash: {this_hash}")

        tx.hash = tob2b(last_hash + this_hash)

        result = await db.insert_tx(tx)
        if result:
            logger.info(f"Saved transaction: {tx.dict()}")
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
        # Проверяем состояние перед закрытием
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