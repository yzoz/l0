import asyncio
import json
import time
from aio_pika import IncomingMessage
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.models.tx import Tx
from src.utils.hash import tob2b

# Инициализация клиентов
db = MongoDB()
rabbitmq = RabbitMQ(host='localhost', queue_name='tx')  # Указываем имя очереди

async def on_message(message: IncomingMessage):
    """Обработка входящего сообщения."""
    async with message.process():  # Автоматически подтверждает сообщение
        tx_data = message.body.decode("utf-8")
        tx = json.loads(tx_data)
        print("Received transaction:", tx)
        tx = Tx(**tx)
        tx.time = time.time_ns()
        tx.v = 1
        m = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + (tx.msg or "")
        this_hash = tob2b(m)
        last = await db.find_one({'$query': {}, '$orderby': {'_id': -1}})
        last_hash = last['hash'] if last else "0"
        print(f"Last hash: {last_hash}, This hash: {this_hash}")
        tx.hash = tob2b(last_hash + this_hash)
        result = await db.insert_tx(tx)
        print("Insert result:", result)
        if result:
            print("Saved transaction:", dict(tx))
        else:
            print("Failed to save transaction")

async def main():
    """Основная функция для запуска consumer'а."""
    await rabbitmq.connect()  # Подключение к RabbitMQ
    print("Waiting for messages...")
    await rabbitmq.consume_messages(on_message)  # Запуск потребления сообщений

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted")