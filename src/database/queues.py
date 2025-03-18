# src/database/queues.py
from aio_pika import connect_robust, Message, DeliveryMode
import pika

async def publish_message(message: str, queue_name: str = "tx"):
    try:
        connection = await connect_robust("amqp://guest:guest@localhost/")
        channel = await connection.channel()
        await channel.declare_queue(queue_name, durable=True)
        print(f"Publishing message to {queue_name}: {message}")  # Отладка
        await channel.default_exchange.publish(
            Message(message.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=queue_name
        )
        await connection.close()
    except Exception as e:
        print(f"Error publishing to RabbitMQ: {str(e)}")  # Отладка

def consume_messages(callback, queue_name: str = "tx"):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)  # Добавлено создание очереди
    channel.basic_consume(queue_name, callback)
    channel.start_consuming()