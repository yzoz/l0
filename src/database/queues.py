import logging
from aio_pika import connect_robust, Message, DeliveryMode

class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        """Инициализация подключения к RabbitMQ с dead-letter очередью."""
        self.connection = await connect_robust(f"amqp://guest:guest@localhost/")
        self.channel = await self.connection.channel()
        #await self.rabbitmq.channel.set_qos(prefetch_count=1)
        """try: # Создание dead-letter очереди
            await self.channel.declare_queue(
                "dead-letter",
                durable=True,
                arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": "tx"}
            )
        except Exception as e:
            logging.error(e)"""

    async def publish_message(self, queue_name, message: str):
        """Публикация сообщения в очередь."""
        await self.channel.default_exchange.publish(
            Message(message.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=queue_name
        )
        logging.info(f"Published message to {queue_name}: {message}")

    async def consume_messages(self, queue_name, callback):
        """Потребление сообщений из очереди."""
        queue = await self.channel.declare_queue(queue_name, durable=True)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await callback(message)  # Обработка ошибок делегируется callback'у

    async def close(self):
        """Закрытие подключения."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()