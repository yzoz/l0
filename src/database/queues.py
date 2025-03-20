import asyncio
from aio_pika import connect_robust, Message, DeliveryMode, IncomingMessage

class RabbitMQ:
    def __init__(self, host='localhost', queue_name='tx'):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    async def connect(self):
        """Инициализация подключения к RabbitMQ."""
        self.connection = await connect_robust(f"amqp://guest:guest@{self.host}/")
        self.channel = await self.connection.channel()
        await self.channel.declare_queue(self.queue_name, durable=True)

    async def publish_message(self, message: str):
        """Публикация сообщения в очередь."""
        await self.channel.default_exchange.publish(
            Message(message.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=self.queue_name
        )

    async def consume_messages(self, callback):
        """Потребление сообщений из очереди с использованием переданного callback."""
        queue = await self.channel.declare_queue(self.queue_name, durable=True)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await callback(message)

    async def close(self):
        """Закрытие подключения."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()