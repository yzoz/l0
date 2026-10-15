import logging

from aio_pika import DeliveryMode, IncomingMessage, Message, connect_robust

logger = logging.getLogger(__name__)

# Очередь транзакций и её dead-letter «парковка»: всё, что consumer
# отбраковал (nack без requeue), копируется в tx.dlq вместо потери —
# сообщения можно разобрать и реплеить.
TX_QUEUE = "tx"
DLQ_QUEUE = "tx.dlq"


class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        """Инициализация подключения к RabbitMQ с dead-letter очередью."""
        self.connection = await connect_robust("amqp://guest:guest@localhost/")
        self.channel = await self.connection.channel()
        # await self.rabbitmq.channel.set_qos(prefetch_count=1)
        await self.channel.declare_queue(DLQ_QUEUE, durable=True)

    async def publish_message(self, queue_name, message: str):
        """Публикация сообщения в очередь."""
        await self.channel.default_exchange.publish(
            Message(message.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT), routing_key=queue_name
        )

    async def publish_to_dlq(self, message: IncomingMessage, reason: str):
        """Сохраняет отбракованное сообщение в DLQ (вместе с причиной отказа)."""
        try:
            await self.channel.default_exchange.publish(
                Message(
                    message.body,
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers={"x-dlq-reason": reason},
                ),
                routing_key=DLQ_QUEUE,
            )
        except Exception as e:
            # DLQ — best effort: само сообщение всё равно отбраковывается.
            logger.error("Failed to publish message to DLQ (%s): %s", reason, e)

    async def consume_messages(self, queue_name: str, callback: callable, stop_event=None):
        """Потребление сообщений из очереди."""
        queue = await self.channel.declare_queue(queue_name, durable=True)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                if stop_event and stop_event.is_set():
                    logger.info("Stop event detected, stopping message consumption")
                    break
                await callback(message)

    async def close(self):
        """Закрытие подключения."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
