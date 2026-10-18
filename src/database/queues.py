"""RabbitMQ-очереди пайплайна и вспомогательные операции над кандидатами.

Пайплайн жизненного цикла транзакции (см. .docs/TODO_003.md):

    API --in--> consumer --Memgraph--> consumer --mid--> confirmer --out--> writer

* in  — подписанный транзакционный JSON (TxStore). Consumer пишет узел и рёбра
        в Memgraph; только после успешной записи публикует tx_hash в mid.
* mid — plain-text tx_hash (128 hex). Confirmer по нему находит транзакцию в
        графе, пересчитывает score её трёх родителей и публикует в out тех,
        чей score >= 1.
* out — plain-text tx_hash кандидата. Writer обрабатывает кандидата: проверка
        родителей, баланса, финализация в Mongo/Redis, удаление из графа.
* in.dlq — «парковка» отбракованных сообщений; публикация best effort.

Сообщения persistent, очереди durable. Внимание: default_exchange молча
отбрасывает сообщения в необъявленную очередь, поэтому все очереди объявляются
в RabbitMQ.connect().
"""

import asyncio
import logging
import uuid

from aio_pika import DeliveryMode, IncomingMessage, Message, connect_robust

from src.config import (
    DLQ_QUEUE,
    IN_QUEUE,
    MID_QUEUE,
    OUT_QUEUE,
    QUEUED_LEASE,
    QUEUED_POLL,
    QUEUED_WAIT,
)

logger = logging.getLogger(__name__)

__all__ = [
    "RabbitMQ",
    "PublishError",
    "IN_QUEUE",
    "MID_QUEUE",
    "OUT_QUEUE",
    "DLQ_QUEUE",
    "enqueue_candidate",
    "queued_token",
    "release_queued",
    "clear_queued",
]


class PublishError(RuntimeError):
    """Временная ошибка публикации в очереди.

    Сигнал для вызывающего кода: событие НЕ потеряно, нужно nack(requeue=True)
    и повторить обработку позже. В отличие от бизнес-ошибок (bad-json, конфликт
    уникальности) такие ошибки никогда не отправляются в DLQ.
    """


class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        """Инициализация подключения к RabbitMQ.

        Объявляем все очереди пайплайна durable, чтобы publish в любую из них
        был безопасен (см. докстринг модуля про default_exchange).
        """
        self.connection = await connect_robust("amqp://guest:guest@localhost/")
        self.channel = await self.connection.channel()
        # await self.rabbitmq.channel.set_qos(prefetch_count=1)
        for queue_name in (IN_QUEUE, MID_QUEUE, OUT_QUEUE, DLQ_QUEUE):
            await self.channel.declare_queue(queue_name, durable=True)

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


# ---------------------------------------------------------------------------
# Маркер queued:{hash} — дедупликация кандидатов в очереди out.
#
# Кандидат может быть поставлен в out несколькими событиями mid (каждое новое
# потомство пересчитывает score родителя). Чтобы не плодить дубли, перед
# публикацией захватывается Redis-маркер ``queued:{hash}``:
#
#   * значение — ownership token (uuid) захватившей попытки;
#   * TTL — аренда QUEUED_LEASE секунд: упавший процесс не блокирует кандидата
#     навсегда, по истечении аренды маркер освобождается сам;
#   * освобождать можно только «свой» маркер (compare-and-delete по token),
#     чтобы не удалить более новую захват;
#   * writer снимает маркер только после финального решения по кандидату.
# ---------------------------------------------------------------------------


async def queued_token(redis, tx_hash: str):
    """Возвращает token текущего маркера queued:{hash} или None."""
    return await redis.client.get(f"queued:{tx_hash}")


async def release_queued(redis, tx_hash: str, token: str) -> bool:
    """Снимает маркер queued:{hash}, только если он принадлежит ``token``.

    Compare-and-delete через Lua: иначе можно удалить чужой более новый маркер,
    захваченный после истечения нашей аренды.
    """
    script = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "return redis.call('del', KEYS[1]) else return 0 end"
    )
    result = await redis.client.eval(script, 1, f"queued:{tx_hash}", token)
    return bool(result)


async def clear_queued(redis, tx_hash: str):
    """Безусловно снимает маркер queued:{hash}.

    Используется writer'ом после финального решения: в этот момент маркер
    гарантированно «наш» либо уже истёк и освобождён.
    """
    await redis.client.delete(f"queued:{tx_hash}")


async def enqueue_candidate(rabbit, redis, tx_hash: str, *, skip_if_queued: bool) -> bool:
    """Ставит кандидата ``tx_hash`` в очередь out с дедупликацией по queued-маркеру.

    Возвращает True, если сообщение опубликовано, False — если кандидат уже
    стоит в очереди (маркер занят) и ``skip_if_queued=True``.

    Два режима ожидания маркера:

    * ``skip_if_queued=True`` (requeue deferred-детей из writer'а): если маркер
      занят — сразу выходим с False. Writer не должен ждать сам себя: событие
      для этого кандидата уже где-то в out.
    * ``skip_if_queued=False`` (обычная публикация из confirmer'а): ждём
      освобождения маркера до QUEUED_WAIT секунд. Если бюджет исчерпан —
      PublishError → caller делает nack/requeue; событие не теряется.

    При сбое публикации снимаем СВОЙ маркер (compare-and-delete) и бросаем
    PublishError: повторная доставка события возьмёт маркер заново.
    """
    key = f"queued:{tx_hash}"
    token = uuid.uuid4().hex
    deadline = asyncio.get_event_loop().time() + QUEUED_WAIT

    while True:
        # SET NX EX — атомарный захват маркера с арендой.
        acquired = await redis.client.set(key, token, nx=True, ex=QUEUED_LEASE)
        if acquired:
            break
        if skip_if_queued:
            logger.debug("queued:%s already held, skip", tx_hash)
            return False
        if asyncio.get_event_loop().time() >= deadline:
            raise PublishError(f"queued marker busy for {tx_hash}, requeue")
        await asyncio.sleep(QUEUED_POLL)

    try:
        await rabbit.publish_message(OUT_QUEUE, tx_hash)
    except Exception as e:
        # Публикация не удалась — освобождаем ТОЛЬКО свой маркер и сигнализируем
        # временную ошибку. Блокировка снята, повторная доставка события сможет
        # захватить маркер заново.
        await release_queued(redis, tx_hash, token)
        raise PublishError(f"publish {tx_hash} to {OUT_QUEUE} failed: {e}") from e

    return True
