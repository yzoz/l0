"""Consumer — первая ступень пайплайна.

LIFECYCLE ТРАНЗАКЦИИ (см. .docs/TODO_003.md):

    API --in(JSON TxStore)--> consumer
    consumer --Memgraph(узел + рёбра CONFIRMS)--> граф
    consumer --mid(plain-text tx_hash)--> confirmer
    confirmer --out(tx_hash кандидатов со score>=1)--> writer

КРИТИЧНО: запись в Memgraph и публикация в mid НЕ атомарны. Если узел уже
записан, а публикация mid упала, сообщение из in НЕ подтверждается и вернётся
повторно. Поэтому порядок обработки — сначала проверка графа по хэшу, и только
потом проверка дубликата:

  1. пересчитываем tx_hash из содержимого сообщения;
  2. если узел с таким хэшем УЖЕ есть в графе — это ретрай после сбоя публикации
     mid или повторная доставка. Сверяем данные, чиним рёбра при необходимости,
     публикуем mid и подтверждаем сообщение. Новый узел НЕ создаём;
  3. иначе — обычный путь: дубликат → баланс → родители → запись в граф → mid →
     подтверждение сообщения.

uniq-резерв (``uniq:{credit}:{uniq}``) снимается ТОЛЬКО на путях отказа, где
узел ещё НЕ записан в граф. Если узел записан (или пишется повторно), резерв
остаётся навсегда — это guard от повторной вставки того же (credit, uniq).
"""

import asyncio
import json
import logging
import os
import time

from aio_pika import IncomingMessage
from neo4j.exceptions import Neo4jError, TransientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import IN_QUEUE, MID_QUEUE
from src.database.graph import Memgraph
from src.database.mongodb import MongoDB
from src.database.queues import PublishError, RabbitMQ
from src.database.redis import Redis
from src.models.tx import TxStore
from src.telemetry import metrics
from src.utils.data import hash_hex, ordered_sum
from src.utils.profile import get_balance

logger = logging.getLogger(__name__)


class ConsumerProcessor:
    """Обработка сообщений очереди in и запись транзакций в граф."""

    def __init__(self, rabbit: RabbitMQ, db: MongoDB, redis: Redis, dag: Memgraph):
        self.db = db
        self.rabbit = rabbit
        self.redis = redis
        self.dag = dag

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает входящее сообщение из очереди in.

        Правило ack/nack:
          * ack — только после успешной записи в граф И успешной публикации mid;
          * nack(requeue=True) — временные ошибки (таймаут, сбой публикации mid,
            сбой графа): сообщение вернётся и будет обработано повторно;
          * nack(requeue=False) + копия в DLQ — бизнес-отказы (bad-json, дубликат,
            недостаточно средств, нет родителя, цикл, конфликт данных).
        """
        try:
            tx = self.parse_message(message)
            await self.process_transaction(tx, message)
        except json.JSONDecodeError:
            logger.error("Failed to parse message: Invalid JSON format")
            metrics.inc("bad_format")
            metrics.event("error", "format", "формат сообщения: невалидный JSON", key="format")
            await self._reject(message, "bad-json")
        except asyncio.TimeoutError:
            logger.error("Timeout during processing")
            metrics.inc("error")
            metrics.event("error", "timeout", "таймаут обработки", key="timeout")
            await message.nack(requeue=True)
        except Exception as e:
            logger.error(f"Unexpected processing error: {e}", exc_info=True)
            metrics.inc("error")
            metrics.event("error", "error", f"ошибка обработки: {type(e).__name__}", key=f"error:{type(e).__name__}")
            await self._reject(message, f"error:{type(e).__name__}")

    async def _reject(self, message: IncomingMessage, reason: str) -> None:
        """Отбраковывает сообщение: копия уходит в DLQ, оригинал — в отказ."""
        await self.rabbit.publish_to_dlq(message, reason)
        await message.nack(requeue=False)

    def parse_message(self, message: IncomingMessage) -> TxStore:
        """Преобразует сырые данные сообщения в объект TxStore.

        Валидация модели здесь же фиксирует форму транзакции: ровно три
        уникальных родителя (см. Tx.parents).
        """
        data = json.loads(message.body.decode("utf-8"))
        content = data["content"]
        content["sign"] = data["sign"]
        return TxStore(**content)

    # ------------------------------------------------------------------
    # Основной путь обработки
    # ------------------------------------------------------------------

    async def process_transaction(self, tx: TxStore, message: IncomingMessage) -> None:
        """Обрабатывает одну транзакцию из очереди in.

        Порядок шагов важен — см. докстринг модуля про неделимость записи в
        граф и публикации mid.
        """
        # Хэш считается по полям TxStore (parents/credit/debit/amount/uniq/msg/sign)
        # тем же способом, что и в API /send/ — клиентский tx_hash обязан совпадать.
        tx_hash = hash_hex(ordered_sum(dict(tx)))
        short = tx_hash[:8]
        metrics.inc("received")

        async with self.redis.client.lock(f"consumer:{tx_hash}"):
            # Шаг 0. Возможно, транзакция УЖЕ записана в граф (ретрай после сбоя
            # публикации mid или повторная доставка сообщения). Проверяем граф
            # ДО проверки дубликата: иначе uniq-гард забраковал бы легитимный
            # ретрай как дубликат и событие потерялось бы.
            existing = await self._fetch_graph_data(tx_hash)
            if existing is not None:
                await self._retry_inserted(tx_hash, tx, existing, message)
                return

            # Обычный путь: транзакции в графе ещё нет.
            # Шаг 1. Проверка дубликатов (заодно резервирует uniq в Redis).
            if await self.check_duplicate(tx):
                logger.info(f"Duplicate transaction: {tx_hash}")
                metrics.inc("dup")
                metrics.event("warn", "dup", f"{short} дубликат", ref=tx_hash, key=f"dup:{tx_hash}")
                await self._reject(message, "dup")
                return

            # Дальше uniq зарезервирован: на любом отказе резерв снимается
            # (узла в графе ещё нет), иначе ретрай легитимной транзакции был бы
            # забракован как дубликат. После успешной записи узла резерв остаётся.
            try:
                # Шаг 2. Проверка баланса.
                if not await self.verify_balance(tx):
                    logger.info(f"Insufficient funds for {tx.credit}")
                    metrics.inc("no_funds")
                    metrics.event(
                        "warn", "funds", f"{short} недостаточно средств", ref=tx_hash, key=f"funds:{tx_hash}"
                    )
                    await self._reject(message, "funds")
                    await self.release_uniq(tx)
                    return

                # Шаг 3. Проверка наличия родителей (в графе или подтверждённых).
                if not await self.verify_parents(tx_hash, tx):
                    metrics.inc("no_parent")
                    metrics.event("warn", "parent", f"{short} нет родителя", ref=tx_hash, key=f"parent:{tx_hash}")
                    await self._reject(message, "parent")
                    await self.release_uniq(tx)
                    return

                # Шаг 4. Запись узла и рёбер в граф.
                if not await self.add_to_graph(tx_hash, tx):
                    await self._reject(message, "graph")
                    await self.release_uniq(tx)
                    return

                # Шаг 5. Уведомление confirmer'а (mid). Публикуем ДО ack: если
                # публикация упала, сообщение вернётся, а узел уже в графе —
                # сработает recovery-путь (шаг 0), который перепубликует mid.
                if not await self._publish_mid(tx_hash):
                    logger.error(f"Failed to publish mid for {tx_hash}, requeue")
                    # НЕ снимаем uniq-резерв: узел уже записан в граф, резерв
                    # должен остаться guard'ом от повторной вставки.
                    await message.nack(requeue=True)
                    return

                await message.ack()
            except PublishError:
                # Временная ошибка публикации mid/очередей: событие не потеряно,
                # сообщение вернётся повторно. uniq-резерв НЕ снимаем, если узел
                # мог быть записан (add_to_graph перед publish mid) — безопаснее
                # оставить резерв, чем потерять guard.
                await message.nack(requeue=True)
                raise
            except BaseException:
                # Сбой/таймаут до записи узла: сообщение может вернуться в
                # очередь — резерв снимаем, чтобы повторная доставка не стала
                # дубликатом.
                await self.release_uniq(tx)
                raise

            logger.info(f"Transaction {tx_hash} processed and added to graph")
            metrics.inc("added")
            metrics.event("ok", "added", f"{short} добавлена", ref=tx_hash, key=f"added:{tx_hash}")

    # ------------------------------------------------------------------
    # Recovery: транзакция уже записана в граф
    # ------------------------------------------------------------------

    async def _retry_inserted(self, tx_hash: str, tx: TxStore, existing: dict, message: IncomingMessage) -> None:
        """Обрабатывает повторную доставку уже записанной транзакции.

        Возможные причины: сбой публикации mid после записи узла, сбой до ack,
        повторная доставка сообщения из in. Действия:

          1. сверяем данные из сообщения с данными узла в графе (без поля time
             — оно проставляется при записи и может отличаться при реплее);
             при расхождении — конфликт данных, в DLQ, второй узел не создаём;
          2. гарантируем наличие рёбер CONFIRMS (MERGE — идемпотентно) и
             отсутствие цикла;
          3. перепубликуем mid (это главная цель recovery);
          4. ack сообщения. Новый узел НЕ создаём, uniq-резерв НЕ трогаем —
             он остаётся guard'ом навсегда.
        """
        short = tx_hash[:8]
        metrics.inc("retried")
        metrics.event("info", "retry", f"{short} повторная доставка", ref=tx_hash, key=f"retry:{tx_hash}")

        # Шаг 1. Сверка данных: хэш уже вычислен из сообщения, значит совпавший
        # хэш при разных данных означает коллизию или порчу — не вставляем второй узел.
        incoming = dict(tx)
        stored = {k: v for k, v in existing.items() if k != "time"}
        if incoming != stored:
            logger.error(f"Hash/data conflict on retry: {tx_hash}")
            metrics.inc("conflict")
            metrics.event("error", "conflict", f"{short} конфликт данных", ref=tx_hash, key=f"conflict:{tx_hash}")
            await self._reject(message, "hash-conflict")
            return

        # Шаг 2. Рёбра могли не успеть создаться до сбоя — дописываем идемпотентно.
        if not await self._ensure_edges(tx_hash, tx):
            await message.nack(requeue=True)
            return

        # Шаг 3. Главная цель recovery — довести уведомление до confirmer'а.
        if not await self._publish_mid(tx_hash):
            logger.error(f"Failed to publish mid on retry for {tx_hash}, requeue")
            await message.nack(requeue=True)
            return

        # Шаг 4. Всё доведено до конца — подтверждаем исходное сообщение.
        await message.ack()
        logger.info(f"Retry of {tx_hash} completed: mid republished, acked")

    async def _fetch_graph_data(self, tx_hash: str) -> dict | None:
        """Возвращает данные узла ``t.data`` по хэшу или None, если узла нет."""
        async with self.dag.driver.session() as session:
            result = await session.run(
                "MATCH (t:Transaction {hash: $hash}) RETURN t.data AS data",
                hash=tx_hash,
            )
            record = await result.single()
            return record["data"] if record else None

    # ------------------------------------------------------------------
    # Публикация события в mid
    # ------------------------------------------------------------------

    async def _publish_mid(self, tx_hash: str) -> bool:
        """Публикует plain-text tx_hash в очередь mid.

        Сообщение mid — источник событий для confirmer'а: по хэшу он читает
        транзакцию и её родителей из Memgraph (граф — источник истины, никакой
        JSON-обёртки не нужно).

        Возвращает False при временной ошибке публикации; вызывающий код обязан
        сделать nack(requeue=True), чтобы не потерять событие.
        """
        try:
            await self.rabbit.publish_message(MID_QUEUE, tx_hash)
            return True
        except Exception as e:
            logger.error(f"Publish to {MID_QUEUE} failed for {tx_hash}: {e}")
            metrics.inc("mid_errors")
            metrics.event("error", "mid", f"{tx_hash[:8]} сбой публикации mid", ref=tx_hash, key=f"mid:{tx_hash}")
            return False

    # ------------------------------------------------------------------
    # Дубликаты и uniq-резерв
    # ------------------------------------------------------------------

    async def check_duplicate(self, tx: TxStore) -> bool:
        """Проверяет дубликат (credit, uniq) и атомарно резервирует uniq.

        Ключ ``uniq:{credit}:{uniq}`` ставится через SET NX — это primary-гард,
        ловящий даже параллельные in-flight транзакции; реестр MongoDB остаётся
        backstop-ом на случай потери Redis. Резерв снимается (``release_uniq``)
        на любом пути отказа ГДЕ УЗЛА ЕЩЁ НЕТ В ГРАФЕ; после успешного добавления
        в граф ключ остаётся навсегда.
        """
        reserved = await self.redis.client.set(f"uniq:{tx.credit}:{tx.uniq}", 1, nx=True)
        if not reserved:
            return True
        exists = await self.db.find_one({"credit": tx.credit, "uniq": tx.uniq})
        return bool(exists)

    async def release_uniq(self, tx: TxStore) -> None:
        """Снимает резерв uniq после отказа, чтобы транзакцию можно было ретраить.

        Вызывать ТОЛЬКО на путях, где узел гарантированно не записан в граф.
        """
        try:
            await self.redis.client.delete(f"uniq:{tx.credit}:{tx.uniq}")
        except Exception as e:
            logger.error("Failed to release uniq for %s: %s", tx.credit, e)

    # ------------------------------------------------------------------
    # Проверки баланса и родителей
    # ------------------------------------------------------------------

    async def verify_balance(self, tx: TxStore) -> bool:
        """Проверяет, достаточно ли средств для выполнения транзакции"""
        balance = await get_balance(tx.credit, self.db, self.redis)
        return balance >= tx.amount

    async def verify_parents(self, tx_hash: str, tx: TxStore) -> bool:
        """Проверяет наличие всех родительских транзакций по порядку и их подтверждение в Redis"""
        for parent_hash in tx.parents:
            # Проверка существования родителя в Memgraph
            async with self.dag.driver.session() as session:
                result = await session.run(
                    """
                    MATCH (t:Transaction {hash: $parent_hash})
                    RETURN t
                    """,
                    parent_hash=parent_hash,
                )
                record = await result.single()
                if not record:
                    logger.info(f"Parent transaction {parent_hash} not found for {tx_hash} in Graph")
                    # Проверка подтверждения родителя в Redis
                    confirmed_key = f"confirmed:{parent_hash}"
                    if not await self.redis.client.exists(confirmed_key):
                        logger.warning(f"Parent transaction {parent_hash} not found for {tx_hash}")
                        return False
        return True

    # ------------------------------------------------------------------
    # Запись в граф
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=0.1, max=2),
        retry=retry_if_exception_type(TransientError),
        reraise=True,
    )
    async def add_to_graph(self, tx_hash: str, tx: TxStore) -> bool:
        """Добавляет транзакцию в граф Memgraph (узел + рёбра CONFIRMS).

        Идемпотентно: узел и рёбра пишутся через MERGE, поэтому повторный вызов
        (ретрай после сбоя, recovery-путь) не создаст дубликатов. Поле weight
        не используется — score транзакции считается в confirmer'е в памяти и
        нигде не сохраняется.
        """
        async with self.redis.client.lock(f"dag:{tx_hash}"):
            async with self.dag.driver.session() as session:
                now = int(time.time_ns() / 1000)
                tx_data = dict(tx, time=now)
                try:
                    # Узел: MERGE по hash — если уже есть, ничего не меняем.
                    await session.run(
                        """
                        MERGE (t:Transaction {hash: $hash})
                        ON CREATE SET t.data = $data
                        """,
                        hash=tx_hash,
                        data=tx_data,
                    )
                    # Рёбра от родителей к нашему узлу (без подтверждённых — они
                    # уже удалены из графа и их рёбра бессмысленны).
                    if not await self._create_edges(tx_hash, tx):
                        return False
                    # Проверка на цикл, пути от узла к самому себе.
                    if not await self._check_cycle(tx_hash):
                        await session.run("MATCH (t:Transaction {hash: $hash}) DETACH DELETE t", hash=tx_hash)
                        return False
                    return True
                except Neo4jError as e:
                    logger.error(f"Service error: {e}")
                    metrics.inc("error")
                    metrics.event("error", "graph", f"{tx_hash[:8]} ошибка графа: {type(e).__name__}", ref=tx_hash)
                    return False

    async def _ensure_edges(self, tx_hash: str, tx: TxStore) -> bool:
        """Идемпотентно дописывает недостающие рёбра и проверяет отсутствие цикла.

        Используется recovery-путём: при сбое между вставкой узла и созданием
        рёбер повторная доставка должна довести граф до целостного состояния.
        """
        async with self.redis.client.lock(f"dag:{tx_hash}"):
            async with self.dag.driver.session():
                try:
                    if not await self._create_edges(tx_hash, tx):
                        return False
                    return await self._check_cycle(tx_hash)
                except Neo4jError as e:
                    logger.error(f"Service error on edge repair: {e}")
                    metrics.inc("error")
                    return False

    async def _create_edges(self, tx_hash: str, tx: TxStore) -> bool:
        """Создаёт рёбра CONFIRMS от неподтверждённых родителей к узлу (MERGE)."""
        async with self.dag.driver.session() as session:
            for parent in tx.parents:
                confirmed_key = f"confirmed:{parent}"
                if not await self.redis.client.exists(confirmed_key):
                    await session.run(
                        """
                        MATCH (parent:Transaction {hash: $parent_hash})
                        MATCH (child:Transaction {hash: $child_hash})
                        MERGE (parent)-[:CONFIRMS]->(child)
                        """,
                        parent_hash=parent,
                        child_hash=tx_hash,
                    )
        return True

    async def _check_cycle(self, tx_hash: str) -> bool:
        """Возвращает False, если от узла есть путь к самому себе (циклическая зависимость)."""
        async with self.dag.driver.session() as session:
            result = await session.run(
                """
                MATCH path = (t:Transaction)-[:CONFIRMS*]->(t)
                WHERE t.hash = $hash
                RETURN count(path) as cycle_count
                """,
                hash=tx_hash,
            )
            cycle_count = (await result.single())["cycle_count"]
            if cycle_count > 0:
                logger.warning(f"Cycle detected for {tx_hash}")
                metrics.inc("cycle")
                metrics.event(
                    "warn", "cycle", f"{tx_hash[:8]} цикл — узел удалён", ref=tx_hash, key=f"cycle:{tx_hash}"
                )
                return False
            return True


class ConsumerService:
    def __init__(self):
        self.db = MongoDB()
        self.rabbit = RabbitMQ()
        self.redis = Redis()
        self.dag = Memgraph()
        self.processor = ConsumerProcessor(self.rabbit, self.db, self.redis, self.dag)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Consumer process {os.getpid()} started")
            await self.rabbit.consume_messages(IN_QUEUE, self.processor.process_message, stop_event=stop_event)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.rabbit.connect()
        await self.db.connect()
        await self.dag.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        if self.rabbit.connection and not self.rabbit.connection.is_closed:
            await self.rabbit.close()
            logger.info("RabbitMQ connection closed")
        await self.db.close()
        await self.dag.close()
        logger.info("All connections closed")
