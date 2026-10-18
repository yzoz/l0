"""Confirmer — вторая ступень пайплайна.

LIFECYCLE ТРАНЗАКЦИИ (см. .docs/TODO_003.md):

    consumer --mid(plain-text tx_hash)--> confirmer --out(tx_hash кандидата)--> writer

Confirmer обрабатывает СОБЫТИЯ, а не сканирует пул. На каждое событие mid
(новая транзакция, записанная в граф) пересчитывается score только ТРЁХ
родителей этой транзакции — новые данные (новый прямой ребёнок) меняют score
именно родителей.

ФОРМУЛА SCORE (транзакции t):

    score(t) = internal_score(t) + SUM( internal_score(child) / 3 )
      по прямым детям t (ребра CONFIRMS).

``internal_score = transfer_score(amount)`` precomputed consumer'ом при вставке
узла и сохранён в графе (см. service/consumer.py). Confirmer НЕ вызывает
transfer_score — только суммирует и делит. Весь расчёт — один Cypher-запрос.

* Вклад каждого ребёнка делится на фиксированную константу 3 — число родителей
  транзакции (PARENTS=3). НЕ на config-значение, а на литерал 3.
* Внуки и более дальние потомки НЕ участвуют: никакого затухания по глубине.
* Порог: score >= 1.0.
* Score НИГДЕ не сохраняется — он вычисляется из internal_score при каждом
  событии.
* Собственная оценка новой транзакции не требуется: transfer_score по формуле
  всегда < 1 (стремится к 1 при x → ∞), поэтому транзакция без детей не может
  пройти порог. Оцениваются только родители.

ДЕДУПЛИКАЦИЯ OUT: публикация кандидата идёт через enqueue_candidate, который
ставит Redis-маркер ``queued:{hash}`` (SET NX + аренда). Повторные события для
одного родителя не создают дублей в очереди out.
"""

import logging
import re

from aio_pika import IncomingMessage

from src.config import MID_QUEUE
from src.database.graph import Memgraph
from src.database.queues import PublishError, RabbitMQ, enqueue_candidate
from src.database.redis import Redis
from src.telemetry import metrics

logger = logging.getLogger(__name__)

# Порог прохождения кандидата: score >= SCORE_THRESHOLD уходит в очередь out.
SCORE_THRESHOLD = 1.0

# Фиксированное число родителей транзакции. Вклад каждого прямого ребёнка в
# score родителя делится ровно на 3. Связано с PARENTS=3 в src/config.py.
CHILD_DIVISOR = 3

# tx_hash — 128 hex-символов sha512 (см. src/utils/data.py).
_HASH_RE = re.compile(r"^[0-9a-f]{128}$")


def candidate_score(parent_internal: float, children_internal: list[float]) -> float:
    """Score родителя: его internal_score плюс вклад прямых детей.

    score = parent.internal_score
          + SUM( child.internal_score / CHILD_DIVISOR )

    ``internal_score`` — precomputed ``transfer_score(amount)``, записанный
    consumer'ом при вставке узла. Confirmer не вызывает формулу — только
    суммирует и делит. Прямые дети — только непосредственные потомки по
    ребрам CONFIRMS. Больше ни один уровень глубины не учитывается.
    """
    score = parent_internal
    for child_score in children_internal:
        score += child_score / CHILD_DIVISOR
    return score


class ConfirmationProcessor:
    """Обработка событий mid: пересчёт score родителей и публикация кандидатов в out."""

    def __init__(self, redis: Redis, dag: Memgraph, rabbit: RabbitMQ):
        self.redis = redis
        self.dag = dag
        self.rabbit = rabbit

    # ------------------------------------------------------------------
    # Обработка события mid
    # ------------------------------------------------------------------

    async def process_event(self, message: IncomingMessage) -> None:
        """Обрабатывает одно событие mid (plain-text tx_hash новой транзакции).

        Ack/nack:
          * ack — событие обработано (включая случай, когда транзакция уже
            удалена из графа как подтверждённая — тогда пропускаем);
          * nack(requeue=True) — временные ошибки графа/очередей, событие
            вернётся и будет обработано повторно;
          * DLQ — повреждённое сообщение (не tx_hash).
        """
        tx_hash = message.body.decode("utf-8").strip()
        # Валидация формата: событие — строго plain-text tx_hash без JSON.
        if not _HASH_RE.match(tx_hash):
            logger.error(f"Invalid mid payload: {tx_hash[:64]!r}")
            metrics.inc("bad_format")
            await self.rabbit.publish_to_dlq(message, "bad-mid-payload")
            await message.nack(requeue=False)
            return

        try:
            await self._process_hash(tx_hash)
        except PublishError as e:
            # Временная ошибка публикации out (в т.ч. занятость queued-маркера
            # сверх бюджета). Событие не теряем — requeue.
            logger.warning(f"Publish error on {tx_hash[:8]}: {e}")
            metrics.inc("errors")
            await message.nack(requeue=True)
            return
        except Exception as e:
            logger.error(f"Unexpected confirmer error for {tx_hash[:8]}: {e}", exc_info=True)
            metrics.inc("errors")
            metrics.event("error", "conf", f"{tx_hash[:8]} ошибка confirmer: {type(e).__name__}", ref=tx_hash)
            await self.rabbit.publish_to_dlq(message, f"confirmer:{type(e).__name__}")
            await message.nack(requeue=False)
            return

        await message.ack()

    async def _process_hash(self, tx_hash: str) -> None:
        """Пересчитывает score родителей транзакции ``tx_hash``.

        Memgraph — источник истины: по хэшу читаем транзакцию, её родителей и
        непосредственных детей каждого родителя. Ничего не пишем в хранилища.
        """
        metrics.inc("events")

        # Загружаем транзакцию. Если узла нет — транзакция уже подтверждена и
        # удалена writer'ом из графа; событие устарело, просто пропускаем.
        data = await self._fetch_tx_data(tx_hash)
        if data is None:
            logger.debug(f"mid for {tx_hash[:8]}: tx not in graph, skip")
            metrics.inc("not_found")
            return

        parents = data.get("parents") or []
        # У транзакции всегда PARENTS (=3) родителя, но защищаемся от пустых данных.
        for parent_hash in parents:
            await self._score_parent(parent_hash)

    async def _score_parent(self, parent_hash: str) -> None:
        """Считает score одного родителя и при необходимости ставит его в out.

        Один Cypher-запрос возвращает precomputed internal_score родителя и
        его прямых детей. Дальше — чистая арифметика (сумма / делитель).
        При параллельных событиях mid возможны конкурентные пересчёты одного
        родителя: это допустимо, очередь out дедуплицируется queued-маркером,
        а повторная обработка кандидата в writer'е идемпотентна.
        """
        row = await self._fetch_parent_scores(parent_hash)
        if row is None:
            # Родитель уже подтверждён/удалён из графа — считать нечего.
            logger.debug(f"parent {parent_hash[:8]} not in graph, skip")
            metrics.inc("not_found")
            return

        parent_score, children_scores = row
        score = candidate_score(parent_score, children_scores)
        metrics.inc("scored")
        metrics.observe("score", score)

        if score < SCORE_THRESHOLD:
            logger.debug(f"parent {parent_hash[:8]} score={score:.4f} < {SCORE_THRESHOLD}, skip")
            return

        # score >= 1 — кандидат готов к подтверждению. Ставим в out с
        # дедупликацией по queued-маркеру (ожидаем освобождения маркера до
        # QUEUED_WAIT; если не дождались — PublishError → requeue события).
        published = await enqueue_candidate(self.rabbit, self.redis, parent_hash, skip_if_queued=False)
        if published:
            metrics.inc("enqueued")
            metrics.event(
                "info",
                "out",
                f"{parent_hash[:8]} score={score:.2f} → out",
                ref=parent_hash,
                key=f"out:{parent_hash}",
            )
        else:
            # skip_if_queued=False не возвращает False, но на всякий случай
            # учитываем: кандидат уже стоит в очереди.
            metrics.inc("dedup")

    # ------------------------------------------------------------------
    # Чтение графа
    # ------------------------------------------------------------------

    async def _fetch_tx_data(self, tx_hash: str) -> dict | None:
        """Возвращает ``t.data`` транзакции по хэшу или None."""
        async with self.dag.driver.session() as session:
            result = await session.run(
                "MATCH (t:Transaction {hash: $hash}) RETURN t.data AS data",
                hash=tx_hash,
            )
            record = await result.single()
            return record["data"] if record else None

    async def _fetch_parent_scores(self, parent_hash: str) -> tuple[float, list[float]] | None:
        """Возвращает internal_score родителя и список internal_score его прямых детей.

        Один Cypher-запрос: precomputed значения из графа, никаких вычислений
        формулы на стороне Python. Прямые дети — только непосредственные
        потомки по ребрам CONFIRMS (OPTIONAL MATCH без глубины).
        """
        async with self.dag.driver.session() as session:
            result = await session.run(
                """
                MATCH (p:Transaction {hash: $hash})
                OPTIONAL MATCH (p)-[:CONFIRMS]->(c:Transaction)
                RETURN p.internal_score AS parent_score, collect(c.internal_score) AS children_scores
                """,
                hash=parent_hash,
            )
            record = await result.single()
            if not record or record["parent_score"] is None:
                return None
            children = [s for s in (record["children_scores"] or []) if s is not None]
            return record["parent_score"], children


class ConfirmationService:
    """Сервис confirmer: потребляет mid, публикует кандидатов в out."""

    def __init__(self):
        self.redis = Redis()
        self.dag = Memgraph()
        self.rabbit = RabbitMQ()
        self.processor = ConfirmationProcessor(self.redis, self.dag, self.rabbit)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info("Confirmer started")
            await self.rabbit.consume_messages(MID_QUEUE, self.processor.process_event, stop_event=stop_event)
        except Exception as e:
            logger.error(f"Confirmer error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.rabbit.connect()
        await self.dag.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        if self.rabbit.connection and not self.rabbit.connection.is_closed:
            await self.rabbit.close()
            logger.info("RabbitMQ connection closed")
        await self.dag.close()
        logger.info("Confirmer connections closed")
