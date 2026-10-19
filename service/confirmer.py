"""Confirmer — вторая ступень пайплайна.

LIFECYCLE ТРАНЗАКЦИИ (см. .docs/TODO_004.md):

    consumer --mid(plain-text tx_hash)--> confirmer --out(tx_hash кандидата)--> writer

Confirmer обрабатывает СОБЫТИЯ, а не сканирует пул. На каждое событие mid
(новая транзакция, записанная в граф) пересчитывается score предков этой
транзакции — новые данные меняют score всех, до кого доходит вклад нового узла.

ФОРМУЛА SCORE (транзакции t):

    score(t) = internal_score(t) + SUM( score(child) / 3 )
      по всем непосредственным детям t (ребра CONFIRMS), рекурсивно по поддереву.

``internal_score = transfer_score(amount)`` precomputed consumer'ом при вставке
узла и сохранён в графе (см. service/consumer.py). Confirmer НЕ вызывает
transfer_score — только суммирует и делит.

* Вклад каждого ребёнка делится на фиксированную константу 3 — число родителей
  транзакции (PARENTS=3). НЕ на config-значение, а на литерал 3.
* Учитываются все ветви поддерева, а не только ведущая к триггеру.
* Порог: score >= 1.0.
* Score НИГДЕ не сохраняется — он вычисляется из internal_score при каждом
  событии.

АЛГОРИТМ. На событие — один пакетный Cypher-запрос всей топологии графа
(узлы, internal_score, рёбра CONFIRMS). Предки триггера ищутся BFS вверх
(обратно по ребрам) с ограничением MAX_DEPTH. Score считается итеративным
memoized DFS снизу вверх (без рекурсивных вызовов Python) по поддеревьям
кандидатов. Сложность O(V + E). В Memgraph ничего не пишется.

ДЕДУПЛИКАЦИЯ OUT: публикация кандидата идёт через enqueue_candidate, который
ставит Redis-маркер ``queued:{hash}`` (SET NX + аренда). Повторные события для
одного кандидата не создают дублей в очереди out.
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

# Глубина подъёма от триггера к кандидатам. None — без ограничения (до корней).
# Ограничивает ТОЛЬКО поиск предков; score кандидата всегда считается по всему
# его поддереву.
MAX_DEPTH: int | None = None

# tx_hash — 128 hex-символов sha512 (см. src/utils/data.py).
_HASH_RE = re.compile(r"^[0-9a-f]{128}$")


def find_ancestors(
    trigger: str, parents: dict[str, list[str]], max_depth: int | None
) -> dict[str, int]:
    """Возвращает предков ``trigger`` (hash -> глубина) по родительским связям.

    depth 1 — прямые родители, 2 — их родители и т.д. ``max_depth=None`` —
    без ограничения. При нескольких путях узел берётся один раз по кратчайшему
    пути (BFS). Триггер в результат не входит.
    """
    found: dict[str, int] = {}
    frontier = [trigger]
    depth = 0
    while frontier:
        depth += 1
        if max_depth is not None and depth > max_depth:
            break
        nxt: list[str] = []
        for node in frontier:
            for parent in parents.get(node, ()):
                if parent == trigger or parent in found:
                    continue
                found[parent] = depth
                nxt.append(parent)
        frontier = nxt
    return found


def compute_scores(
    internal: dict[str, float],
    children: dict[str, list[str]],
    roots,
) -> dict[str, float]:
    """Считает score для поддеревьев ``roots`` итеративным memoized DFS.

    score(t) = internal_score(t) + SUM( score(child) / CHILD_DIVISOR ) по всем
    непосредственным детям. Обход снизу вверх явным стеком — без рекурсивных
    вызовов Python. При цикле в графе поднимает ValueError (частичный результат
    не публикуется).
    """
    scores: dict[str, float] = {}
    for root in roots:
        if root in scores:
            continue
        stack = [(root, False)]
        on_stack: set[str] = set()
        while stack:
            node, expanded = stack.pop()
            if expanded:
                total = internal[node]
                for child in children.get(node, ()):
                    total += scores[child] / CHILD_DIVISOR
                scores[node] = total
                on_stack.discard(node)
                continue
            if node in scores:
                continue
            if node in on_stack:
                raise ValueError(f"cycle in CONFIRMS graph at {node[:8]}")
            on_stack.add(node)
            stack.append((node, True))
            for child in children.get(node, ()):
                if child not in scores:
                    stack.append((child, False))
    return scores


class ConfirmationProcessor:
    """Обработка событий mid: пересчёт score предков и публикация кандидатов в out."""

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
          * nack(requeue=False) — повреждённое сообщение (не tx_hash) или
            необработанное исключение: терминальный отказ.
        """
        tx_hash = message.body.decode("utf-8").strip()
        # Валидация формата: событие — строго plain-text tx_hash без JSON.
        if not _HASH_RE.match(tx_hash):
            logger.error(f"Invalid mid payload: {tx_hash[:64]!r}")
            metrics.inc("bad_format")
            #LOGGING: терминальный отказ — повреждённое событие mid, сообщение исключается.
            # Для timeseries DB фиксируется событие:
            #   service   = "confirmer"
            #   event     = "reject"
            #   reason    = "bad-mid-payload"
            #   outcome   = "drop"       # терминально, retry не будет
            #   payload   = tx_hash       # сырое тело сообщения (до 64 символов)
            #   queue     = "mid"
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
            #LOGGING: терминальный отказ — необработанное исключение confirmer, сообщение исключается.
            # Для timeseries DB фиксируется событие:
            #   service   = "confirmer"
            #   event     = "reject"
            #   reason    = f"confirmer:{type(e).__name__}"
            #   outcome   = "drop"       # терминально, retry не будет
            #   tx_hash   = tx_hash       # идентификатор транзакции (128 hex)
            #   error     = str(e)       # текст исключения для диагностики
            #   queue     = "mid"
            await message.nack(requeue=False)
            return

        await message.ack()

    async def _process_hash(self, tx_hash: str) -> None:
        """Пересчитывает score предков транзакции ``tx_hash`` и публикует кандидатов.

        Один пакетный Cypher-запрос читает весь граф. Score считается в памяти,
        в хранилища ничего не пишется.
        """
        metrics.inc("events")

        internal, children = await self._load_graph()

        # Узла нет — транзакция уже подтверждена и удалена writer'ом; пропускаем.
        if tx_hash not in internal:
            logger.debug(f"mid for {tx_hash[:8]}: tx not in graph, skip")
            metrics.inc("not_found")
            return

        parents: dict[str, list[str]] = {h: [] for h in internal}
        for parent, kids in children.items():
            for child in kids:
                parents[child].append(parent)

        ancestors = find_ancestors(tx_hash, parents, MAX_DEPTH)
        if not ancestors:
            return

        # Считаем score всех кандидатов до первой публикации: при цикле или
        # нарушении инварианта исключение поднимется, и частичный результат не
        # уйдёт в out.
        scores = compute_scores(internal, children, ancestors.keys())

        for candidate in ancestors:
            score = scores[candidate]
            metrics.inc("scored")
            metrics.observe("score", score)

            if score < SCORE_THRESHOLD:
                logger.info(f"candidate {candidate[:8]} score={score:.4f} < {SCORE_THRESHOLD}, skip")
                continue

            logger.info(f"candidate {candidate[:8]} score={score:.4f} > {SCORE_THRESHOLD}, out")
            # score >= 1 — кандидат готов к подтверждению. Ставим в out с
            # дедупликацией по queued-маркеру (ожидаем освобождения маркера до
            # QUEUED_WAIT; если не дождались — PublishError → requeue события).
            published = await enqueue_candidate(self.rabbit, self.redis, candidate, skip_if_queued=False)
            if published:
                metrics.inc("enqueued")
                metrics.event(
                    "info",
                    "out",
                    f"{candidate[:8]} score={score:.2f} → out",
                    ref=candidate,
                    key=f"out:{candidate}",
                )
            else:
                # skip_if_queued=False не возвращает False, но на всякий случай
                # учитываем: кандидат уже стоит в очереди.
                metrics.inc("dedup")

    # ------------------------------------------------------------------
    # Чтение графа
    # ------------------------------------------------------------------

    async def _load_graph(self) -> tuple[dict[str, float], dict[str, list[str]]]:
        """Один Cypher-запрос: hash, internal_score и дети каждого узла.

        Возвращает (internal, children). Нарушение инварианта (нечисловой
        internal_score или ссылка на отсутствующий узел) поднимает ValueError.
        """
        async with self.dag.driver.session() as session:
            result = await session.run(
                """
                MATCH (n:Transaction)
                OPTIONAL MATCH (n)-[:CONFIRMS]->(c:Transaction)
                RETURN n.hash AS hash, n.internal_score AS score, collect(c.hash) AS children
                """
            )
            internal: dict[str, float] = {}
            children: dict[str, list[str]] = {}
            async for record in result:
                h = record["hash"]
                s = record["score"]
                if isinstance(s, bool) or not isinstance(s, (int, float)):
                    raise ValueError(f"invalid internal_score for {h[:8]}")
                internal[h] = s
                children[h] = [c for c in (record["children"] or []) if c is not None]

        for h, kids in children.items():
            for child in kids:
                if child not in internal:
                    raise ValueError(f"edge to unknown node {child[:8]} from {h[:8]}")

        logger.info("Nodes: %s", len(internal))
        return internal, children


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
