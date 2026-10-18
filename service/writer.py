"""Writer — финальная ступень пайплайна.

LIFECYCLE ТРАНЗАКЦИИ (см. .docs/TODO_003.md):

    confirmer --out(plain-text tx_hash кандидата)--> writer --> Mongo/Redis/Memgraph

Writer потребляет очередь out (никакого сканирования графа по таймеру). Кандидат
приходит как plain-text tx_hash; по нему загружаются данные узла из Memgraph и
выполняется полный конвейер подтверждения:

  1. min-pool guard: в пуле должно оставаться > PARENTS (=3) кандидатов для
     будущего выбора родителей клиентом. Если <= 3 — событие отбрасывается,
     транзакция остаётся в графе БЕЗ каких-либо пометок;
  2. уже подтверждён (маркер ``confirmed:{hash}``) — добиваем «хвост»:
     перепостановка deferred-детей, удаление узла, снятие queued-маркера;
  3. классификация родителей: ok / pending / dead;
     dead → каскадное удаление поддерева;
     pending → маркер ``deferred:{hash}``, событие подтверждается и ждёт
     повторной постановки при подтверждении родителя;
  4. проверка баланса (кроме минтов);
  5. финализация: Mongo (источник истины) → Redis (балансы + confirmed-маркер
     с TTL) → удаление узла из графа.

ДЕРЖАТЕЛЬ ОЧЕРЕДИ ``queued:{hash}`` (см. src/database/queues.py) снимается
Только после финального решения по кандидату. На временных ошибках маркер
остаётся, сообщение возвращается в очередь (nack requeue) и обрабатывается
повторно.
"""

import logging
import os
import re

from aio_pika import IncomingMessage
from neo4j.exceptions import Neo4jError
from pymongo.errors import DuplicateKeyError

from src.config import CONFIRMED_TTL, OUT_QUEUE, PARENTS
from src.database.graph import Memgraph
from src.database.mongodb import MongoDB
from src.database.queues import (
    PublishError,
    RabbitMQ,
    clear_queued,
    enqueue_candidate,
    queued_token,
    release_queued,
)
from src.database.redis import Redis
from src.models.tx import TxWrite
from src.telemetry import metrics
from src.utils.profile import get_balance

logger = logging.getLogger(__name__)

# Сентинелы генезиса: у стартовых минтов parents=["0","1","2"] — это корни DAG,
# их нет ни в Mongo, ни в графе, но они всегда считаются подтверждёнными.
GENESIS_PARENTS = frozenset({"0", "1", "2"})

# tx_hash — 128 hex-символов sha512 (см. src/utils/data.py).
_HASH_RE = re.compile(r"^[0-9a-f]{128}$")


def _is_mint(tx: TxWrite) -> bool:
    """Минт (genesis) — транзакция без плательщика: ``credit`` пуст.

    Минт создаёт деньги (их pre-load в Redis делает genesis.py), а не переводит
    их, поэтому в Redis он не трогает ``balance:*`` — иначе начисление удвоится.
    """
    return not tx.credit.strip()


class WriterProcessor:
    """Обработка кандидатов из очереди out и финализация транзакций."""

    def __init__(self, db: MongoDB, redis: Redis, dag: Memgraph, rabbit: RabbitMQ):
        self.db = db
        self.redis = redis
        self.dag = dag
        self.rabbit = rabbit

    # ------------------------------------------------------------------
    # Точка входа: сообщение из очереди out
    # ------------------------------------------------------------------

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает одно сообщение out (plain-text tx_hash кандидата).

        Ack/nack:
          * ack — кандидат получил финальное решение (подтверждён, мёртв,
            отложен, отброшен min-pool guard'ом или уже обработан);
          * nack(requeue=True) — временная ошибка: маркер queued остаётся,
            сообщение вернётся и обработка повторится;
          * nack(requeue=False) — повреждённое сообщение (не tx_hash) или
            необработанное исключение: терминальный отказ.
        """
        tx_hash = message.body.decode("utf-8").strip()
        if not _HASH_RE.match(tx_hash):
            logger.error(f"Invalid out payload: {tx_hash[:64]!r}")
            metrics.inc("bad_format")
            #LOGGING: терминальный отказ — повреждённое событие out, сообщение исключается.
            # Для timeseries DB фиксируется событие:
            #   service   = "writer"
            #   event     = "reject"
            #   reason    = "bad-out-payload"
            #   outcome   = "drop"       # терминально, retry не будет
            #   payload   = tx_hash       # сырое тело сообщения (до 64 символов)
            #   queue     = "out"
            await message.nack(requeue=False)
            return

        try:
            outcome = await self.process_candidate(tx_hash)
        except PublishError as e:
            # Временная ошибка публикации (requeue deferred-детей). Узел ещё в
            # графе, маркер queued остаётся — повторная доставка добьёт «хвост».
            logger.warning(f"Publish error on {tx_hash[:8]}: {e}")
            metrics.inc("errors")
            await message.nack(requeue=True)
            return
        except Exception as e:
            logger.error(f"Unexpected writer error for {tx_hash[:8]}: {e}", exc_info=True)
            metrics.inc("errors")
            metrics.event("error", "write", f"{tx_hash[:8]} ошибка writer: {type(e).__name__}", ref=tx_hash)
            #LOGGING: терминальный отказ — необработанное исключение writer, сообщение исключается.
            # Для timeseries DB фиксируется событие:
            #   service   = "writer"
            #   event     = "reject"
            #   reason    = f"writer:{type(e).__name__}"
            #   outcome   = "drop"       # терминально, retry не будет
            #   tx_hash   = tx_hash       # идентификатор транзакции (128 hex)
            #   error     = str(e)       # текст исключения для диагностики
            #   queue     = "out"
            await message.nack(requeue=False)
            return

        if outcome == "retry":
            # Временная ошибка финализации: маркер queued НЕ трогаем, чтобы
            # кандидат не был повторно поставлен в out параллельно.
            await message.nack(requeue=True)
            return

        await message.ack()

    # ------------------------------------------------------------------
    # Обработка кандидата
    # ------------------------------------------------------------------

    async def process_candidate(self, tx_hash: str) -> str:
        """Обрабатывает кандидата ``tx_hash``. Возвращает "ack" или "retry".

        "retry" — временная ошибка (сбой Mongo/Redis/графа, сбой публикации
        deferred-детей): вызывающий код делает nack(requeue=True), маркер
        queued остаётся за нами. "ack" — финальное решение принято.
        """
        short = tx_hash[:8]

        # Шаг 1. Min-pool guard. В пуле должно оставаться больше PARENTS (=3)
        # транзакций, чтобы клиент мог выбрать родителей. Если кандидатов
        # меньше — событие отбрасывается, транзакция остаётся в графе без
        # каких-либо пометок (принятое поведение: повторное рассмотрение
        # возможно только по новому связанному событию).
        num_txs = await self._count_txs()
        if num_txs <= PARENTS:
            await clear_queued(self.redis, tx_hash)
            metrics.inc("dropped")
            metrics.event("info", "drop", f"{short} отброшен: пул <= {PARENTS}", ref=tx_hash, key=f"drop:{tx_hash}")
            return "ack"

        async with self.dag.driver.session() as session:
            # Идемпотентность: одна обработка кандидата за раз.
            async with self.redis.client.lock(f"writer:{tx_hash}"):
                token = await queued_token(self.redis, tx_hash)

                # Шаг 2. Уже подтверждён — добиваем «хвост» (повторная доставка
                # после сбоя на прошлой попытке, либо дубль сообщения out).
                if await self.redis.client.exists(f"confirmed:{tx_hash}"):
                    logger.info(f"{short} already confirmed, finishing tail")
                    return await self._finish_tail(session, tx_hash, token)

                # Шаг 3. Загрузка данных узла. Если узла нет — кандидат уже
                # обработан и удалён (мёртвый/дубликат), событие устарело.
                data = await self._fetch_node_data(session, tx_hash)
                if data is None:
                    logger.debug(f"{short} not in graph, ack and clear marker")
                    await clear_queued(self.redis, tx_hash)
                    return "ack"

                tx = self.create_tx(tx_hash, data)

                # Шаг 4. Целостность цепочки: подтверждать можно только тех, чьи
                # родители уже в реестре. Раньше — ждём, мертвы — каскад.
                parents_state = await self._parents_state(session, tx)
                if parents_state == "dead":
                    metrics.inc("parent_dead")
                    metrics.event(
                        "warn",
                        "parent-dead",
                        f"{short} мёртвый родитель — каскадное удаление",
                        ref=tx_hash,
                        key=f"parent-dead:{tx_hash}",
                    )
                    await self._kill_subtree(session, tx_hash)
                    await clear_queued(self.redis, tx_hash)
                    return "ack"

                if parents_state == "pending":
                    # Родитель ещё в пуле: помечаем кандидата deferred и ждём
                    # события о подтверждении родителя — тогда writer перепоставит
                    # его в out (см. _requeue_deferred_children) без сканирования
                    # графа. Маркер deferred без TTL: ждать можно бесконечно.
                    await self.redis.client.set(f"deferred:{tx_hash}", 1)
                    await clear_queued(self.redis, tx_hash)
                    metrics.inc("deferred")
                    metrics.event(
                        "info", "defer", f"{short} отложен до родителя", ref=tx_hash, key=f"defer:{tx_hash}"
                    )
                    return "ack"

                # Родители подтверждены — пометка ожидания больше не нужна.
                await self.redis.client.delete(f"deferred:{tx_hash}")

                # Шаг 5. Проверка баланса. У минта нет плательщика: проверка
                # баланса его только ложно отсекает (пустой credit даёт баланс 0),
                # поэтому пропускаем.
                if not _is_mint(tx):
                    balance = await get_balance(tx.credit, self.db, self.redis)
                    if balance < tx.amount:
                        metrics.inc("insufficient")
                        metrics.event(
                            "warn",
                            "funds",
                            f"{short} недостаточно средств",
                            ref=tx_hash,
                            key=f"funds:{tx_hash}",
                        )
                        await self._kill_subtree(session, tx_hash)
                        await clear_queued(self.redis, tx_hash)
                        return "ack"

                # Шаг 6. Финализация: Mongo → Redis. Узел из графа здесь ещё НЕ
                # удаляется — удаление входит в «хвост» (_finish_tail), чтобы
                # deferred-дети были перепоставлены ДО потери рёбер.
                status = await self._confirm(session, tx_hash, data)
                if status == "failed":
                    # Временная ошибка финализации (Mongo insert / Redis pipeline).
                    # Маркер queued остаётся, сообщение вернётся повторно.
                    return "retry"

                # status in ("confirmed", "dup") — добиваем «хвост».
                return await self._finish_tail(session, tx_hash, token)

    async def _finish_tail(self, session, tx_hash: str, token: str | None) -> str:
        """«Хвост» финализации: перепостановка deferred-детей, удаление узла, снятие маркера.

        Порядок важен: сначала перепоставляем deferred-прямых детей в out, и
        только потом удаляем узел из графа — рёбра CONFIRMS должны существовать
        в момент сбора детей. Если публикация детей упала — временная ошибка,
        узел остаётся в графе, повторная доставка дойдёт до сюда же через ветку
        ``confirmed:{hash}`` и повторит «хвост».
        """
        short = tx_hash[:8]
        try:
            await self._requeue_deferred_children(session, tx_hash)
        except PublishError as e:
            logger.warning(f"Failed to requeue deferred children of {short}: {e}")
            return "retry"

        # Узел удалён — рёбра потеряны, дети уже в очереди.
        await self._delete_node(session, tx_hash)
        await self._release_marker(tx_hash, token)
        logger.info(f"{short} finalized: node removed, marker released")
        return "ack"

    async def _release_marker(self, tx_hash: str, token: str | None) -> None:
        """Снимает queued-маркер после финального решения.

        Если token известен — compare-and-delete (не трогаем чужой более новый
        маркер). Если маркер уже истёк (token None) — безусловное удаление:
        решение финально, кандидат обработан.
        """
        if token is not None:
            await release_queued(self.redis, tx_hash, token)
        else:
            await clear_queued(self.redis, tx_hash)

    # ------------------------------------------------------------------
    # Подтверждение: Mongo (источник истины) → Redis (балансы + маркер)
    # ------------------------------------------------------------------

    async def _confirm(self, session, tx_hash: str, data: dict) -> str:
        """Подтверждает одну транзакцию. Возвращает "confirmed" | "dup" | "failed".

        Порядок Mongo -> Redis соблюдается осознанно: MongoDB — источник истины,
        запись в неё выполняется первой, а при сбое последующих шагов
        вставленный документ удаляется (компенсация).

        Минт (``credit`` пуст) попадает в реестр, но не двигает ``balance:*`` в
        Redis: его баланс уже задан pre-load-ом genesis, повторное начисление
        удвоило бы сумму.

        Узел из графа здесь НЕ удаляется — это делает _finish_tail после
        перепостановки deferred-детей.
        """
        tx = self.create_tx(tx_hash, data)
        confirmed_key = f"confirmed:{tx.hash}"

        # 1. MongoDB — источник истины.
        try:
            inserted = await self.db.insert_tx(tx)
        except DuplicateKeyError:
            # Транзакция уже подтверждена ранее (повторная доставка / дубль).
            # Узел удалит «хвост», второй документ не создаётся.
            logger.warning("Duplicate key error for hash: %s", tx.hash)
            metrics.inc("dup")
            metrics.event("warn", "dup", f"{tx.hash[:8]} дубликат записи", ref=tx.hash, key=f"dup:{tx.hash}")
            return "dup"

        if not inserted:
            logger.error("Failed to insert confirmed TX: %s", tx.hash)
            metrics.inc("insert_errors")
            metrics.event("error", "insert", f"{tx.hash[:8]} ошибка записи", ref=tx.hash, key=f"insert:{tx.hash}")
            return "failed"
        logger.info("Confirmed TX saved: %s", tx.hash)

        # 2. Redis — маркер подтверждения; для перевода дополнительно обновляются
        # балансы. Минт balance:* не трогает — его pre-load уже установлен genesis.
        try:
            async with self.redis.client.pipeline() as pipe:
                if not _is_mint(tx):
                    await pipe.incrby(f"balance:{tx.debit}", tx.amount)
                    await pipe.decrby(f"balance:{tx.credit}", tx.amount)
                await pipe.set(confirmed_key, 1, ex=CONFIRMED_TTL)
                await pipe.execute()
        except Exception as e:
            # Компенсация: откатываем источник истины, чтобы транзакция
            # не осталась подтверждённой без обновлённых балансов.
            logger.error("Redis pipeline failed for %s, rolling back Mongo: %s", tx.hash, e)
            metrics.inc("redis_errors")
            metrics.inc("compensation")
            metrics.event(
                "error", "compensate", f"{tx.hash[:8]} компенсация Mongo", ref=tx.hash, key=f"compensate:{tx.hash}"
            )
            try:
                await self.db.tx.delete_one({"hash": tx.hash})
            except Exception:
                logger.error("Failed to roll back Mongo TX %s", tx.hash, exc_info=True)
                metrics.inc("compensation_errors")
            return "failed"

        metrics.inc("confirmed")
        metrics.event("ok", "confirm", f"{tx.hash[:8]} подтверждена", ref=tx.hash, key=f"confirm:{tx.hash}")
        return "confirmed"

    # ------------------------------------------------------------------
    # Перепостановка deferred-детей в out
    # ------------------------------------------------------------------

    async def _requeue_deferred_children(self, session, tx_hash: str) -> None:
        """Перепоставляет в out deferred-прямых детей подтверждённого узла.

        Когда родитель подтверждается, его отложенные дети должны быть
        пересмотрены writer'ом без сканирования графа. Рёбра CONFIRMS ещё
        существуют (узел не удалён) — по ним находим детей, у которых стоит
        маркер ``deferred:{hash}``, и публикуем их в out.

        Публикация идёт с ``skip_if_queued=True``: если кандидат уже стоит в
        очереди (маркер queued занят), пропускаем — дубль не нужен. Сбой
        публикации — PublishError → временная ошибка, дети не теряются.
        """
        result = await session.run(
            """
            MATCH (t:Transaction {hash: $hash})-[:CONFIRMS]->(c:Transaction)
            RETURN c.hash AS hash
            """,
            hash=tx_hash,
        )
        children = [r["hash"] async for r in result]
        for child_hash in children:
            if await self.redis.client.exists(f"deferred:{child_hash}"):
                published = await enqueue_candidate(self.rabbit, self.redis, child_hash, skip_if_queued=True)
                if published:
                    metrics.inc("requeued")
                    metrics.event(
                        "info",
                        "requeue",
                        f"{child_hash[:8]} перепоставлен после подтверждения родителя",
                        ref=child_hash,
                        key=f"requeue:{child_hash}",
                    )
                else:
                    # Уже стоит в очереди — дубль не требуется.
                    metrics.inc("requeue_skipped")

    # ------------------------------------------------------------------
    # Граф: чтение, удаление, каскад
    # ------------------------------------------------------------------

    async def _count_txs(self) -> int:
        """Возвращает текущее число узлов транзакций в графе (размер пула)."""
        async with self.dag.driver.session() as session:
            result = await session.run("MATCH (t:Transaction) RETURN count(t) AS total")
            record = await result.single()
            return record["total"]

    async def _fetch_node_data(self, session, tx_hash: str) -> dict | None:
        """Возвращает ``t.data`` узла по хэшу или None, если узла нет."""
        result = await session.run(
            "MATCH (t:Transaction {hash: $hash}) RETURN t.data AS data",
            hash=tx_hash,
        )
        record = await result.single()
        return record["data"] if record else None

    async def _delete_node(self, session, tx_hash: str) -> bool:
        """Удаляет узел транзакции из графа Memgraph."""
        try:
            async with self.redis.client.lock(f"dag:{tx_hash}"):
                await session.run(
                    """
                    MATCH (t:Transaction {hash: $hash})
                    DETACH DELETE t
                    """,
                    hash=tx_hash,
                )
            return True
        except Neo4jError as e:
            logger.error("Failed to delete graph node %s: %s", tx_hash, e)
            metrics.inc("delete_errors")
            metrics.event(
                "error", "delete", f"{tx_hash[:8]} ошибка удаления узла", ref=tx_hash, key=f"delete:{tx_hash}"
            )
            return False

    async def _parents_state(self, session, tx: TxWrite) -> str:
        """Классифицирует родителей транзакции: ``ok`` | ``pending`` | ``dead``.

        * ``ok`` — каждый родитель либо сентинел генезиса, либо уже подтверждён
          (запись в MongoDB — источник истины — или маркер ``confirmed:*``);
        * ``pending`` — родитель ещё в пуле (узел в графе): подтверждаться рано,
          нужно дождаться его подтверждения;
        * ``dead`` — родитель выбыл, не будучи подтверждённым: цепочка порвана,
          подтвердиться невозможно, остаётся каскадное удаление.
        """
        pending = False
        for parent_hash in tx.parents:
            if parent_hash in GENESIS_PARENTS:
                continue
            if await self.db.find_one({"hash": parent_hash}):
                continue
            if await self.redis.client.exists(f"confirmed:{parent_hash}"):
                continue
            result = await session.run(
                """
                MATCH (t:Transaction {hash: $hash})
                RETURN t.hash LIMIT 1
                """,
                hash=parent_hash,
            )
            if await result.single():
                pending = True
                continue
            return "dead"
        return "pending" if pending else "ok"

    async def _kill_subtree(self, session, tx_hash: str) -> bool:
        """Каскадно убирает мёртвый узел и всё его неподтверждённое поддерево.

        Применяется, когда транзакция уже не сможет подтвердиться (мёртвый
        родитель / недостаточно средств). Потомки в графе гарантированно
        неподтверждены (топологический порядок confirm), поэтому умирают вместе
        с ней; резервы ``uniq:*``, маркеры ``deferred:*`` и ``queued:*``
        снимаются, чтобы адреса могли ретраить транзакции, а очередь не
        хранила зависшие кандидаты.
        """
        try:
            async with self.redis.client.lock(f"dag:{tx_hash}"):
                result = await session.run(
                    """
                    MATCH (t:Transaction {hash: $hash})
                    OPTIONAL MATCH (t)-[:CONFIRMS*0..]->(d:Transaction)
                    WITH DISTINCT d WHERE d IS NOT NULL
                    RETURN d.hash AS hash, d.data AS data
                    """,
                    hash=tx_hash,
                )
                nodes = [{"hash": r["hash"], "data": r["data"] or {}} async for r in result]
                if nodes:
                    await session.run(
                        """
                        MATCH (t:Transaction)
                        WHERE t.hash IN $hashes
                        DETACH DELETE t
                        """,
                        hashes=[n["hash"] for n in nodes],
                    )
            # Снятие резервов: подтверждёнными эти узлы быть не могут.
            for n in nodes:
                data = n["data"]
                try:
                    await self.redis.client.delete(f"uniq:{data.get('credit', '')}:{data.get('uniq', '')}")
                    await self.redis.client.delete(f"deferred:{n['hash']}")
                    await self.redis.client.delete(f"queued:{n['hash']}")
                except Exception as e:
                    logger.error("Failed to release keys for %s: %s", n["hash"], e)
            return True
        except Neo4jError as e:
            logger.error("Failed to kill subtree %s: %s", tx_hash, e)
            metrics.inc("delete_errors")
            metrics.event(
                "error", "delete", f"{tx_hash[:8]} ошибка удаления поддерева", ref=tx_hash, key=f"delete:{tx_hash}"
            )
            return False

    def create_tx(self, hash: str, data: dict) -> TxWrite:
        tx_dict = dict(data)
        tx_dict["hash"] = hash
        return TxWrite(**tx_dict)


class WriterService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = Redis()
        self.dag = Memgraph()
        self.rabbit = RabbitMQ()
        self.processor = WriterProcessor(self.db, self.redis, self.dag, self.rabbit)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Writer process {os.getpid()} started")
            await self.rabbit.consume_messages(OUT_QUEUE, self.processor.process_message, stop_event=stop_event)
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
