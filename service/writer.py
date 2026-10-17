import asyncio
import logging
import os

from neo4j.exceptions import Neo4jError
from pymongo.errors import DuplicateKeyError

from src.config import CONFIRMED_TTL, POOL_SIZES, PARENTS
from src.database.graph import Memgraph
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.models.tx import TxWrite
from src.telemetry import metrics
from src.utils.profile import get_balance



logger = logging.getLogger(__name__)

# Сентинелы генезиса: у стартовых минтов parents=["0","1"] — это корни DAG,
# их нет ни в Mongo, ни в графе, но они всегда считаются подтверждёнными.
GENESIS_PARENTS = frozenset({"0", "1", "2"})


def _is_mint(tx: TxWrite) -> bool:
    """Минт (genesis) — транзакция без плательщика: ``credit`` пуст.

    Минт создаёт деньги (их pre-load в Redis делает genesis.py), а не переводит
    их, поэтому в Redis он не трогает ``balance:*`` — иначе начисление удвоится.
    """
    return not tx.credit.strip()


class WriterProcessor:
    def __init__(self, db: MongoDB, redis: Redis, dag: Memgraph):
        self.db = db
        self.redis = redis
        self.dag = dag

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
          нужно дождаться его бесконечно;
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
        с ней; резервы ``uniq:*`` и маркеры ``deferred:*`` снимаются, чтобы
        адреса могли ретраить транзакции.
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

    async def _confirm(self, session, tx_hash: str, data: dict) -> None:
        """Подтверждает одну транзакцию в явном порядке Mongo -> Redis -> Memgraph.

        MongoDB считается источником истины: запись в неё выполняется первой, а
        при сбое последующих шагов вставленный документ удаляется (компенсация).

        Минт (``credit`` пуст) попадает в реестр, но не двигает ``balance:*`` в
        Redis: его баланс уже задан pre-load-ом genesis, повторное начисление
        удвоило бы сумму.
        """
        tx = self.create_tx(tx_hash, data)
        confirmed_key = f"confirmed:{tx.hash}"

        # 1. MongoDB — источник истины.
        try:
            inserted = await self.db.insert_tx(tx)
        except DuplicateKeyError:
            logger.warning("Duplicate key error for hash: %s", tx.hash)
            metrics.inc("dup")
            metrics.event("warn", "dup", f"{tx.hash[:8]} дубликат записи", ref=tx.hash, key=f"dup:{tx.hash}")
            await self._delete_node(session, tx.hash)
            return

        if not inserted:
            logger.error("Failed to insert confirmed TX: %s", tx.hash)
            metrics.inc("insert_errors")
            metrics.event("error", "insert", f"{tx.hash[:8]} ошибка записи", ref=tx.hash, key=f"insert:{tx.hash}")
            return
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
            return

        # 3. Memgraph — очистка узла из графа.
        # На этом шаге данные уже согласованы; при сбое узел останется в графе,
        # но из-за маркера confirmed_key повторно обработан не будет.
        metrics.inc("confirmed")
        metrics.event("ok", "confirm", f"{tx.hash[:8]} подтверждена", ref=tx.hash, key=f"confirm:{tx.hash}")
        await self._delete_node(session, tx.hash)

    async def check_pool(self):
        
        """Проверяет пул неподтверждённых транзакций и подтверждает те, что превышают порог"""
        async with self.dag.driver.session() as session:
            result = await session.run("MATCH (t:Transaction) RETURN count(t) AS total")
            record = await result.single()
            num_txs = record["total"]
            CONFIRM_WEIGHT = POOL_SIZES[0][1]
            for threshold, w, _ in POOL_SIZES:
                if num_txs > threshold:
                    CONFIRM_WEIGHT = w
            # Получение транзакций с весом >= порога
            result = await session.run(
                """
                MATCH (t:Transaction)
                WHERE t.weight >= $threshold
                RETURN t.hash, t.data
                """,
                threshold=CONFIRM_WEIGHT,
            )
            confirmed = [{"hash": record["t.hash"], "data": record["t.data"]} async for record in result]
            metrics.set("above_threshold", len(confirmed))
            if not num_txs <= PARENTS:
                for t in confirmed:
                    confirmed_key = f"confirmed:{t['hash']}"
                    async with self.redis.client.lock(f"writer:{t['hash']}"):
                        if await self.redis.client.exists(confirmed_key):
                            continue

                        tx = self.create_tx(t["hash"], t["data"])

                        # Целостность цепочки: подтверждать можно только тех, чьи
                        # родители уже в реестре. Раньше — ждём, мертвы — каскад.
                        parents_state = await self._parents_state(session, tx)
                        if parents_state == "dead":
                            metrics.inc("parent_dead")
                            metrics.event(
                                "warn",
                                "parent-dead",
                                f"{t['hash'][:8]} мёртвый родитель — каскадное удаление",
                                ref=t["hash"],
                                key=f"parent-dead:{t['hash']}",
                            )
                            await self._kill_subtree(session, t["hash"])
                            continue
                        if parents_state == "pending":
                            # Родитель ещё в пуле: ждём его подтверждения
                            # бесконечно — маркер deferred без TTL.
                            await self.redis.client.set(f"deferred:{t['hash']}", 1)
                            metrics.inc("deferred")
                            continue
                        # Родители подтверждены — пометка ожидания больше не нужна.
                        await self.redis.client.delete(f"deferred:{t['hash']}")

                        # У минта нет плательщика: проверка баланса его только ложно
                        # отсекает (пустой credit даёт баланс 0), поэтому пропускаем.
                        if not _is_mint(tx):
                            balance = await get_balance(tx.credit, self.db, self.redis)
                            if balance < tx.amount:
                                # Недостаточно средств — транзакция не может быть подтверждена.
                                metrics.inc("insufficient")
                                metrics.event(
                                    "warn",
                                    "funds",
                                    f"{tx.hash[:8]} недостаточно средств",
                                    ref=tx.hash,
                                    key=f"funds:{tx.hash}",
                                )
                                await self._kill_subtree(session, tx.hash)
                                continue

                        await self._confirm(session, t["hash"], t["data"])

    def create_tx(self, hash: str, data: dict) -> TxWrite:
        tx_dict = dict(data)
        tx_dict["hash"] = hash
        return TxWrite(**tx_dict)

    async def periodic_check_pool(self, stop_event=None):
        while not (stop_event and stop_event.is_set()):
            await self.check_pool()
            await asyncio.sleep(0.1)


class WriterService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = Redis()
        self.dag = Memgraph()
        self.processor = WriterProcessor(self.db, self.redis, self.dag)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Writer process {os.getpid()} started")
            await self.processor.periodic_check_pool(stop_event)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.db.connect()
        await self.dag.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.db.close()
        await self.dag.close()
        logger.info("All connections closed")
