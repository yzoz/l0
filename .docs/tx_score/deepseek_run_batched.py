"""
batched_memgraph.py — production-реализация пакетного алгоритма.

Заменяет InMemoryGraphStore на реальный Memgraph через neo4j-совместимый
драйвер (Memgraph поддерживает протокол Bolt).

Импортирует из score_engine:
  Node, ScoreResult, GraphStoreError, MissingInternalScoreError,
  CycleDetectedError, _filter_and_pack, _compute_batched
"""

from __future__ import annotations

import time
from fractions import Fraction
from typing import Optional, Dict, List, Iterable, Any, Callable

from neo4j import GraphDatabase, Driver

from score_engine import (
    Node,
    ScoreResult,
    GraphStoreError,
    MissingInternalScoreError,
    CycleDetectedError,
    _filter_and_pack,
    _compute_batched,
)


# =============================================================================
# 1. MEMGRAPH STORE (Cypher)
# =============================================================================
# Схема (подтвердить в реальном проекте!):
#   (:Transaction {id: string, internal_score: float})
#   (child:Transaction)-[:PARENT]->(parent:Transaction)
#
# Направление: от потомка к родителю (см. ТЗ §2.1).
# =============================================================================


class MemgraphStore:
    """Адаптер Memgraph: пакетные Cypher-запросы, счётчик queries/rows."""

    # --- Cypher: пакетное чтение узлов ---
    CYPHER_GET_NODES = """
    UNWIND $ids AS nid
    MATCH (n:Transaction {id: nid})
    RETURN n.id AS id, n.internal_score AS internal_score
    """

    # --- Cypher: пакетное чтение родителей (для BFS кандидатов) ---
    # Для каждого child из $ids возвращаем его родителей.
    CYPHER_GET_PARENTS_BATCH = """
    UNWIND $ids AS cid
    MATCH (child:Transaction {id: cid})-[:PARENT]->(parent:Transaction)
    RETURN cid AS child_id, parent.id AS parent_id
    """

    # --- Cypher: пакетное чтение потомков (для раскрытия зависимостей) ---
    # Для каждого parent из $ids возвращаем его потомков.
    CYPHER_GET_CHILDREN_BATCH = """
    UNWIND $ids AS pid
    MATCH (child:Transaction)-[:PARENT]->(parent:Transaction {id: pid})
    RETURN pid AS parent_id, child.id AS child_id
    """

    # --- Cypher: проверка существования триггера ---
    CYPHER_GET_TRIGGER = """
    MATCH (n:Transaction {id: $id})
    RETURN n.id AS id, n.internal_score AS internal_score
    """

    def __init__(self, uri: str, user: str = "", password: str = "",
                 database: str = "memgraph", batch_size: int = 1000):
        self._driver: Driver = GraphDatabase.driver(uri, auth=(user, password))
        self._database = database
        self._batch_size = batch_size
        self._stats = {"queries": 0, "rows_returned": 0}

    # -- lifecycle ---------------------------------------------------------
    def close(self) -> None:
        self._driver.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- low-level ---------------------------------------------------------
    def _run(self, query: str, **params) -> List[Dict[str, Any]]:
        self._stats["queries"] += 1
        with self._driver.session(database=self._database) as s:
            result = s.run(query, **params)
            rows = [dict(r) for r in result]
        self._stats["rows_returned"] += len(rows)
        return rows

    def _chunks(self, ids: List[str]) -> Iterable[List[str]]:
        """Дробим длинный список id на пакеты для UNWIND."""
        n = self._batch_size
        for i in range(0, len(ids), n):
            yield ids[i:i + n]

    # -- stats -------------------------------------------------------------
    def reset_stats(self) -> None:
        self._stats = {"queries": 0, "rows_returned": 0}

    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    # -- чтение ------------------------------------------------------------
    def get_node(self, node_id: str) -> Optional[Node]:
        rows = self._run(self.CYPHER_GET_TRIGGER, id=node_id)
        if not rows:
            return None
        r = rows[0]
        return Node(id=r["id"], internal_score=_to_fraction(r.get("internal_score")))

    def get_nodes_batch(self, ids: Iterable[str]) -> Dict[str, Node]:
        id_list = list(ids)
        out: Dict[str, Node] = {}
        for chunk in self._chunks(id_list):
            for r in self._run(self.CYPHER_GET_NODES, ids=chunk):
                out[r["id"]] = Node(
                    id=r["id"],
                    internal_score=_to_fraction(r.get("internal_score")),
                )
        return out

    def get_parents_batch(self, ids: Iterable[str]) -> Dict[str, List[str]]:
        id_list = list(ids)
        out: Dict[str, List[str]] = {}
        for chunk in self._chunks(id_list):
            for r in self._run(self.CYPHER_GET_PARENTS_BATCH, ids=chunk):
                out.setdefault(r["child_id"], []).append(r["parent_id"])
        return out

    def get_children_batch(self, ids: Iterable[str]) -> Dict[str, List[str]]:
        id_list = list(ids)
        out: Dict[str, List[str]] = {}
        for chunk in self._chunks(id_list):
            for r in self._run(self.CYPHER_GET_CHILDREN_BATCH, ids=chunk):
                out.setdefault(r["parent_id"], []).append(r["child_id"])
        return out

    # Удобные одиночные обёртки (нужны для get_node внутри BFS и т.п.)
    def get_parents(self, node_id: str) -> List[str]:
        return self.get_parents_batch([node_id]).get(node_id, [])

    def get_children(self, node_id: str) -> List[str]:
        return self.get_children_batch([node_id]).get(node_id, [])


# =============================================================================
# 2. КОНВЕРСИЯ ТИПОВ
# =============================================================================
# Memgraph хранит internal_score как float64. Для проверки корректности
# и точного сравнения с порогом переводим в Fraction, сохраняя битовую
# точность исходного float: Fraction(x) делает именно это.
# В production, если бизнес-модель допускает Decimal, заменить на Decimal(str(x)).
# =============================================================================

def _to_fraction(v: Any) -> Optional[Fraction]:
    if v is None:
        return None
    if isinstance(v, Fraction):
        return v
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):   # NaN/Inf
            return None
        return Fraction(v)
    if isinstance(v, int):
        return Fraction(v)
    raise GraphStoreError(f"Unsupported internal_score type: {type(v).__name__}")


# =============================================================================
# 3. BATCHED-АЛГОРИТМ НА MEMGRAPH
# =============================================================================
# Отличие от in-memory: методы get_*_batch ходят в Memgraph.
# Логика полностью совпадает с run_batched из score_engine:
#   1) BFS кандидатов (MAX_DEPTH ограничивает ТОЛЬКО этот BFS)
#   2) Раскрытие всех зависимостей (без ограничения глубины!)
#   3) Пакетное вычисление score волнами
#   4) Фильтрация по порогу
# =============================================================================

def find_candidates_memgraph(
    store: MemgraphStore,
    tx_id: str,
    max_depth: Optional[int],
) -> Dict[str, int]:
    """BFS к предкам, всё пакетно. Глубина ограничивает только кандидатов."""
    if max_depth is not None and max_depth < 0:
        raise ValueError(f"max_depth must be >= 0 or None, got {max_depth}")

    trigger = store.get_node(tx_id)
    if trigger is None:
        raise GraphStoreError(f"Trigger {tx_id} not found")
    if max_depth == 0:
        return {}

    depths: Dict[str, int] = {}
    frontier: List[str] = [tx_id]
    depth = 0
    while frontier:
        depth += 1
        if max_depth is not None and depth > max_depth:
            break
        parents_map = store.get_parents_batch(frontier)
        next_frontier: List[str] = []
        for nid in frontier:
            for p in parents_map.get(nid, ()):
                if p not in depths:
                    depths[p] = depth
                    next_frontier.append(p)
        frontier = next_frontier
    return depths


def expand_dependencies_memgraph(
    store: MemgraphStore,
    seeds: Iterable[str],
) -> tuple[Dict[str, Node], Dict[str, List[str]]]:
    """Раскрывает всех потомков кандидатов до корней. Пакетно. Без MAX_DEPTH."""
    nodes: Dict[str, Node] = {}
    children_map: Dict[str, List[str]] = {}
    visited: set[str] = set()

    frontier: List[str] = list(seeds)
    while frontier:
        frontier = [n for n in frontier if n not in visited]
        if not frontier:
            break

        batch_nodes = store.get_nodes_batch(frontier)
        batch_children = store.get_children_batch(frontier)

        next_frontier: List[str] = []
        seen_next: set[str] = set()
        for nid in frontier:
            node = batch_nodes.get(nid)
            if node is None:
                raise GraphStoreError(f"Node {nid} not found in store")
            if node.internal_score is None:
                raise MissingInternalScoreError(nid)
            nodes[nid] = node
            children_map[nid] = batch_children.get(nid, [])
            visited.add(nid)
            for c in children_map[nid]:
                if c not in visited and c not in seen_next:
                    next_frontier.append(c)
                    seen_next.add(c)
        frontier = next_frontier

    return nodes, children_map


def run_batched_memgraph(
    store: MemgraphStore,
    tx_id: str,
    max_depth: Optional[int] = None,
    threshold: Fraction = Fraction(1),
    num_type: Callable = Fraction,
) -> ScoreResult:
    store.reset_stats()
    t0 = time.perf_counter()

    try:
        candidates = find_candidates_memgraph(store, tx_id, max_depth)
    except GraphStoreError as e:
        return ScoreResult(tx_id, max_depth, threshold, {}, [],
                           {"queries": store.stats()["queries"], "error": str(e)},
                           error=str(e))
    t_cand = time.perf_counter()

    if not candidates:
        return ScoreResult(tx_id, max_depth, threshold, {}, [], {
            "queries": store.stats()["queries"],
            "rows_returned": store.stats()["rows_returned"],
            "nodes_loaded": 0, "edges_loaded": 0,
            "candidates_time_ms": (t_cand - t0) * 1000,
            "load_time_ms": 0.0, "compute_time_ms": 0.0,
            "total_time_ms": (t_cand - t0) * 1000,
        })

    try:
        nodes, children_map = expand_dependencies_memgraph(store, set(candidates.keys()))
        t_load = time.perf_counter()
        scores = _compute_batched(nodes, children_map, num_type)   # из score_engine
        t_compute = time.perf_counter()
    except GraphStoreError as e:
        return ScoreResult(tx_id, max_depth, threshold, {}, [],
                           {"queries": store.stats()["queries"], "error": str(e)},
                           error=str(e))

    edges = sum(len(c) for c in children_map.values())
    stats = {
        "queries": store.stats()["queries"],
        "rows_returned": store.stats()["rows_returned"],
        "nodes_loaded": len(nodes),
        "edges_loaded": edges,
        "candidates_time_ms": (t_cand - t0) * 1000,
        "load_time_ms": (t_load - t_cand) * 1000,
        "compute_time_ms": (t_compute - t_load) * 1000,
        "total_time_ms": (t_compute - t0) * 1000,
    }
    return _filter_and_pack(   # из score_engine
        tx_id, max_depth, threshold, candidates, scores, num_type, stats
    )


# =============================================================================
# 4. ФОРМИРОВАНИЕ ИТОГОВОГО СПИСКА ДЛЯ OUT
# =============================================================================
# Возвращает именно то, что нужно передать существующему механизму out:
# список транзакций (id, score, depth), прошедших все условия:
#   v != tx
#   1 <= d(tx, v) <= MAX_DEPTH   (или без верхней границы при None)
#   score(v) >= S
# =============================================================================

def build_out_payload(
    result: ScoreResult,
    convert_score: Callable[[Any], float] = float,
) -> List[Dict[str, Any]]:
    """Преобразует ScoreResult в список кандидатов для публикации в out."""
    if result.error:
        raise GraphStoreError(f"Cannot build out payload: {result.error}")

    payload: List[Dict[str, Any]] = []
    for q in result.qualified:
        payload.append({
            "id": q["id"],
            "score": convert_score(q["score"]),
            "depth": q["depth"],
        })
    # Стабильный порядок: по возрастанию глубины, затем по id — удобно для тестов
    payload.sort(key=lambda x: (x["depth"], x["id"]))
    return payload


# =============================================================================
# 5. ПРИМЕР ИСПОЛЬЗОВАНИЯ
# =============================================================================

def example() -> None:
    uri = "bolt://localhost:7687"
    with MemgraphStore(uri, user="", password="", batch_size=1000) as store:
        result = run_batched_memgraph(
            store,
            tx_id="tx_trigger_001",
            max_depth=2,
            threshold=Fraction(1, 2),
        )
        if result.error:
            print(f"Ошибка: {result.error}")
            return

        out_items = build_out_payload(result)
        print(f"Кандидатов в out: {len(out_items)}")
        for item in out_items:
            print(f"  {item['id']}  score={item['score']:.6f}  depth={item['depth']}")

        print(f"\nStats: {result.stats}")


if __name__ == "__main__":
    example()