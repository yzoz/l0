#!/usr/bin/env python3
"""
Интеграция Score Recalculation Engine с Memgraph.

Алгоритмы (DFS / TOPOLOGICAL / BATCH) импортируются из score_recalculation.
Здесь только:
  - Cypher-запросы (пакетная загрузка предков и зависимостей);
  - сборка GraphSnapshot;
  - оркестрация и возврат списка кандидатов.

Предполагаемая схема (проверить в проекте!):
  (:Transaction {id: str, internal_score: float})
  (child:Transaction)-[:PARENT]->(parent:Transaction)
  Ребро направлено от потомка к родителю.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

# --- импорт готовых алгоритмов (не копируем) ---
from score_recalculation import (
    Algorithm,
    Candidate,
    GraphSnapshot,
    ScoreResult,
    recalculate_scores,
)

# ---------------------------------------------------------------------------
# Абстракция драйвера Memgraph (подставить реальный neo4j/mgclient)
# ---------------------------------------------------------------------------

class MemgraphClient:
    """
    Минимальный интерфейс. В production:
      from neo4j import GraphDatabase
      driver = GraphDatabase.driver(uri, auth=...)
      и execute() оборачивает session.run(...).data()
    """

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError("Подключите реальный драйвер Memgraph/Neo4j")


# ---------------------------------------------------------------------------
# Cypher-запросы (шаблоны)
# ---------------------------------------------------------------------------

# 1. Батч: по списку id вернуть их непосредственных родителей + internal_score родителя
CYPHER_PARENTS_OF = """
UNWIND $ids AS id
MATCH (child:Transaction {id: id})-[:PARENT]->(parent:Transaction)
RETURN
  child.id          AS child_id,
  parent.id         AS parent_id,
  parent.internal_score AS parent_internal_score
"""

# 2. Батч: по списку id вернуть их непосредственных потомков + internal_score потомка
#    (нужно для раскрытия зависимостей score)
CYPHER_CHILDREN_OF = """
UNWIND $ids AS id
MATCH (child:Transaction)-[:PARENT]->(parent:Transaction {id: id})
RETURN
  parent.id         AS parent_id,
  child.id          AS child_id,
  child.internal_score AS child_internal_score
"""

# 3. Батч: internal_score по списку id (если не пришёл вместе со связями)
CYPHER_INTERNAL_SCORES = """
UNWIND $ids AS id
MATCH (t:Transaction {id: id})
RETURN t.id AS id, t.internal_score AS internal_score
"""

# 4. Проверка существования триггера
CYPHER_EXISTS = """
MATCH (t:Transaction {id: $tx_id})
RETURN t.id AS id, t.internal_score AS internal_score
LIMIT 1
"""


# ---------------------------------------------------------------------------
# Пакетная загрузка
# ---------------------------------------------------------------------------

def _chunks(ids: Sequence[str], batch_size: int) -> List[List[str]]:
    return [list(ids[i : i + batch_size]) for i in range(0, len(ids), batch_size)]


def load_trigger(client: MemgraphClient, tx_id: str) -> Optional[Decimal]:
    rows = client.execute(CYPHER_EXISTS, {"tx_id": tx_id})
    if not rows:
        return None
    sc = rows[0].get("internal_score")
    if sc is None:
        raise ValueError(f"trigger {tx_id} has no internal_score")
    return Decimal(str(sc))


def fetch_parents_batch(
    client: MemgraphClient,
    ids: Sequence[str],
    batch_size: int,
) -> List[Dict[str, Any]]:
    """Все рёбра child→parent для пакета ids + internal_score родителей."""
    out: List[Dict[str, Any]] = []
    for chunk in _chunks(ids, batch_size):
        if not chunk:
            continue
        out.extend(client.execute(CYPHER_PARENTS_OF, {"ids": chunk}))
    return out


def fetch_children_batch(
    client: MemgraphClient,
    ids: Sequence[str],
    batch_size: int,
) -> List[Dict[str, Any]]:
    """Все рёбра parent←child для пакета ids + internal_score потомков."""
    out: List[Dict[str, Any]] = []
    for chunk in _chunks(ids, batch_size):
        if not chunk:
            continue
        out.extend(client.execute(CYPHER_CHILDREN_OF, {"ids": chunk}))
    return out


def fetch_internal_scores(
    client: MemgraphClient,
    ids: Sequence[str],
    batch_size: int,
) -> Dict[str, Decimal]:
    result: Dict[str, Decimal] = {}
    for chunk in _chunks(ids, batch_size):
        if not chunk:
            continue
        for row in client.execute(CYPHER_INTERNAL_SCORES, {"ids": chunk}):
            tid = row["id"]
            sc = row.get("internal_score")
            if sc is None:
                raise ValueError(f"missing internal_score for {tid}")
            result[tid] = Decimal(str(sc))
    return result


# ---------------------------------------------------------------------------
# Фаза 1: предки с минимальной глубиной (BFS вверх, пакетно)
# ---------------------------------------------------------------------------

def load_ancestors(
    client: MemgraphClient,
    tx_id: str,
    max_depth: Optional[int],
    batch_size: int = 256,
) -> Tuple[Dict[str, int], GraphSnapshot]:
    """
    BFS от tx_id вверх по PARENT.

    Возвращает:
      ancestor_depths: {id: min_depth}  (без самого tx_id)
      partial_graph:   GraphSnapshot с загруженными узлами/рёбрами на пути к предкам
    """
    graph = GraphSnapshot()
    ancestor_depths: Dict[str, int] = {}

    if max_depth is not None and max_depth < 0:
        raise ValueError("max_depth must be >= 0 or None")
    if max_depth == 0:
        return {}, graph

    # Убедимся, что триггер есть
    trigger_score = load_trigger(client, tx_id)
    if trigger_score is None:
        raise ValueError(f"trigger tx_id={tx_id} not found")
    graph.add_node(tx_id, trigger_score)

    frontier = [tx_id]
    depth = 0

    while frontier:
        if max_depth is not None and depth >= max_depth:
            break

        rows = fetch_parents_batch(client, frontier, batch_size)
        next_frontier: List[str] = []
        seen_in_level: Set[str] = set()

        for row in rows:
            child_id = row["child_id"]
            parent_id = row["parent_id"]
            parent_sc = row.get("parent_internal_score")

            if parent_sc is None:
                raise ValueError(f"missing internal_score for parent {parent_id}")

            graph.add_node(parent_id, Decimal(str(parent_sc)))
            graph.add_edge(child_id, parent_id)

            if parent_id not in ancestor_depths and parent_id != tx_id:
                ancestor_depths[parent_id] = depth + 1
                if parent_id not in seen_in_level:
                    seen_in_level.add(parent_id)
                    next_frontier.append(parent_id)

        frontier = next_frontier
        depth += 1

    return ancestor_depths, graph


# ---------------------------------------------------------------------------
# Фаза 2: раскрытие зависимостей (потомки кандидатов, без MAX_DEPTH)
# ---------------------------------------------------------------------------

def expand_dependencies(
    client: MemgraphClient,
    graph: GraphSnapshot,
    candidate_ids: Sequence[str],
    batch_size: int = 256,
) -> GraphSnapshot:
    """
    Итеративно подгружает всех потомков кандидатов (и их потомков…).
    MAX_DEPTH сюда не применяется.
    """
    needed: Set[str] = set(candidate_ids)
    frontier: List[str] = list(candidate_ids)

    while frontier:
        rows = fetch_children_batch(client, frontier, batch_size)
        next_frontier: List[str] = []
        seen: Set[str] = set()

        for row in rows:
            parent_id = row["parent_id"]
            child_id = row["child_id"]
            child_sc = row.get("child_internal_score")

            if child_sc is None:
                raise ValueError(f"missing internal_score for child {child_id}")

            graph.add_node(child_id, Decimal(str(child_sc)))
            graph.add_edge(child_id, parent_id)

            if child_id not in needed:
                needed.add(child_id)
                if child_id not in seen:
                    seen.add(child_id)
                    next_frontier.append(child_id)

        frontier = next_frontier

    return graph


# ---------------------------------------------------------------------------
# Главная функция: Cypher + Python → список кандидатов
# ---------------------------------------------------------------------------

def find_out_candidates(
    client: MemgraphClient,
    tx_id: str,
    max_depth: Optional[int] = None,
    threshold: float | Decimal = 0.5,
    algorithm: Algorithm = Algorithm.TOPOLOGICAL,
    batch_size: int = 256,
) -> List[Dict[str, Any]]:
    """
    Полный пайплайн:

      1. Пакетный BFS предков до max_depth.
      2. Раскрытие всех потомков кандидатов (для точного score).
      3. Вызов recalculate_scores (импорт из score_recalculation).
      4. Возврат списка найденных транзакций:

         [
           {"tx_id": "...", "depth": 1, "score": 0.42},
           ...
         ]

    Условия включения (как в ТЗ):
      - v != tx_id
      - 1 <= depth(v) <= max_depth  (или без верхней границы, если max_depth is None)
      - score(v) >= threshold
    """
    threshold = Decimal(str(threshold))

    # --- 1. Предки ---
    ancestor_depths, graph = load_ancestors(
        client, tx_id, max_depth, batch_size=batch_size
    )

    if not ancestor_depths:
        return []

    # --- 2. Зависимости для score ---
    graph = expand_dependencies(
        client, graph, list(ancestor_depths.keys()), batch_size=batch_size
    )

    # --- 3. Расчёт (готовые алгоритмы) ---
    result: ScoreResult = recalculate_scores(
        graph=graph,
        tx_id=tx_id,
        max_depth=max_depth,
        threshold=threshold,
        algorithm=algorithm,
        batch_size=batch_size,
    )

    if result.diagnostics.error:
        raise RuntimeError(result.diagnostics.error)

    # --- 4. Список транзакций ---
    return [
        {
            "tx_id": c.tx_id,
            "depth": c.depth,
            "score": float(c.score),
        }
        for c in result.candidates
    ]


# ---------------------------------------------------------------------------
# Пример использования (мок-клиент для локальной проверки)
# ---------------------------------------------------------------------------

class InMemoryMemgraphClient(MemgraphClient):
    """
    Мок: эмулирует Cypher поверх уже загруженного GraphSnapshot.
    Удобно для unit-тестов без реального Memgraph.
    """

    def __init__(self, graph: GraphSnapshot) -> None:
        self.g = graph

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        params = params or {}
        # Грубое, но устойчивое к пробелам распознавание шаблона
        compact = "".join(query.split())

        # EXISTS trigger
        if "MATCH(t:Transaction{id:$tx_id})" in compact:
            tx_id = params["tx_id"]
            if not self.g.has_node(tx_id):
                return []
            return [{"id": tx_id, "internal_score": float(self.g.internal[tx_id])}]

        # PARENTS OF: (child {id:id})-[:PARENT]->(parent)
        if "child:Transaction{id:id})-[:PARENT]->(parent:Transaction)" in compact:
            rows = []
            for cid in params.get("ids", []):
                for pid in self.g.parents.get(cid, []):
                    rows.append({
                        "child_id": cid,
                        "parent_id": pid,
                        "parent_internal_score": float(self.g.internal[pid]),
                    })
            return rows

        # CHILDREN OF: (child)-[:PARENT]->(parent {id:id})
        if "child:Transaction)-[:PARENT]->(parent:Transaction{id:id}" in compact:
            rows = []
            for pid in params.get("ids", []):
                for cid in self.g.children.get(pid, []):
                    rows.append({
                        "parent_id": pid,
                        "child_id": cid,
                        "child_internal_score": float(self.g.internal[cid]),
                    })
            return rows

        # INTERNAL SCORES
        if "MATCH(t:Transaction{id:id})" in compact and "internal_score" in compact:
            rows = []
            for tid in params.get("ids", []):
                if self.g.has_node(tid):
                    rows.append({
                        "id": tid,
                        "internal_score": float(self.g.internal[tid]),
                    })
            return rows

        raise NotImplementedError(f"Mock cannot handle query:\n{query}")


def demo() -> None:
    """Демо на синтетическом G2 через мок-клиент."""
    from score_recalculation import generate_g2_converging

    graph, tx_id = generate_g2_converging()
    client = InMemoryMemgraphClient(graph)

    candidates = find_out_candidates(
        client=client,
        tx_id=tx_id,
        max_depth=None,
        threshold=0.3,
        algorithm=Algorithm.TOPOLOGICAL,
        batch_size=64,
    )

    print(f"trigger={tx_id}")
    print(f"candidates ({len(candidates)}):")
    for c in candidates:
        print(f"  {c['tx_id']:10s}  depth={c['depth']}  score={c['score']:.6f}")


if __name__ == "__main__":
    demo()
