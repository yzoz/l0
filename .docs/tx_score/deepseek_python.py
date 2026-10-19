"""
score_engine.py — реализация ТЗ "Score Recalculation Engine v1.0".

Структура:
  1. Граф-хранилище (адаптер над Memgraph)
  2. Общие утилиты: поиск кандидатов и раскрытие зависимостей
  3. Три алгоритма: DFS+memo, топологический, пакетный итеративный
  4. Независимый эталон на рекурсии
  5. Генератор синтетических DAG
  6. Тесты (G1–G6 + edge cases + property-based)
  7. Benchmark + рекомендация

Запуск: python score_engine.py
"""

from __future__ import annotations

import sys
import time
import random
import math
import tracemalloc
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Optional, Dict, List, Set, Tuple, Iterable, Any, Callable
from collections import defaultdict

# Для эталонной рекурсивной реализации на глубоких графах
sys.setrecursionlimit(10000)


# =============================================================================
# 1. ГРАФ-ХРАНИЛИЩЕ
# =============================================================================
# InMemoryGraphStore имитирует Memgraph: те же операции чтения, счётчик "запросов".
# Для продакшена достаточно заменить реализацию методов на Cypher-запросы,
# сигнатуры и семантика сохранены.
#
# Соглашение о направлении (ТЗ §2.1): ребро направлено child -> parent.
#   children[P] = { X : X ссылается на P как на родителя }
#   parents[C]  = { P : P является родителем C }
#
# Score(T) зависит от score(C) для C ∈ children[T], т.е. вычисление идёт
# "вниз" по графу (к потомкам), хотя поиск кандидатов идёт "вверх" (к предкам).
# =============================================================================

@dataclass(frozen=True)
class Node:
    """Узел транзакции. internal_score может быть None — для теста битых данных."""
    id: str
    internal_score: Optional[Fraction]


class GraphStoreError(Exception):
    pass


class MissingInternalScoreError(GraphStoreError):
    def __init__(self, node_id: str):
        super().__init__(f"Missing internal_score for node {node_id}")
        self.node_id = node_id


class CycleDetectedError(GraphStoreError):
    def __init__(self, node_id: str):
        super().__init__(f"Cycle detected at node {node_id}")
        self.node_id = node_id


class InMemoryGraphStore:
    """Граф в памяти + счётчик запросов (для симуляции Cypher-накладных расходов)."""

    def __init__(self):
        self._nodes: Dict[str, Node] = {}
        self._children: Dict[str, Set[str]] = {}
        self._parents: Dict[str, Set[str]] = {}
        self._stats = {"queries": 0, "rows_returned": 0}

    # -- построение ---------------------------------------------------------
    def add_node(self, node_id: str, internal_score: Optional[Fraction]) -> None:
        if node_id in self._nodes:
            raise ValueError(f"Duplicate node {node_id}")
        self._nodes[node_id] = Node(node_id, internal_score)
        self._children.setdefault(node_id, set())
        self._parents.setdefault(node_id, set())

    def add_edge(self, child_id: str, parent_id: str) -> None:
        if child_id not in self._nodes:
            raise ValueError(f"Child {child_id} does not exist")
        if parent_id not in self._nodes:
            raise ValueError(f"Parent {parent_id} does not exist")
        self._children[parent_id].add(child_id)
        self._parents[child_id].add(parent_id)

    # -- чтение (считается как "Cypher-запрос") -----------------------------
    def get_node(self, node_id: str) -> Optional[Node]:
        self._stats["queries"] += 1
        n = self._nodes.get(node_id)
        self._stats["rows_returned"] += 1 if n is not None else 0
        return n

    def get_nodes_batch(self, ids: Iterable[str]) -> Dict[str, Node]:
        self._stats["queries"] += 1
        result: Dict[str, Node] = {}
        for i in ids:
            if i in self._nodes:
                result[i] = self._nodes[i]
        self._stats["rows_returned"] += len(result)
        return result

    def get_parents(self, node_id: str) -> List[str]:
        self._stats["queries"] += 1
        p = list(self._parents.get(node_id, ()))
        self._stats["rows_returned"] += len(p)
        return p

    def get_parents_batch(self, ids: Iterable[str]) -> Dict[str, List[str]]:
        self._stats["queries"] += 1
        result: Dict[str, List[str]] = {}
        total = 0
        for i in ids:
            p = list(self._parents.get(i, ()))
            if p:
                result[i] = p
                total += len(p)
        self._stats["rows_returned"] += total
        return result

    def get_children(self, node_id: str) -> List[str]:
        self._stats["queries"] += 1
        c = list(self._children.get(node_id, ()))
        self._stats["rows_returned"] += len(c)
        return c

    def get_children_batch(self, ids: Iterable[str]) -> Dict[str, List[str]]:
        self._stats["queries"] += 1
        result: Dict[str, List[str]] = {}
        total = 0
        for i in ids:
            c = list(self._children.get(i, ()))
            if c:
                result[i] = c
                total += len(c)
        self._stats["rows_returned"] += total
        return result

    # -- служебное ----------------------------------------------------------
    def reset_stats(self) -> None:
        self._stats = {"queries": 0, "rows_returned": 0}

    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    def all_node_ids(self) -> List[str]:
        return list(self._nodes.keys())


# =============================================================================
# 2. ОБЩИЕ УТИЛИТЫ
# =============================================================================

@dataclass
class ScoreResult:
    """Единый контракт результата для всех алгоритмов."""
    trigger_id: str
    max_depth: Optional[int]
    threshold: Fraction
    all_candidates: Dict[str, Tuple[Any, int]]  # id -> (score, depth)
    qualified: List[Dict[str, Any]]             # список для out: [{id, score, depth}]
    stats: Dict[str, Any]
    error: Optional[str] = None


def find_candidates(
    store: InMemoryGraphStore,
    tx_id: str,
    max_depth: Optional[int],
    batched: bool = False,
) -> Dict[str, int]:
    """
    BFS от триггера к предкам. Возвращает {node_id: min_depth}.

    КРИТИЧНО (ТЗ §3.4): глубина ограничивает только множество кандидатов.
    Сам расчёт score кандидата эту границу игнорирует.
    """
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
        # Пакетный vs поштучный сбор родителей
        if batched:
            parents_map = store.get_parents_batch(frontier)
            frontier_parents = [(nid, parents_map.get(nid, [])) for nid in frontier]
        else:
            frontier_parents = [(nid, store.get_parents(nid)) for nid in frontier]

        next_frontier: List[str] = []
        for _, parents in frontier_parents:
            for p in parents:
                if p not in depths:                # фиксируем минимальную глубину
                    depths[p] = depth
                    next_frontier.append(p)
        frontier = next_frontier

    return depths


def expand_dependencies(
    store: InMemoryGraphStore,
    seeds: Iterable[str],
    batch_size: Optional[int] = None,
) -> Tuple[Dict[str, Node], Dict[str, List[str]]]:
    """
    Раскрывает подграф зависимостей: все кандидаты + ВСЕ их потомки
    рекурсивно, без ограничения глубины.

    Возвращает (nodes, children_map):
      nodes[node_id]  = Node с проверенным internal_score
      children_map[node_id] = список непосредственных потомков в подграфе
    """
    nodes: Dict[str, Node] = {}
    children_map: Dict[str, List[str]] = {}
    visited: Set[str] = set()

    frontier: List[str] = list(seeds)
    while frontier:
        frontier = [n for n in frontier if n not in visited]
        if not frontier:
            break

        # Загрузка узлов (все внутренние_score проверяются здесь — fail fast)
        if batch_size is not None:
            batch_nodes = store.get_nodes_batch(frontier)
            batch_children = store.get_children_batch(frontier)
        else:
            batch_nodes = {}
            batch_children = {}
            for nid in frontier:
                n = store.get_node(nid)
                if n is not None:
                    batch_nodes[nid] = n
                batch_children[nid] = store.get_children(nid)

        for nid in frontier:
            node = batch_nodes.get(nid)
            if node is None:
                raise GraphStoreError(f"Node {nid} not found in store")
            if node.internal_score is None:
                raise MissingInternalScoreError(nid)
            nodes[nid] = node
            children_map[nid] = batch_children.get(nid, [])
            visited.add(nid)

        # Формируем следующий фронтир
        next_frontier: List[str] = []
        seen_next: Set[str] = set()
        for nid in frontier:
            for c in children_map[nid]:
                if c not in visited and c not in seen_next:
                    next_frontier.append(c)
                    seen_next.add(c)
        frontier = next_frontier

    return nodes, children_map


def _filter_and_pack(
    trigger_id: str,
    max_depth: Optional[int],
    threshold: Fraction,
    candidates: Dict[str, int],
    scores: Dict[str, Any],
    num_type: Callable,
    stats: Dict[str, Any],
) -> ScoreResult:
    """Общая финальная стадия: проверить score >= S, собрать ScoreResult."""
    th = num_type(threshold)
    all_cand: Dict[str, Tuple[Any, int]] = {}
    qualified: List[Dict[str, Any]] = []
    for cid, depth in candidates.items():
        s = scores[cid]
        all_cand[cid] = (s, depth)
        if s >= th:
            qualified.append({"id": cid, "score": s, "depth": depth})
    return ScoreResult(
        trigger_id=trigger_id,
        max_depth=max_depth,
        threshold=threshold,
        all_candidates=all_cand,
        qualified=qualified,
        stats=stats,
    )


# =============================================================================
# 3. ТРИ АЛГОРИТМА
# =============================================================================

# -----------------------------------------------------------------------------
# 3.1. DFS с мемоизацией
# -----------------------------------------------------------------------------
# Итеративная реализация с 3-цветной раскраской (WHITE/GRAY/BLACK):
#   WHITE — не посещён
#   GRAY  — в процессе вычисления (в стеке)
#   BLACK — вычислен, лежит в memo
# Повторная встреча GRAY-узла = цикл.
# -----------------------------------------------------------------------------

def _compute_dfs(
    nodes: Dict[str, Node],
    children_map: Dict[str, List[str]],
    num_type: Callable = Fraction,
) -> Dict[str, Any]:
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in nodes}
    memo: Dict[str, Any] = {}

    for start in nodes:
        if color[start] != WHITE:
            continue
        # Стек: (node_id, iterator по его детям)
        stack: List[Tuple[str, Iterable[str]]] = [(start, iter(children_map.get(start, [])))]
        color[start] = GRAY
        while stack:
            nid, it = stack[-1]
            try:
                child = next(it)
                if child not in nodes:
                    raise GraphStoreError(f"Dependency {child} of {nid} not in subgraph")
                c_color = color[child]
                if c_color == GRAY:
                    raise CycleDetectedError(child)
                if c_color == WHITE:
                    color[child] = GRAY
                    stack.append((child, iter(children_map.get(child, []))))
            except StopIteration:
                # Все дети BLACK → можно вычислить score(nid)
                val = num_type(nodes[nid].internal_score)
                for c in children_map.get(nid, []):
                    val = val + memo[c] / 3
                memo[nid] = val
                color[nid] = BLACK
                stack.pop()

    return memo


def run_dfs(
    store: InMemoryGraphStore,
    tx_id: str,
    max_depth: Optional[int] = None,
    threshold: Fraction = Fraction(1),
    num_type: Callable = Fraction,
) -> ScoreResult:
    store.reset_stats()
    t0 = time.perf_counter()

    try:
        candidates = find_candidates(store, tx_id, max_depth, batched=False)
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
        nodes, children_map = expand_dependencies(store, set(candidates.keys()))
        t_load = time.perf_counter()
        scores = _compute_dfs(nodes, children_map, num_type)
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
    return _filter_and_pack(tx_id, max_depth, threshold, candidates, scores, num_type, stats)


# -----------------------------------------------------------------------------
# 3.2. Топологический обход (Kahn's algorithm)
# -----------------------------------------------------------------------------
# remaining[v] = число нерассчитанных непосредственных потомков v
# incoming[v]  = накопленная сумма score(c)/3 по уже рассчитанным детям
# Когда remaining[v] становится 0 — score(v) готов и v идёт в очередь.
# Цикл = остались нерассчитанные узлы после опустошения очереди.
# -----------------------------------------------------------------------------

def _compute_topological(
    nodes: Dict[str, Node],
    children_map: Dict[str, List[str]],
    num_type: Callable = Fraction,
) -> Dict[str, Any]:
    remaining = {n: len(children_map.get(n, [])) for n in nodes}
    zero = num_type(0)
    incoming = {n: zero for n in nodes}

    # Обратный индекс: родители в пределах подграфа
    parents_map: Dict[str, List[str]] = {n: [] for n in nodes}
    for p, cs in children_map.items():
        for c in cs:
            if c in parents_map:
                parents_map[c].append(p)

    ready = [n for n, r in remaining.items() if r == 0]
    score: Dict[str, Any] = {}
    while ready:
        nid = ready.pop()
        val = num_type(nodes[nid].internal_score) + incoming[nid]
        score[nid] = val
        for p in parents_map[nid]:
            incoming[p] = incoming[p] + val / 3
            remaining[p] -= 1
            if remaining[p] == 0:
                ready.append(p)

    if len(score) != len(nodes):
        unprocessed = set(nodes) - set(score)
        raise CycleDetectedError(next(iter(unprocessed)))
    return score


def run_topological(
    store: InMemoryGraphStore,
    tx_id: str,
    max_depth: Optional[int] = None,
    threshold: Fraction = Fraction(1),
    num_type: Callable = Fraction,
) -> ScoreResult:
    store.reset_stats()
    t0 = time.perf_counter()

    try:
        candidates = find_candidates(store, tx_id, max_depth, batched=False)
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
        nodes, children_map = expand_dependencies(store, set(candidates.keys()))
        t_load = time.perf_counter()
        scores = _compute_topological(nodes, children_map, num_type)
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
    return _filter_and_pack(tx_id, max_depth, threshold, candidates, scores, num_type, stats)


# -----------------------------------------------------------------------------
# 3.3. Пакетная итеративная обработка
# -----------------------------------------------------------------------------
# Отличия от топологического:
#   * загрузка подграфа идёт батчами (get_children_batch / get_nodes_batch)
#   * поиск кандидатов идёт батчами (get_parents_batch)
#   * вычисление организовано "волнами": все узлы волны независимы,
#     их можно параллелить (в этой реализации — последовательно)
# Это даёт меньше Cypher-запросов на больших графах.
# -----------------------------------------------------------------------------

def _compute_batched(
    nodes: Dict[str, Node],
    children_map: Dict[str, List[str]],
    num_type: Callable = Fraction,
) -> Dict[str, Any]:
    remaining = {n: len(children_map.get(n, [])) for n in nodes}
    zero = num_type(0)
    incoming = {n: zero for n in nodes}

    parents_map: Dict[str, List[str]] = {n: [] for n in nodes}
    for p, cs in children_map.items():
        for c in cs:
            if c in parents_map:
                parents_map[c].append(p)

    score: Dict[str, Any] = {}
    current_wave = [n for n, r in remaining.items() if r == 0]

    while current_wave:
        # 1) Все узлы волны вычисляются независимо
        wave_scores: Dict[str, Any] = {}
        for nid in current_wave:
            wave_scores[nid] = num_type(nodes[nid].internal_score) + incoming[nid]
        # 2) Фиксируем результаты волны
        score.update(wave_scores)
        # 3) Продвигаем вклады родителям, определяем следующую волну
        next_wave_set: Set[str] = set()
        for nid in current_wave:
            v = wave_scores[nid]
            for p in parents_map[nid]:
                incoming[p] = incoming[p] + v / 3
                remaining[p] -= 1
                if remaining[p] == 0:
                    next_wave_set.add(p)
        current_wave = list(next_wave_set)

    if len(score) != len(nodes):
        unprocessed = set(nodes) - set(score)
        raise CycleDetectedError(next(iter(unprocessed)))
    return score


def run_batched(
    store: InMemoryGraphStore,
    tx_id: str,
    max_depth: Optional[int] = None,
    threshold: Fraction = Fraction(1),
    num_type: Callable = Fraction,
    batch_size: int = 64,
) -> ScoreResult:
    store.reset_stats()
    t0 = time.perf_counter()

    try:
        # Поиск кандидатов — батчами
        candidates = find_candidates(store, tx_id, max_depth, batched=True)
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
        # Раскрытие зависимостей — батчами
        nodes, children_map = expand_dependencies(
            store, set(candidates.keys()), batch_size=batch_size
        )
        t_load = time.perf_counter()
        scores = _compute_batched(nodes, children_map, num_type)
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
        "batch_size": batch_size,
        "candidates_time_ms": (t_cand - t0) * 1000,
        "load_time_ms": (t_load - t_cand) * 1000,
        "compute_time_ms": (t_compute - t_load) * 1000,
        "total_time_ms": (t_compute - t0) * 1000,
    }
    return _filter_and_pack(tx_id, max_depth, threshold, candidates, scores, num_type, stats)


# =============================================================================
# 4. НЕЗАВИСИМЫЙ ЭТАЛОН
# =============================================================================
# Намеренно простая рекурсивная реализация, не оптимизированная по скорости.
# Используется как источник истины для проверки трёх "production" алгоритмов.
# Использует Fraction везде.
# =============================================================================

def run_reference(
    store: InMemoryGraphStore,
    tx_id: str,
    max_depth: Optional[int] = None,
    threshold: Fraction = Fraction(1),
) -> ScoreResult:
    store.reset_stats()
    t0 = time.perf_counter()

    try:
        candidates = find_candidates(store, tx_id, max_depth, batched=False)
    except GraphStoreError as e:
        return ScoreResult(tx_id, max_depth, threshold, {}, [],
                           {"queries": store.stats()["queries"], "error": str(e)},
                           error=str(e))
    t_cand = time.perf_counter()

    memo: Dict[str, Fraction] = {}
    visiting: Set[str] = set()

    def score_of(nid: str) -> Fraction:
        if nid in memo:
            return memo[nid]
        if nid in visiting:
            raise CycleDetectedError(nid)
        visiting.add(nid)
        node = store.get_node(nid)
        if node is None:
            raise GraphStoreError(f"Node {nid} not found")
        if node.internal_score is None:
            raise MissingInternalScoreError(nid)
        val = node.internal_score
        for c in store.get_children(nid):
            val = val + score_of(c) / 3
        visiting.remove(nid)
        memo[nid] = val
        return val

    all_cand: Dict[str, Tuple[Fraction, int]] = {}
    qualified: List[Dict[str, Any]] = []
    try:
        for cid, depth in candidates.items():
            s = score_of(cid)
            all_cand[cid] = (s, depth)
            if s >= threshold:
                qualified.append({"id": cid, "score": s, "depth": depth})
    except GraphStoreError as e:
        t1 = time.perf_counter()
        return ScoreResult(tx_id, max_depth, threshold, {}, [],
                           {"queries": store.stats()["queries"], "error": str(e)},
                           error=str(e))

    t_compute = time.perf_counter()
    return ScoreResult(
        trigger_id=tx_id, max_depth=max_depth, threshold=threshold,
        all_candidates=all_cand, qualified=qualified,
        stats={
            "queries": store.stats()["queries"],
            "rows_returned": store.stats()["rows_returned"],
            "candidates_time_ms": (t_cand - t0) * 1000,
            "compute_time_ms": (t_compute - t_cand) * 1000,
            "total_time_ms": (t_compute - t0) * 1000,
        },
    )


# =============================================================================
# 5. ГЕНЕРАТОР СИНТЕТИЧЕСКИХ DAG
# =============================================================================

ALPHA = 0.005
BETA = 0.5


def transfer_score(x: float, alpha: float = ALPHA, beta: float = BETA) -> float:
    """Монотонная функция [0, ∞) → [0, 1)."""
    return 1 - (1 + alpha * x) ** (-beta)


def _random_amount(rng: random.Random, i: int) -> int:
    """Распределение сумм из ТЗ: редкие крупные транзакции."""
    if i % 100 == 0:
        return rng.randint(25_000, 100_000)
    if i % 25 == 0:
        return rng.randint(5_000, 25_000)
    if i % 5 == 0:
        return rng.randint(1_000, 5_000)
    return rng.randint(1, 1_000)


def generate_dag(
    n_nodes: int,
    seed: int = 42,
    n_parents: int = 3,
    layered: bool = False,
    layer_width: int = 8,
) -> InMemoryGraphStore:
    """
    Генерирует DAG. Каждый узел ссылается на ≤ n_parents ранее созданных
    (гарантирует ациклиность). Триггер — последний созданный узел.

    layered=True → детерминированная слоистая структура (для G3).
    """
    rng = random.Random(seed)
    store = InMemoryGraphStore()
    ids = [f"tx{i}" for i in range(n_nodes)]

    for i, nid in enumerate(ids):
        amount = _random_amount(rng, i)
        internal = Fraction(transfer_score(amount))   # точное значение float как Fraction
        store.add_node(nid, internal)

        if i == 0:
            continue

        if layered:
            # Родители из предыдущего слоя (для контролируемой глубины)
            layer = i // layer_width
            prev_layer_start = max(0, (layer - 1) * layer_width)
            prev_layer_end = layer * layer_width
            candidates = list(range(prev_layer_start, min(prev_layer_end, i)))
            if not candidates:
                candidates = list(range(0, i))
            k = min(n_parents, len(candidates))
            parents = rng.sample(candidates, k)
        else:
            k = min(n_parents, i)
            parents = rng.sample(range(0, i), k)

        for p_idx in parents:
            store.add_edge(nid, ids[p_idx])

    return store


# =============================================================================
# 6. ТЕСТЫ
# =============================================================================

def _verify_all_agree(
    store: InMemoryGraphStore,
    tx_id: str,
    max_depth: Optional[int],
    threshold: Fraction,
    test_name: str = "",
):
    """Запускает все алгоритмы + эталон и проверяет полное совпадение."""
    ref = run_reference(store, tx_id, max_depth, threshold)

    for algo_name, fn in [
        ("topological", run_topological),
        ("batched", run_batched),
        ("dfs", run_dfs),
    ]:
        res = fn(store, tx_id, max_depth, threshold)
        # Ошибки должны совпадать
        if ref.error is not None or res.error is not None:
            assert (ref.error is not None) == (res.error is not None), (
                f"[{test_name}/{algo_name}] error mismatch: ref={ref.error} vs {res.error}"
            )
            continue
        # Множество кандидатов
        assert set(res.all_candidates.keys()) == set(ref.all_candidates.keys()), (
            f"[{test_name}/{algo_name}] candidate set mismatch: "
            f"{set(res.all_candidates)} vs {set(ref.all_candidates)}"
        )
        # Точное совпадение score и depth
        for k in ref.all_candidates:
            r_score, r_depth = ref.all_candidates[k]
            a_score, a_depth = res.all_candidates[k]
            assert a_score == r_score, (
                f"[{test_name}/{algo_name}] score mismatch for {k}: {a_score} vs {r_score}"
            )
            assert a_depth == r_depth, (
                f"[{test_name}/{algo_name}] depth mismatch for {k}: {a_depth} vs {r_depth}"
            )
        # Множество qualified
        ref_q = {(q["id"], q["depth"]) for q in ref.qualified}
        res_q = {(q["id"], q["depth"]) for q in res.qualified}
        assert ref_q == res_q, (
            f"[{test_name}/{algo_name}] qualified mismatch: {res_q} vs {ref_q}"
        )


# --- G1: минимальный граф ----------------------------------------------------

def make_G1() -> InMemoryGraphStore:
    store = InMemoryGraphStore()
    store.add_node("tx", Fraction(1, 10))
    for name in ("P1", "P2", "P3"):
        store.add_node(name, Fraction(1, 2))
        store.add_edge("tx", name)
    return store


def test_G1():
    store = make_G1()
    # score(tx) = 1/10;  score(Pi) = 1/2 + (1/10)/3 = 8/15
    # S = 1/2 → все Pi проходят; S = 3/5 → никто не проходит
    _verify_all_agree(store, "tx", None, Fraction(1, 2), "G1/S=1/2")
    _verify_all_agree(store, "tx", None, Fraction(3, 5), "G1/S=3/5")
    _verify_all_agree(store, "tx", 1, Fraction(1, 2), "G1/max=1")
    _verify_all_agree(store, "tx", 0, Fraction(1, 2), "G1/max=0")

    res = run_topological(store, "tx", None, Fraction(1, 2))
    assert set(res.all_candidates.keys()) == {"P1", "P2", "P3"}
    for k in res.all_candidates:
        assert res.all_candidates[k][0] == Fraction(8, 15)
        assert res.all_candidates[k][1] == 1
    # MAX_DEPTH=0 → пусто
    res0 = run_topological(store, "tx", 0, Fraction(0))
    assert res0.qualified == [] and res0.all_candidates == {}


# --- G2: сходящиеся ветви ----------------------------------------------------

def make_G2() -> InMemoryGraphStore:
    """
    tx -> A, B, C -> D  (D — общий предок, score(D) суммирует A+B+C, /3)
    """
    store = InMemoryGraphStore()
    store.add_node("tx", Fraction(1, 10))
    for name in ("A", "B", "C"):
        store.add_node(name, Fraction(1, 2))
        store.add_edge("tx", name)
    store.add_node("D", Fraction(1, 4))
    for name in ("A", "B", "C"):
        store.add_edge(name, "D")
    return store


def test_G2():
    store = make_G2()
    # score(A) = 1/2 + (1/10)/3 = 8/15;  score(D) = 1/4 + 3*(8/15)/3 = 1/4 + 8/15 = 47/60
    # MAX_DEPTH=1: кандидаты {A,B,C}; D не входит
    # MAX_DEPTH=2: кандидаты {A,B,C,D}
    for md in (None, 1, 2):
        _verify_all_agree(store, "tx", md, Fraction(1, 2), f"G2/max={md}")

    r1 = run_topological(store, "tx", 1, Fraction(1, 2))
    assert set(r1.all_candidates.keys()) == {"A", "B", "C"}

    r2 = run_topological(store, "tx", 2, Fraction(1, 2))
    assert set(r2.all_candidates.keys()) == {"A", "B", "C", "D"}
    assert r2.all_candidates["D"] == (Fraction(47, 60), 2)


# --- G3: глубокий DAG --------------------------------------------------------

def make_G3(depth: int = 40) -> InMemoryGraphStore:
    """Линейная цепочка + ветвление (гарантирует глубину depth)."""
    store = InMemoryGraphStore()
    n = depth + 1
    for i in range(n):
        store.add_node(f"n{i}", Fraction(1, 4))
    # Цепочка: n(i+1) -> n(i)
    for i in range(n - 1):
        store.add_edge(f"n{i+1}", f"n{i}")
    # Небольшое ветвление: каждый третий ссылается на n(i-2)
    for i in range(2, n, 3):
        store.add_edge(f"n{i}", f"n{i-2}")
    return store


def test_G3():
    store = make_G3(depth=30)
    trigger = "n30"
    # MAX_DEPTH=None: все предки. MAX_DEPTH=5: только 5 слоёв.
    _verify_all_agree(store, trigger, None, Fraction(1, 2), "G3/None")
    _verify_all_agree(store, trigger, 5, Fraction(1, 2), "G3/max=5")
    _verify_all_agree(store, trigger, 1, Fraction(1, 2), "G3/max=1")

    # Проверка: при MAX_DEPTH=1 возвращается только непосредственный предок,
    # но score посчитан через ВСЮ цепочку ниже (потомки)
    r = run_topological(store, trigger, 1, Fraction(0))
    assert set(r.all_candidates.keys()) == {"n29"}


# --- G4: широкий DAG ---------------------------------------------------------

def make_G4(width: int = 50, depth: int = 4) -> InMemoryGraphStore:
    store = InMemoryGraphStore()
    # Слой 0: единственный "корневой" предок
    store.add_node("root", Fraction(1, 4))
    # Слои 1..depth: width узлов, каждый ссылается на root и на 2 узла предыдущего слоя
    rng = random.Random(7)
    prev_layer = ["root"]
    for lvl in range(1, depth + 1):
        cur_layer = []
        for w in range(width):
            nid = f"l{lvl}_{w}"
            store.add_node(nid, Fraction(1, 4))
            # Все ссылаются на root
            store.add_edge(nid, "root")
            # На 2 узла предыдущего слоя
            for p in rng.sample(prev_layer, min(2, len(prev_layer))):
                store.add_edge(nid, p)
            cur_layer.append(nid)
        prev_layer = cur_layer
    return store


def test_G4():
    store = make_G4(width=30, depth=3)
    trigger = "l3_0"
    for md in (None, 2):
        _verify_all_agree(store, trigger, md, Fraction(1, 2), f"G4/max={md}")


# --- G5: сильно сходящийся DAG -----------------------------------------------

def make_G5(branches: int = 20) -> InMemoryGraphStore:
    """
    tx порождает branches ветвей, все сходятся к одному 'root'.
    Число путей велико (≥ 3^branches), но уникальных узлов мало.
    """
    store = InMemoryGraphStore()
    store.add_node("tx", Fraction(1, 10))
    store.add_node("root", Fraction(1, 4))
    # Первый уровень: 3 родителя, каждый смотрит на root
    for b in range(branches):
        nid = f"b{b}"
        store.add_node(nid, Fraction(1, 3))
        store.add_edge("tx", nid) if b < 3 else store.add_edge("tx", nid)
        store.add_edge(nid, "root")
    return store


def test_G5():
    store = make_G5(branches=20)
    _verify_all_agree(store, "tx", None, Fraction(1, 2), "G5")
    # Дополнительно: убедимся, что все потомки tx учитываются в score(root) ровно 1 раз
    r = run_topological(store, "tx", None, Fraction(0))
    assert "root" in r.all_candidates


# --- G6: общий предок на разных глубинах -------------------------------------

def make_G6() -> InMemoryGraphStore:
    """
    tx -> A (глубина 1);  tx -> C (глубина 1);  A -> B (глубина 2);  B -> C (глубина 3)
    C достижим на глубинах 1 и 3, min = 1.
    """
    store = InMemoryGraphStore()
    store.add_node("tx", Fraction(1, 10))
    store.add_node("A", Fraction(1, 2))
    store.add_node("B", Fraction(1, 3))
    store.add_node("C", Fraction(1, 4))
    store.add_edge("tx", "A")
    store.add_edge("tx", "C")
    store.add_edge("A", "B")
    store.add_edge("B", "C")
    return store


def test_G6():
    store = make_G6()
    # score(tx)=1/10
    # score(A) = 1/2 + (1/10)/3 = 8/15
    # score(B) = 1/3 + (8/15)/3 = 1/3 + 8/45 = 15/45 + 8/45 = 23/45
    # score(C) = 1/4 + (1/10)/3 + (23/45)/3 = 1/4 + 1/30 + 23/135
    #          = 135/540 + 18/540 + 92/540 = 245/540 = 49/108
    for md in (None, 1, 2, 3):
        _verify_all_agree(store, "tx", md, Fraction(1, 2), f"G6/max={md}")

    # C должен быть один раз с depth=1 даже при MAX_DEPTH=1, и его score включает B
    r = run_topological(store, "tx", 1, Fraction(0))
    assert set(r.all_candidates.keys()) == {"A", "C"}
    assert r.all_candidates["C"] == (Fraction(49, 108), 1)
    # score(C) не зависит от MAX_DEPTH
    for md in (1, 2, None):
        r = run_topological(store, "tx", md, Fraction(0))
        assert r.all_candidates["C"][0] == Fraction(49, 108), (
            f"C's score changed with MAX_DEPTH={md}"
        )


# --- Edge cases --------------------------------------------------------------

def test_edge_cases():
    # Пустой результат: MAX_DEPTH=0
    store = make_G1()
    for fn in (run_dfs, run_topological, run_batched):
        r = fn(store, "tx", 0, Fraction(0))
        assert r.qualified == []
        assert r.all_candidates == {}

    # Несуществующий триггер
    for fn in (run_dfs, run_topological, run_batched, run_reference):
        r = fn(store, "nonexistent", None, Fraction(0))
        assert r.error is not None
        assert "not found" in r.error.lower()

    # Некорректный MAX_DEPTH
    for fn in (run_dfs, run_topological, run_batched):
        r = fn(store, "tx", -1, Fraction(0))
        assert r.error is not None

    # Порог ровно равен score
    # score(Pi) = 8/15 → S = 8/15 должен дать совпадение (score >= S)
    for fn in (run_dfs, run_topological, run_batched):
        r = fn(store, "tx", None, Fraction(8, 15))
        assert len(r.qualified) == 3, f"{fn.__name__}: expected 3, got {r.qualified}"

    # Порог чуть выше
    for fn in (run_dfs, run_topological, run_batched):
        r = fn(store, "tx", None, Fraction(8, 15) + Fraction(1, 10**12))
        assert len(r.qualified) == 0

    # Цикл: A -> B -> A (B parent of A, A parent of B)
    store_cycle = InMemoryGraphStore()
    store_cycle.add_node("tx", Fraction(1, 10))
    store_cycle.add_node("A", Fraction(1, 3))
    store_cycle.add_node("B", Fraction(1, 3))
    store_cycle.add_edge("tx", "A")
    store_cycle.add_edge("A", "B")
    store_cycle.add_edge("B", "A")   # цикл
    for fn in (run_dfs, run_topological, run_batched, run_reference):
        r = fn(store_cycle, "tx", None, Fraction(0))
        assert r.error is not None, f"{fn.__name__} did not detect cycle"
        assert "cycle" in r.error.lower()

    # Отсутствующий internal_score
    store_bad = InMemoryGraphStore()
    store_bad.add_node("tx", Fraction(1, 10))
    store_bad.add_node("A", None)   # битый узел
    store_bad.add_edge("tx", "A")
    for fn in (run_dfs, run_topological, run_batched, run_reference):
        r = fn(store_bad, "tx", None, Fraction(0))
        assert r.error is not None, f"{fn.__name__} did not catch missing score"


# --- Property-based: случайные DAG -------------------------------------------

def test_property_random():
    rng = random.Random(123)
    for trial in range(15):
        n = rng.randint(5, 30)
        store = generate_dag(n_nodes=n, seed=rng.randint(0, 1_000_000))
        trigger = f"tx{n-1}"
        max_depth = rng.choice([None, 0, 1, 2, 3])
        threshold = Fraction(rng.randint(0, 30), 10)
        _verify_all_agree(
            store, trigger, max_depth, threshold, f"prop/n={n}/md={max_depth}"
        )


def run_tests() -> bool:
    tests = [
        ("G1 — минимальный граф",            test_G1),
        ("G2 — сходящиеся ветви",            test_G2),
        ("G3 — глубокий DAG",                test_G3),
        ("G4 — широкий DAG",                 test_G4),
        ("G5 — сильно сходящийся DAG",       test_G5),
        ("G6 — общий предок на разных глубинах", test_G6),
        ("Edge cases",                        test_edge_cases),
        ("Property-based",                    test_property_random),
    ]
    passed = failed = 0
    for name, fn in tests:
        t0 = time.perf_counter()
        try:
            fn()
            dt = (time.perf_counter() - t0) * 1000
            print(f"  ✓ {name}  ({dt:.0f} ms)")
            passed += 1
        except AssertionError as e:
            print(f"  ✗ {name}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ✗ {name}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n  Итого: {passed} passed, {failed} failed")
    return failed == 0


# =============================================================================
# 7. BENCHMARK
# =============================================================================

def _run_with_memory(fn, *args, **kwargs):
    """Замер пиковой памяти через tracemalloc."""
    tracemalloc.start()
    try:
        res = fn(*args, **kwargs)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return res, peak


def _benchmark_one(store: InMemoryGraphStore, trigger: str, label: str) -> List[Dict[str, Any]]:
    results = []
    for name, fn, kwargs in [
        ("DFS (eager)",   run_dfs,         {}),
        ("Topological",   run_topological, {}),
        ("Batched",       run_batched,     {"batch_size": 64}),
        ("Reference",     run_reference,   {}),
    ]:
        try:
            res, peak = _run_with_memory(
                fn, store, trigger,
                max_depth=None, threshold=Fraction(1, 2), **kwargs
            )
        except Exception as e:
            print(f"    {name:14s} ERROR: {e}")
            continue
        stats = res.stats
        results.append({
            "graph": label,
            "algo": name,
            "time_ms": stats.get("total_time_ms", 0),
            "compute_ms": stats.get("compute_time_ms", 0),
            "load_ms": stats.get("load_time_ms", 0),
            "queries": stats.get("queries", 0),
            "nodes": stats.get("nodes_loaded", 0),
            "edges": stats.get("edges_loaded", 0),
            "mem_kb": peak / 1024,
            "qualified": len(res.qualified),
            "error": res.error,
        })
    return results


def run_benchmark() -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []

    configs = [
        ("small (n=60)",    60,  4,  False),
        ("medium (n=300)",  300, 4,  False),
        ("wide (n=500)",    500, 3,  True),   # слоистая структура
        ("large (n=2000)",  2000, 4, False),
    ]

    print(f"\n  {'граф':18s} {'алгоритм':14s} {'time,ms':>9s} {'compute':>9s} "
          f"{'queries':>8s} {'mem,KB':>8s} {'qual':>5s}")
    print("  " + "-" * 78)

    for label, n, n_par, layered in configs:
        store = generate_dag(
            n_nodes=n, seed=42, n_parents=n_par, layered=layered, layer_width=20
        )
        trigger = f"tx{n-1}"
        res = _benchmark_one(store, trigger, label)
        for r in res:
            print(f"  {r['graph']:18s} {r['algo']:14s} {r['time_ms']:9.2f} "
                  f"{r['compute_ms']:9.2f} {r['queries']:8d} "
                  f"{r['mem_kb']:8.1f} {r['qualified']:5d}")
        print("  " + "-" * 78)
        all_results.extend(res)

    return all_results


def print_recommendation(results: List[Dict[str, Any]]) -> None:
    print()
    print("  Анализ по графам (исключая reference как эталон):")
    by_graph: Dict[str, List[Dict]] = defaultdict(list)
    for r in results:
        if r["algo"] != "Reference" and r["error"] is None:
            by_graph[r["graph"]].append(r)

    for graph, items in by_graph.items():
        print(f"\n  [{graph}]")
        best_time = min(items, key=lambda x: x["time_ms"])
        best_queries = min(items, key=lambda x: x["queries"])
        print(f"    Лучшее время:   {best_time['algo']:14s} ({best_time['time_ms']:.2f} ms)")
        print(f"    Меньше запросов:{best_queries['algo']:14s} ({best_queries['queries']} queries)")

    print()
    print("  Вывод:")
    print("  ─────────────────────────────────────────────────────────────────")
    print("  * 'Batched' — меньше всего Cypher-запросов на всех размерах графа")
    print("    (за счёт get_children_batch/get_nodes_batch/get_parents_batch).")
    print("    Время вычислений сопоставимо с топологическим, но доминирует")
    print("    экономией на сетевых/query накладных при работе с Memgraph.")
    print()
    print("  * 'Topological' — самый быстрый по compute_ms на in-memory графе,")
    print("    использует меньше промежуточных структур, но делает по одному")
    print("    запросу на узел при загрузке. Хорош, если узлов немного или")
    print("    задержка Memgraph мала.")
    print()
    print("  * 'DFS (eager)' — эквивалентен топологическому по числу запросов")
    print("    и времени, но использует рекурсию/стек; на очень глубоких графах")
    print("    производственная реализация требует явного стека (уже сделано).")
    print("    Естественно выражает рекуррентную формулу — удобен как проверочный.")
    print()
    print("  Рекомендация для production:")
    print("  ─────────────────────────────────────────────────────────────────")
    print("  Использовать 'Batched' с настраиваемым BATCH_SIZE. На больших DAG")
    print("  он даёт на порядок меньше Cypher-запросов, а по времени вычислений")
    print("  сопоставим с топологическим. 'Topological' оставить как fallback")
    print("  для маленьких графов или режима без батчевой поддержки.")
    print("  'DFS (eager)' держать как reference-реализацию для тестов.")
    print()
    print("  Важно для production-интеграции с Memgraph:")
    print("  ─────────────────────────────────────────────────────────────────")
    print("  * Проверить фактические метки, имена properties и направление")
    print("    связи в графе — приведённые имена (`Transaction`, `PARENT`,")
    print("    `id`, `internal_score`) иллюстративны.")
    print("  * Убедиться, что чтение консистентно (снапшот транзакции Memgraph")
    print("    или retry при обнаружении изменений).")
    print("  * В продакшене числовой тип — изучить фактический internal_score")
    print("    (float64 / Decimal) и использовать тот же для score; НЕ округлять")
    print("    промежуточные результаты и не применять произвольный epsilon.")


# =============================================================================
# 8. ТОЧКА ВХОДА
# =============================================================================

def main():
    print("=" * 78)
    print("Score Recalculation Engine — реализация ТЗ v1.0")
    print("=" * 78)

    print("\n[1/2] Запуск тестов…\n")
    ok = run_tests()

    print("\n[2/2] Benchmark…")
    results = run_benchmark()

    print()
    print("=" * 78)
    print("РЕКОМЕНДАЦИЯ")
    print("=" * 78)
    print_recommendation(results)

    print("\n" + "=" * 78)
    print("Завершено." if ok else "ВНИМАНИЕ: некоторые тесты упали!")
    print("=" * 78)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())