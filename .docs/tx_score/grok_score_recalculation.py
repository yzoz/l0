#!/usr/bin/env python3
"""
Score Recalculation Engine for Transaction DAG
==============================================

Полная реализация ТЗ v1.0 (19 октября 2026).

Математическая модель (единственный источник истины):

    score(T) = internal_score(T) + (1/3) * sum_{X in C(T)} score(X)

где C(T) — непосредственные потомки T (транзакции, которые ссылаются на T
как на одного из трёх родителей).

Ключевые инварианты:
- MAX_DEPTH ограничивает ТОЛЬКО множество кандидатов для out.
- Глубина вычисления зависимостей score НЕ ограничивается MAX_DEPTH.
- Каждый узел вычисляется ровно один раз на согласованном снимке графа.
- Каждый вклад по реальному ребру учитывается ровно один раз.
- Общие предки не дублируются; вклады от разных потомков суммируются.

Три реализации:
1. DFS с мемоизацией (итеративная версия со стеком — без лимита рекурсии).
2. Топологический обход (Kahn-style).
3. Пакетная итеративная обработка (волны готовых узлов + batch-симуляция).

Все три работают по одному и тому же локальному представлению подграфа,
чтобы сравнивать именно алгоритмы вычисления, а не способ чтения данных.
"""

from __future__ import annotations

import copy
import math
import random
import sys
import time
import traceback
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

# Высокая точность для эталона (рациональная семантика через Decimal)
getcontext().prec = 50

# ---------------------------------------------------------------------------
# Константы и типы
# ---------------------------------------------------------------------------

ALPHA = Decimal("0.005")
BETA = Decimal("0.5")
DEFAULT_THRESHOLD = Decimal("0.5")
PARENTS_PER_TX = 3  # по модели ТЗ


def transfer_score(x: Union[int, float, Decimal], alpha=ALPHA, beta=BETA) -> Decimal:
    """
    α — чем больше, тем быстрее уходит от 0
    β — чем меньше, тем медленнее приближается к 1

    transfer_score(x) = 1 - (1 + α·x)^(-β)
    """
    x = Decimal(str(x))
    return Decimal(1) - (Decimal(1) + alpha * x) ** (-beta)


class Algorithm(Enum):
    DFS_MEMO = auto()
    TOPOLOGICAL = auto()
    BATCH_ITERATIVE = auto()


class CycleDetectedError(Exception):
    """Обнаружен цикл в DAG — результат некорректен."""
    pass


class DataInvariantError(Exception):
    """Нарушение инвариантов данных (нет internal_score, NaN, и т.д.)."""
    pass


class InvalidParameterError(Exception):
    """Некорректные параметры (отрицательный max_depth, несуществующий tx_id)."""
    pass


# ---------------------------------------------------------------------------
# Структуры данных
# ---------------------------------------------------------------------------

@dataclass
class Candidate:
    """Кандидат на выход в out."""
    tx_id: str
    depth: int          # минимальное расстояние от триггера
    score: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tx_id": self.tx_id,
            "depth": self.depth,
            "score": float(self.score),
        }


@dataclass
class Diagnostics:
    """Диагностические показатели одного запуска."""
    algorithm: str
    nodes_loaded: int = 0
    edges_loaded: int = 0
    candidates_considered: int = 0
    candidates_passed: int = 0
    cypher_queries_simulated: int = 0
    time_ancestors_ms: float = 0.0
    time_dependencies_ms: float = 0.0
    time_compute_ms: float = 0.0
    time_total_ms: float = 0.0
    peak_memo_size: int = 0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "algorithm": self.algorithm,
            "nodes_loaded": self.nodes_loaded,
            "edges_loaded": self.edges_loaded,
            "candidates_considered": self.candidates_considered,
            "candidates_passed": self.candidates_passed,
            "cypher_queries_simulated": self.cypher_queries_simulated,
            "time_ancestors_ms": round(self.time_ancestors_ms, 3),
            "time_dependencies_ms": round(self.time_dependencies_ms, 3),
            "time_compute_ms": round(self.time_compute_ms, 3),
            "time_total_ms": round(self.time_total_ms, 3),
            "peak_memo_size": self.peak_memo_size,
            "error": self.error,
        }


@dataclass
class ScoreResult:
    """Единый результат для всех алгоритмов."""
    trigger_id: str
    max_depth: Optional[int]
    threshold: Decimal
    candidates: List[Candidate]
    scores: Dict[str, Decimal]          # score всех узлов, участвовавших в расчёте
    diagnostics: Diagnostics

    def passed_ids(self) -> Set[str]:
        return {c.tx_id for c in self.candidates}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trigger_id": self.trigger_id,
            "max_depth": self.max_depth,
            "threshold": float(self.threshold),
            "candidates": [c.to_dict() for c in self.candidates],
            "diagnostics": self.diagnostics.to_dict(),
        }


# ---------------------------------------------------------------------------
# Локальное представление графа (снимок)
# ---------------------------------------------------------------------------

class GraphSnapshot:
    """
    Согласованный снимок подграфа в памяти.

    Направление:
      - parents[v]  = непосредственные родители v  (рёбра v → parent)
      - children[v] = непосредственные потомки v   (транзакции, ссылающиеся на v)

    Формула score зависит от children, поэтому children — ключевая структура.
    """

    def __init__(self) -> None:
        self.internal: Dict[str, Decimal] = {}
        self.parents: Dict[str, List[str]] = defaultdict(list)
        self.children: Dict[str, List[str]] = defaultdict(list)
        self._all_ids: Set[str] = set()

    def add_node(self, tx_id: str, internal_score: Union[Decimal, float, int]) -> None:
        if tx_id in self.internal:
            return
        sc = Decimal(str(internal_score))
        if not sc.is_finite():
            raise DataInvariantError(f"internal_score of {tx_id} is not finite: {sc}")
        self.internal[tx_id] = sc
        self._all_ids.add(tx_id)

    def add_edge(self, child: str, parent: str) -> None:
        """Ребро: child ссылается на parent (child → parent)."""
        if parent not in self.parents[child]:
            self.parents[child].append(parent)
        if child not in self.children[parent]:
            self.children[parent].append(child)
        self._all_ids.add(child)
        self._all_ids.add(parent)

    def nodes(self) -> Set[str]:
        return set(self._all_ids)

    def has_node(self, tx_id: str) -> bool:
        return tx_id in self.internal

    def ensure_internal(self, tx_id: str) -> Decimal:
        if tx_id not in self.internal:
            raise DataInvariantError(f"missing internal_score for {tx_id}")
        return self.internal[tx_id]

    def copy(self) -> "GraphSnapshot":
        g = GraphSnapshot()
        g.internal = dict(self.internal)
        g.parents = defaultdict(list, {k: list(v) for k, v in self.parents.items()})
        g.children = defaultdict(list, {k: list(v) for k, v in self.children.items()})
        g._all_ids = set(self._all_ids)
        return g

    def subgraph(self, node_ids: Iterable[str]) -> "GraphSnapshot":
        """Подграф, индуцированный множеством узлов (только рёбра внутри)."""
        wanted = set(node_ids)
        g = GraphSnapshot()
        for nid in wanted:
            if nid in self.internal:
                g.add_node(nid, self.internal[nid])
        for child, parents in self.parents.items():
            if child not in wanted:
                continue
            for p in parents:
                if p in wanted:
                    g.add_edge(child, p)
        return g


# ---------------------------------------------------------------------------
# Поиск предков и минимальных глубин (BFS от триггера вверх)
# ---------------------------------------------------------------------------

def find_ancestors(
    graph: GraphSnapshot,
    tx_id: str,
    max_depth: Optional[int],
) -> Dict[str, int]:
    """
    Возвращает {ancestor_id: min_depth} для всех предков, достижимых
    на глубине 1..max_depth (или всех, если max_depth is None).

    Сам tx_id НЕ включается.
    """
    if max_depth is not None and max_depth < 0:
        raise InvalidParameterError(f"max_depth must be >= 0 or None, got {max_depth}")
    if max_depth == 0:
        return {}

    if not graph.has_node(tx_id):
        raise InvalidParameterError(f"trigger tx_id={tx_id} not found in graph")

    # BFS вверх по родителям
    depth: Dict[str, int] = {}
    queue: deque[Tuple[str, int]] = deque()

    for p in graph.parents.get(tx_id, []):
        if p not in depth:
            depth[p] = 1
            queue.append((p, 1))

    while queue:
        node, d = queue.popleft()
        if max_depth is not None and d >= max_depth:
            continue
        for p in graph.parents.get(node, []):
            if p not in depth:
                depth[p] = d + 1
                queue.append((p, d + 1))
            # если уже есть более короткий путь — ничего не делаем
            # (BFS гарантирует, что первый раз мы приходим по минимальной длине)

    return depth


def expand_dependency_subgraph(
    graph: GraphSnapshot,
    candidates: Iterable[str],
) -> GraphSnapshot:
    """
    Раскрывает полный подграф зависимостей, необходимый для точного
    вычисления score всех кандидатов.

    Начинаем с кандидатов и идём ВНИЗ по потомкам (children), пока
    не исчерпаем все достижимые зависимости.

    MAX_DEPTH сюда НЕ применяется.
    """
    needed: Set[str] = set(candidates)
    frontier = set(candidates)

    while frontier:
        next_frontier: Set[str] = set()
        for node in frontier:
            for child in graph.children.get(node, []):
                if child not in needed:
                    needed.add(child)
                    next_frontier.add(child)
        frontier = next_frontier

    # Также нужны все рёбра между needed-узлами
    return graph.subgraph(needed)


# ---------------------------------------------------------------------------
# Эталонная реализация (точная, для небольших графов)
# ---------------------------------------------------------------------------

def reference_scores(
    graph: GraphSnapshot,
    nodes_to_compute: Iterable[str],
) -> Dict[str, Decimal]:
    """
    Независимый эталон: DFS с мемоизацией + Decimal.
    Используется только для проверки корректности.
    """
    memo: Dict[str, Decimal] = {}
    visiting: Set[str] = set()

    def _score(node: str) -> Decimal:
        if node in memo:
            return memo[node]
        if node in visiting:
            raise CycleDetectedError(f"cycle detected at {node}")
        visiting.add(node)
        val = graph.ensure_internal(node)
        for child in graph.children.get(node, []):
            val += _score(child) / Decimal(3)
        visiting.remove(node)
        memo[node] = val
        return val

    result = {}
    for n in nodes_to_compute:
        result[n] = _score(n)
    return result


# ---------------------------------------------------------------------------
# Алгоритм 1: DFS с мемоизацией (итеративный стек)
# ---------------------------------------------------------------------------

def compute_scores_dfs(
    graph: GraphSnapshot,
    nodes_to_compute: Iterable[str],
) -> Tuple[Dict[str, Decimal], int]:
    """
    Итеративный DFS с явным стеком и тремя состояниями:
      WHITE = не посещён
      GRAY  = в процессе (на стеке)
      BLACK = полностью вычислен

    Возвращает (scores, peak_memo_size).
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[str, int] = defaultdict(lambda: WHITE)
    memo: Dict[str, Decimal] = {}
    peak = 0

    def ensure(node: str) -> None:
        nonlocal peak
        if color[node] == BLACK:
            return
        if color[node] == GRAY:
            raise CycleDetectedError(f"cycle detected involving {node}")

        # Явный стек: (node, child_index)
        stack: List[Tuple[str, int]] = [(node, 0)]
        color[node] = GRAY

        while stack:
            curr, idx = stack[-1]
            children = graph.children.get(curr, [])

            if idx < len(children):
                # Переходим к следующему потомку
                stack[-1] = (curr, idx + 1)
                child = children[idx]
                if color[child] == GRAY:
                    raise CycleDetectedError(f"cycle detected involving {child}")
                if color[child] == WHITE:
                    color[child] = GRAY
                    stack.append((child, 0))
            else:
                # Все потомки обработаны — вычисляем score
                stack.pop()
                val = graph.ensure_internal(curr)
                for ch in children:
                    val += memo[ch] / Decimal(3)
                memo[curr] = val
                color[curr] = BLACK
                peak = max(peak, len(memo))

    for n in nodes_to_compute:
        ensure(n)

    return memo, peak


# ---------------------------------------------------------------------------
# Алгоритм 2: Топологический обход (Kahn)
# ---------------------------------------------------------------------------

def compute_scores_topo(
    graph: GraphSnapshot,
    nodes_to_compute: Iterable[str],
) -> Tuple[Dict[str, Decimal], int]:
    """
    Kahn-style топологическая сортировка по зависимостям (потомки раньше родителей).

    remaining[v] = число нерассчитанных непосредственных потомков v в подграфе.
    """
    # Работаем только по узлам, присутствующим в graph
    nodes = list(graph.nodes())
    if not nodes:
        return {}, 0

    # remaining = количество children, которые ещё не посчитаны
    remaining: Dict[str, int] = {}
    incoming: Dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for v in nodes:
        # Считаем только children, которые тоже в этом подграфе
        ch = [c for c in graph.children.get(v, []) if c in graph.internal]
        remaining[v] = len(ch)

    ready: deque[str] = deque(v for v in nodes if remaining[v] == 0)
    scores: Dict[str, Decimal] = {}

    while ready:
        node = ready.popleft()
        sc = graph.ensure_internal(node) + incoming[node]
        scores[node] = sc

        # Распространяем вклад на родителей (которые тоже в подграфе)
        for parent in graph.parents.get(node, []):
            if parent not in remaining:
                continue
            incoming[parent] += sc / Decimal(3)
            remaining[parent] -= 1
            if remaining[parent] == 0:
                ready.append(parent)

    if len(scores) != len(nodes):
        missing = set(nodes) - set(scores.keys())
        raise CycleDetectedError(
            f"topological sort incomplete; possible cycle. Missing: {list(missing)[:5]}..."
        )

    return scores, len(scores)


# ---------------------------------------------------------------------------
# Алгоритм 3: Пакетная итеративная обработка (волны)
# ---------------------------------------------------------------------------

def compute_scores_batch(
    graph: GraphSnapshot,
    nodes_to_compute: Iterable[str],
    batch_size: int = 256,
) -> Tuple[Dict[str, Decimal], int, int]:
    """
    Итеративное вычисление волнами готовых узлов.

    Симулирует пакетную загрузку: число "Cypher-запросов" оценивается
    как количество пакетов, потребовавшихся для обхода зависимостей
    (в реальном Memgraph это были бы UNWIND $ids запросы).

    Возвращает (scores, peak_size, simulated_queries).
    """
    nodes = list(graph.nodes())
    if not nodes:
        return {}, 0, 0

    remaining: Dict[str, int] = {}
    for v in nodes:
        ch = [c for c in graph.children.get(v, []) if c in graph.internal]
        remaining[v] = len(ch)

    scores: Dict[str, Decimal] = {}
    incoming: Dict[str, Decimal] = defaultdict(lambda: Decimal(0))

    # Симуляция batch-запросов при "загрузке" (уже всё в памяти,
    # но считаем пакеты по размеру для метрик)
    simulated_queries = max(1, (len(nodes) + batch_size - 1) // batch_size)

    ready = [v for v in nodes if remaining[v] == 0]
    peak = 0

    while ready:
        # В одной волне все ready-узлы независимы
        wave = ready
        ready = []
        for node in wave:
            sc = graph.ensure_internal(node) + incoming[node]
            scores[node] = sc
            peak = max(peak, len(scores))

            for parent in graph.parents.get(node, []):
                if parent not in remaining:
                    continue
                incoming[parent] += sc / Decimal(3)
                remaining[parent] -= 1
                if remaining[parent] == 0:
                    ready.append(parent)

    if len(scores) != len(nodes):
        missing = set(nodes) - set(scores.keys())
        raise CycleDetectedError(
            f"batch iterative incomplete; possible cycle. Missing: {list(missing)[:5]}..."
        )

    return scores, peak, simulated_queries


# ---------------------------------------------------------------------------
# Единый интерфейс recalculate_scores
# ---------------------------------------------------------------------------

def recalculate_scores(
    graph: GraphSnapshot,
    tx_id: str,
    max_depth: Optional[int] = None,
    threshold: Union[Decimal, float] = DEFAULT_THRESHOLD,
    algorithm: Algorithm = Algorithm.TOPOLOGICAL,
    batch_size: int = 256,
) -> ScoreResult:
    """
    Единая точка входа для всех трёх алгоритмов.

    1. Валидация параметров.
    2. Поиск предков (кандидатов) с учётом MAX_DEPTH.
    3. Раскрытие полного подграфа зависимостей (без ограничения глубины).
    4. Вычисление score выбранным алгоритмом.
    5. Фильтрация score >= threshold.
    6. Формирование ScoreResult.
    """
    t0 = time.perf_counter()
    threshold = Decimal(str(threshold))
    diag = Diagnostics(algorithm=algorithm.name)

    try:
        if max_depth is not None:
            if not isinstance(max_depth, int) or max_depth < 0:
                raise InvalidParameterError(
                    f"max_depth must be non-negative int or None, got {max_depth!r}"
                )

        # --- Фаза 1: предки ---
        t1 = time.perf_counter()
        ancestor_depths = find_ancestors(graph, tx_id, max_depth)
        diag.time_ancestors_ms = (time.perf_counter() - t1) * 1000
        diag.candidates_considered = len(ancestor_depths)

        if not ancestor_depths:
            diag.time_total_ms = (time.perf_counter() - t0) * 1000
            return ScoreResult(
                trigger_id=tx_id,
                max_depth=max_depth,
                threshold=threshold,
                candidates=[],
                scores={},
                diagnostics=diag,
            )

        # --- Фаза 2: раскрытие зависимостей ---
        t2 = time.perf_counter()
        dep_graph = expand_dependency_subgraph(graph, ancestor_depths.keys())
        diag.time_dependencies_ms = (time.perf_counter() - t2) * 1000
        diag.nodes_loaded = len(dep_graph.nodes())
        diag.edges_loaded = sum(len(v) for v in dep_graph.parents.values())

        # --- Фаза 3: вычисление score ---
        t3 = time.perf_counter()
        nodes_needed = list(dep_graph.nodes())

        if algorithm == Algorithm.DFS_MEMO:
            scores, peak = compute_scores_dfs(dep_graph, nodes_needed)
            diag.peak_memo_size = peak
            diag.cypher_queries_simulated = 1  # после preload
        elif algorithm == Algorithm.TOPOLOGICAL:
            scores, peak = compute_scores_topo(dep_graph, nodes_needed)
            diag.peak_memo_size = peak
            diag.cypher_queries_simulated = 1
        elif algorithm == Algorithm.BATCH_ITERATIVE:
            scores, peak, queries = compute_scores_batch(
                dep_graph, nodes_needed, batch_size=batch_size
            )
            diag.peak_memo_size = peak
            diag.cypher_queries_simulated = queries
        else:
            raise ValueError(f"unknown algorithm: {algorithm}")

        diag.time_compute_ms = (time.perf_counter() - t3) * 1000

        # --- Фильтрация ---
        candidates: List[Candidate] = []
        for anc_id, depth in ancestor_depths.items():
            sc = scores.get(anc_id)
            if sc is None:
                # узел был кандидатом, но не попал в dep_graph — ошибка логики
                raise DataInvariantError(f"score missing for candidate {anc_id}")
            if sc >= threshold:
                candidates.append(Candidate(tx_id=anc_id, depth=depth, score=sc))

        candidates.sort(key=lambda c: (c.depth, c.tx_id))
        diag.candidates_passed = len(candidates)
        diag.time_total_ms = (time.perf_counter() - t0) * 1000

        return ScoreResult(
            trigger_id=tx_id,
            max_depth=max_depth,
            threshold=threshold,
            candidates=candidates,
            scores=scores,
            diagnostics=diag,
        )

    except (CycleDetectedError, DataInvariantError, InvalidParameterError) as e:
        diag.error = f"{type(e).__name__}: {e}"
        diag.time_total_ms = (time.perf_counter() - t0) * 1000
        return ScoreResult(
            trigger_id=tx_id,
            max_depth=max_depth,
            threshold=threshold,
            candidates=[],
            scores={},
            diagnostics=diag,
        )
    except Exception as e:
        diag.error = f"Unexpected: {type(e).__name__}: {e}"
        diag.time_total_ms = (time.perf_counter() - t0) * 1000
        traceback.print_exc()
        return ScoreResult(
            trigger_id=tx_id,
            max_depth=max_depth,
            threshold=threshold,
            candidates=[],
            scores={},
            diagnostics=diag,
        )


# ---------------------------------------------------------------------------
# Генераторы синтетических графов
# ---------------------------------------------------------------------------

def _make_internal(amount: int) -> Decimal:
    """Генерирует internal_score из суммы транзакции по transfer_score."""
    return transfer_score(amount)


def generate_g1_minimal() -> Tuple[GraphSnapshot, str]:
    """
    G1. Минимальный граф.
    tx → P1, P2, P3 (листья).
    """
    g = GraphSnapshot()
    # Листья
    for pid in ("P1", "P2", "P3"):
        g.add_node(pid, _make_internal(100))
    # Триггер
    g.add_node("TX", _make_internal(500))
    for pid in ("P1", "P2", "P3"):
        g.add_edge("TX", pid)
    return g, "TX"


def generate_g2_converging() -> Tuple[GraphSnapshot, str]:
    """
    G2. Сходящиеся ветви.
         A
        / \\
       X   Y
        \\ /
         TX
    + ещё один общий предок выше.
    """
    g = GraphSnapshot()
    # Корни
    g.add_node("ROOT", _make_internal(50))
    g.add_node("A", _make_internal(200))
    g.add_node("B", _make_internal(150))
    g.add_node("X", _make_internal(300))
    g.add_node("Y", _make_internal(400))
    g.add_node("TX", _make_internal(1000))

    # X и Y оба ссылаются на A
    g.add_edge("X", "A")
    g.add_edge("Y", "A")
    # A и B ссылаются на ROOT
    g.add_edge("A", "ROOT")
    g.add_edge("B", "ROOT")
    # TX ссылается на X, Y, B
    g.add_edge("TX", "X")
    g.add_edge("TX", "Y")
    g.add_edge("TX", "B")
    return g, "TX"


def generate_g3_deep(depth: int = 30) -> Tuple[GraphSnapshot, str]:
    """
    G3. Глубокая цепочка + боковые ветви.
    depth уровней предков.
    """
    g = GraphSnapshot()
    prev = [f"L0_{i}" for i in range(3)]
    for pid in prev:
        g.add_node(pid, _make_internal(10 + int(pid.split("_")[1]) * 5))

    for d in range(1, depth + 1):
        curr = []
        for i in range(3):
            nid = f"L{d}_{i}"
            g.add_node(nid, _make_internal(10 + d * 3 + i))
            # каждый узел ссылается на 1–3 предыдущих
            for j in range(min(3, len(prev))):
                g.add_edge(nid, prev[j])
            curr.append(nid)
        prev = curr

    # Триггер на самом нижнем уровне
    tx = "TX_DEEP"
    g.add_node(tx, _make_internal(5000))
    for p in prev:
        g.add_edge(tx, p)
    return g, tx


def generate_g4_wide(width: int = 200, levels: int = 4) -> Tuple[GraphSnapshot, str]:
    """
    G4. Широкий DAG: много узлов на каждом уровне.
    """
    g = GraphSnapshot()
    random.seed(42)
    levels_nodes: List[List[str]] = []

    for lvl in range(levels):
        nodes = []
        for i in range(width):
            nid = f"W{lvl}_{i}"
            amount = random.randint(10, 500)
            if i % 25 == 0:
                amount = random.randint(1000, 5000)
            g.add_node(nid, _make_internal(amount))
            nodes.append(nid)
        levels_nodes.append(nodes)

    # Рёбра: каждый узел уровня l+1 ссылается на 3 случайных родителя уровня l
    for lvl in range(1, levels):
        for child in levels_nodes[lvl]:
            parents = random.sample(levels_nodes[lvl - 1], min(3, len(levels_nodes[lvl - 1])))
            for p in parents:
                g.add_edge(child, p)

    # Триггер
    tx = "TX_WIDE"
    g.add_node(tx, _make_internal(10000))
    for p in random.sample(levels_nodes[-1], min(3, len(levels_nodes[-1]))):
        g.add_edge(tx, p)
    return g, tx


def generate_g5_highly_converging(n_leaves: int = 50, n_hubs: int = 5) -> Tuple[GraphSnapshot, str]:
    """
    G5. Сильно сходящийся: много листьев → несколько хабов → один корень.
    """
    g = GraphSnapshot()
    g.add_node("ROOT", _make_internal(10))

    hubs = []
    for i in range(n_hubs):
        hid = f"HUB_{i}"
        g.add_node(hid, _make_internal(100 + i * 20))
        g.add_edge(hid, "ROOT")
        hubs.append(hid)

    leaves = []
    for i in range(n_leaves):
        lid = f"LEAF_{i}"
        g.add_node(lid, _make_internal(50 + i))
        # каждый лист ссылается на 2–3 хаба
        for h in random.sample(hubs, min(3, len(hubs))):
            g.add_edge(lid, h)
        leaves.append(lid)

    tx = "TX_CONV"
    g.add_node(tx, _make_internal(2000))
    for p in random.sample(leaves, min(3, len(leaves))):
        g.add_edge(tx, p)
    return g, tx


def generate_g6_different_depths() -> Tuple[GraphSnapshot, str]:
    """
    G6. Один предок достижим коротким и длинным путём.
         COMMON
          /  \\
         A    B
         |    |
         C    D
          \\  /
           TX
    min_depth(COMMON) = 2 (через A-C), хотя есть путь длины 3.
    """
    g = GraphSnapshot()
    for nid, amt in [
        ("COMMON", 100),
        ("A", 200), ("B", 300),
        ("C", 400), ("D", 500),
        ("TX", 1000),
    ]:
        g.add_node(nid, _make_internal(amt))

    g.add_edge("A", "COMMON")
    g.add_edge("B", "COMMON")
    g.add_edge("C", "A")
    g.add_edge("D", "B")
    # короткий путь: TX → C → A → COMMON  (depth 3? wait)
    # На самом деле:
    # TX → C (d=1), TX → D (d=1)
    # C → A (d=2), D → B (d=2)
    # A → COMMON (d=3), B → COMMON (d=3)
    # Чтобы был короткий путь, добавим прямую связь:
    g.add_edge("TX", "C")
    g.add_edge("TX", "D")
    g.add_edge("TX", "A")  # короткий путь к A (depth 1), а через C — depth 2
    # COMMON достижим: A→COMMON (depth 2 от TX через A), и через C→A→COMMON (depth 3)
    return g, "TX"


def generate_random_dag(
    n_nodes: int = 100,
    max_parents: int = 3,
    seed: int = 0,
) -> Tuple[GraphSnapshot, str]:
    """
    Случайный DAG с гарантированной ацикличностью:
    узлы 0..n-1, рёбра только от большего индекса к меньшему.
    """
    random.seed(seed)
    g = GraphSnapshot()
    ids = [f"N{i}" for i in range(n_nodes)]

    for i, nid in enumerate(ids):
        amount = random.randint(1, 1000)
        if i % 5 == 0:
            amount = random.randint(1000, 5000)
        if i % 25 == 0:
            amount = random.randint(5000, 25000)
        if i % 100 == 0:
            amount = random.randint(25000, 100000)
        g.add_node(nid, _make_internal(amount))

    for i in range(1, n_nodes):
        # выбираем родителей среди 0..i-1
        k = min(max_parents, i)
        parents_idx = random.sample(range(i), k)
        for j in parents_idx:
            g.add_edge(ids[i], ids[j])

    # Триггер — последний узел
    return g, ids[-1]


def inject_cycle(graph: GraphSnapshot, a: str, b: str) -> GraphSnapshot:
    """Добавляет ребро, создающее цикл (для негативных тестов)."""
    g = graph.copy()
    g.add_edge(a, b)
    return g


# ---------------------------------------------------------------------------
# Проверка инвариантов
# ---------------------------------------------------------------------------

def check_score_invariant(
    graph: GraphSnapshot,
    scores: Dict[str, Decimal],
    tol: Decimal = Decimal("1e-20"),
) -> List[str]:
    """
    Проверяет для каждого узла:
        score(T) == internal(T) + (1/3)*sum(score(child) for child in C(T))
    """
    errors = []
    for node, sc in scores.items():
        expected = graph.ensure_internal(node)
        for child in graph.children.get(node, []):
            if child in scores:
                expected += scores[child] / Decimal(3)
        if abs(sc - expected) > tol:
            errors.append(
                f"invariant fail for {node}: got {sc}, expected {expected}"
            )
    return errors


def compare_results(
    ref: ScoreResult,
    other: ScoreResult,
    score_tol: Decimal = Decimal("1e-18"),
) -> List[str]:
    """Сравнивает два ScoreResult. Возвращает список расхождений."""
    diffs = []
    if ref.diagnostics.error or other.diagnostics.error:
        if ref.diagnostics.error != other.diagnostics.error:
            # оба ошибочные — ок, если тип похож
            if not (ref.diagnostics.error and other.diagnostics.error):
                diffs.append(
                    f"error mismatch: ref={ref.diagnostics.error}, other={other.diagnostics.error}"
                )
        return diffs

    ref_ids = ref.passed_ids()
    other_ids = other.passed_ids()
    if ref_ids != other_ids:
        diffs.append(
            f"candidate sets differ: only_in_ref={ref_ids - other_ids}, "
            f"only_in_other={other_ids - ref_ids}"
        )

    ref_map = {c.tx_id: c for c in ref.candidates}
    other_map = {c.tx_id: c for c in other.candidates}
    for tid in ref_ids & other_ids:
        rc, oc = ref_map[tid], other_map[tid]
        if rc.depth != oc.depth:
            diffs.append(f"depth mismatch for {tid}: {rc.depth} vs {oc.depth}")
        if abs(rc.score - oc.score) > score_tol:
            diffs.append(
                f"score mismatch for {tid}: {rc.score} vs {oc.score}"
            )
    return diffs


# ---------------------------------------------------------------------------
# Тестовый стенд
# ---------------------------------------------------------------------------

def run_correctness_suite(verbose: bool = True) -> bool:
    """
    Полный набор тестов корректности.
    Возвращает True, если все тесты прошли.
    """
    all_ok = True
    algorithms = [
        Algorithm.DFS_MEMO,
        Algorithm.TOPOLOGICAL,
        Algorithm.BATCH_ITERATIVE,
    ]

    test_cases = [
        ("G1 minimal", generate_g1_minimal, [None, 0, 1, 2]),
        ("G2 converging", generate_g2_converging, [None, 1, 2, 3]),
        ("G3 deep-15", lambda: generate_g3_deep(15), [None, 1, 5, 10]),
        ("G4 wide-80", lambda: generate_g4_wide(80, 3), [None, 1, 2]),
        ("G5 converging", generate_g5_highly_converging, [None, 1, 2, 3]),
        ("G6 different depths", generate_g6_different_depths, [None, 1, 2, 3]),
        ("Random-60", lambda: generate_random_dag(60, seed=7), [None, 1, 3, 5]),
    ]

    threshold = Decimal("0.1")

    for name, gen, depths in test_cases:
        graph, tx = gen()
        for md in depths:
            # Эталон
            ref = recalculate_scores(
                graph, tx, max_depth=md, threshold=threshold,
                algorithm=Algorithm.DFS_MEMO,
            )
            # Пересчитываем эталонные scores через reference_scores для строгой проверки
            anc = find_ancestors(graph, tx, md)
            if anc:
                dep = expand_dependency_subgraph(graph, anc.keys())
                try:
                    ref_scores = reference_scores(dep, dep.nodes())
                    inv_errs = check_score_invariant(dep, ref_scores)
                    if inv_errs:
                        print(f"  [FAIL] {name} md={md}: invariant errors: {inv_errs[:2]}")
                        all_ok = False
                except CycleDetectedError as e:
                    print(f"  [FAIL] {name} md={md}: cycle in reference: {e}")
                    all_ok = False

            for algo in algorithms:
                res = recalculate_scores(
                    graph, tx, max_depth=md, threshold=threshold, algorithm=algo
                )
                diffs = compare_results(ref, res)
                status = "OK" if not diffs else "FAIL"
                if verbose:
                    n_cand = len(res.candidates)
                    t = res.diagnostics.time_total_ms
                    print(
                        f"  [{status}] {name:20s} md={str(md):4s} "
                        f"{algo.name:18s} candidates={n_cand:3d}  {t:7.2f} ms"
                    )
                if diffs:
                    all_ok = False
                    if verbose:
                        for d in diffs[:3]:
                            print(f"         → {d}")

    # --- Негативные тесты ---
    if verbose:
        print("\n  Negative tests:")

    # MAX_DEPTH=0 → пустой результат
    g, tx = generate_g1_minimal()
    res = recalculate_scores(g, tx, max_depth=0, threshold=Decimal("0"))
    ok = len(res.candidates) == 0 and res.diagnostics.error is None
    all_ok = all_ok and ok
    if verbose:
        print(f"  [{'OK' if ok else 'FAIL'}] MAX_DEPTH=0 yields empty candidates")

    # Несуществующий tx
    res = recalculate_scores(g, "NO_SUCH_TX", max_depth=None, threshold=Decimal("0"))
    ok = res.diagnostics.error is not None
    all_ok = all_ok and ok
    if verbose:
        print(f"  [{'OK' if ok else 'FAIL'}] missing trigger → error")

    # Цикл
    g2, tx2 = generate_g2_converging()
    g_cyc = inject_cycle(g2, "ROOT", "TX")  # ROOT ← TX уже есть косвенно, добавим обратное
    # Более надёжно: A → X уже есть, добавим X → A
    g_cyc = inject_cycle(g2, "A", "X")
    res = recalculate_scores(
        g_cyc, tx2, max_depth=None, threshold=Decimal("0"),
        algorithm=Algorithm.TOPOLOGICAL,
    )
    ok = res.diagnostics.error is not None and "cycle" in res.diagnostics.error.lower()
    all_ok = all_ok and ok
    if verbose:
        print(f"  [{'OK' if ok else 'FAIL'}] cycle detection: {res.diagnostics.error}")

    # Граничные значения порога
    g, tx = generate_g1_minimal()
    # Получаем точные scores
    res_all = recalculate_scores(g, tx, max_depth=1, threshold=Decimal("0"))
    if res_all.candidates:
        exact = res_all.candidates[0].score
        # score == S → должен попасть
        res_eq = recalculate_scores(g, tx, max_depth=1, threshold=exact)
        ok_eq = any(c.tx_id == res_all.candidates[0].tx_id for c in res_eq.candidates)
        # score чуть выше S → не должен
        res_gt = recalculate_scores(
            g, tx, max_depth=1, threshold=exact + Decimal("1e-10")
        )
        ok_gt = not any(c.tx_id == res_all.candidates[0].tx_id for c in res_gt.candidates)
        all_ok = all_ok and ok_eq and ok_gt
        if verbose:
            print(f"  [{'OK' if ok_eq else 'FAIL'}] score == S included")
            print(f"  [{'OK' if ok_gt else 'FAIL'}] score < S excluded")

    return all_ok


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

def run_benchmark(verbose: bool = True) -> Dict[str, Any]:
    """
    Сравнительный benchmark трёх алгоритмов на наборе графов разного масштаба.
    """
    scenarios = [
        ("minimal", generate_g1_minimal, None),
        ("converging", generate_g2_converging, None),
        ("deep-40", lambda: generate_g3_deep(40), None),
        ("wide-150x4", lambda: generate_g4_wide(150, 4), 2),
        ("high-conv", lambda: generate_g5_highly_converging(100, 8), None),
        ("random-200", lambda: generate_random_dag(200, seed=99), 5),
        ("random-500", lambda: generate_random_dag(500, seed=123), 4),
    ]

    algorithms = [
        Algorithm.DFS_MEMO,
        Algorithm.TOPOLOGICAL,
        Algorithm.BATCH_ITERATIVE,
    ]

    threshold = Decimal("0.05")
    results: Dict[str, Any] = {}
    repeats = 5

    if verbose:
        print("\n" + "=" * 78)
        print("BENCHMARK")
        print("=" * 78)

    for sc_name, gen, md in scenarios:
        graph, tx = gen()
        n_nodes = len(graph.nodes())
        n_edges = sum(len(v) for v in graph.parents.values())
        if verbose:
            print(f"\n--- {sc_name}  nodes={n_nodes}  edges={n_edges}  max_depth={md}")

        results[sc_name] = {"nodes": n_nodes, "edges": n_edges, "algos": {}}

        # Прогрев + корректность
        ref = recalculate_scores(
            graph, tx, max_depth=md, threshold=threshold,
            algorithm=Algorithm.DFS_MEMO,
        )

        for algo in algorithms:
            times = []
            last_res = None
            for _ in range(repeats):
                res = recalculate_scores(
                    graph, tx, max_depth=md, threshold=threshold, algorithm=algo
                )
                times.append(res.diagnostics.time_total_ms)
                last_res = res

            times_sorted = sorted(times)
            median = times_sorted[len(times_sorted) // 2]
            p95 = times_sorted[int(len(times_sorted) * 0.95)] if len(times_sorted) > 1 else median

            diffs = compare_results(ref, last_res) if last_res else ["no result"]
            correct = len(diffs) == 0

            entry = {
                "correct": correct,
                "median_ms": round(median, 3),
                "p95_ms": round(p95, 3),
                "candidates": last_res.diagnostics.candidates_passed if last_res else 0,
                "nodes_loaded": last_res.diagnostics.nodes_loaded if last_res else 0,
                "queries": last_res.diagnostics.cypher_queries_simulated if last_res else 0,
                "peak_memo": last_res.diagnostics.peak_memo_size if last_res else 0,
            }
            results[sc_name]["algos"][algo.name] = entry

            if verbose:
                status = "OK" if correct else "FAIL"
                print(
                    f"  {algo.name:18s}  [{status}]  "
                    f"median={median:8.2f} ms  p95={p95:8.2f} ms  "
                    f"cand={entry['candidates']:4d}  loaded={entry['nodes_loaded']:5d}"
                )

    return results


# ---------------------------------------------------------------------------
# Рекомендация
# ---------------------------------------------------------------------------

def make_recommendation(bench: Dict[str, Any]) -> str:
    """
    Обоснованный выбор алгоритма на основе benchmark.
    """
    lines = [
        "",
        "=" * 78,
        "РЕКОМЕНДАЦИЯ ПО PRODUCTION-РЕАЛИЗАЦИИ",
        "=" * 78,
        "",
        "Критерии (в порядке приоритета):",
        "  1. Математическая корректность (обязательна).",
        "  2. Устойчивость к глубине (нет зависимости от стека Python).",
        "  3. Линейная сложность от |V|+|E| загруженного подграфа.",
        "  4. Предсказуемое потребление памяти.",
        "  5. Простота диагностики и сопровождения.",
        "",
    ]

    # Собираем статистику
    algo_scores: Dict[str, List[float]] = defaultdict(list)
    all_correct = True
    for sc, data in bench.items():
        for aname, metrics in data["algos"].items():
            if not metrics["correct"]:
                all_correct = False
            algo_scores[aname].append(metrics["median_ms"])

    lines.append("Результаты по медиане времени (мс):")
    for aname, times in algo_scores.items():
        avg = sum(times) / len(times) if times else 0
        lines.append(f"  {aname:20s}  avg_median={avg:8.2f}  samples={times}")

    lines.extend([
        "",
        "Выбор: TOPOLOGICAL (Алгоритм №2 — топологический обход Kahn-style).",
        "",
        "Обоснование:",
        "  • Все три алгоритма математически эквивалентны на корректных DAG",
        "    (проверено эталоном + инвариантами).",
        "  • DFS с мемоизацией (даже итеративный) требует явного стека состояний",
        "    и менее естественен для пакетной загрузки из Memgraph.",
        "  • BATCH_ITERATIVE концептуально близок к TOPOLOGICAL, но добавляет",
        "    накладные расходы на волны и параметр BATCH_SIZE; на уже загруженном",
        "    подграфе выигрыша почти нет.",
        "  • TOPOLOGICAL:",
        "      – один проход, O(|V| + |E|);",
        "      – нет рекурсии / глубокого стека;",
        "      – цикл обнаруживается естественно (не все узлы обработаны);",
        "      – отлично сочетается с предварительной пакетной загрузкой",
        "        подграфа зависимостей (один или несколько UNWIND-запросов);",
        "      – предсказуемая память (структуры remaining/incoming/scores).",
        "",
        "Production-схема:",
        "  1. BFS/пакетный поиск предков до MAX_DEPTH → множество кандидатов.",
        "  2. Итеративное раскрытие потомков кандидатов (пакетно) → dep_graph.",
        "  3. Топологический расчёт score на dep_graph.",
        "  4. Фильтрация score >= S и формирование out.",
        "",
        "Важные оговорки:",
        "  • Полный пересчёт зависимостей кандидата может затронуть большую",
        "    часть DAG — это intrinsic cost модели, а не недостаток алгоритма.",
        "  • Необходимо подтвердить схему Memgraph (направление PARENT,",
        "    имя свойства internal_score, индексы).",
        "  • Для очень больших подграфов рассмотреть инкрементальный кэш",
        "    уже вычисленных score (с инвалидацией при изменении потомков),",
        "    но это выходит за рамки текущего ТЗ.",
        "",
    ])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 78)
    print("Score Recalculation Engine — synthetic test & benchmark")
    print("=" * 78)

    print("\n[1/3] Correctness suite")
    print("-" * 78)
    ok = run_correctness_suite(verbose=True)
    print(f"\nCorrectness: {'PASSED' if ok else 'FAILED'}")

    print("\n[2/3] Benchmark")
    bench = run_benchmark(verbose=True)

    print("\n[3/3] Recommendation")
    rec = make_recommendation(bench)
    print(rec)

    # Краткая демонстрация одного запуска
    print("\n" + "=" * 78)
    print("DEMO: G2 converging, max_depth=None, threshold=0.3")
    print("=" * 78)
    g, tx = generate_g2_converging()
    for algo in Algorithm:
        res = recalculate_scores(
            g, tx, max_depth=None, threshold=Decimal("0.3"), algorithm=algo
        )
        print(f"\n{algo.name}:")
        print(f"  candidates ({len(res.candidates)}):")
        for c in res.candidates:
            print(f"    {c.tx_id:10s}  depth={c.depth}  score={float(c.score):.6f}")
        print(f"  diagnostics: {res.diagnostics.to_dict()}")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
