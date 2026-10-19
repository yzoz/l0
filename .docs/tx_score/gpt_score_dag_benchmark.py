"""DAG transaction score: three implementations, synthetic graph, tests, benchmark.

Python 3.10+, standard library only.
Run:
  python score_dag_benchmark.py
  python score_dag_benchmark.py --nodes 20000 --seed 7
  python score_dag_benchmark.py --nodes 100000 --max-depth 5 --bench-repeats 3
  python score_dag_benchmark.py --skip-benchmark

Graph orientation: child -> parent.
score(v) = internal_score(v) + sum(score(child) / 3 for child in children(v)).

MAX_DEPTH limits which ancestors of trigger are eligible as output candidates; it does
not truncate the descendant dependency closure needed to calculate their exact scores.
This is an in-memory reference implementation. The batch algorithm models batched
frontier processing; an actual Memgraph adapter should replace the graph loader with
parameterized Cypher/UNWIND queries and measure database/network costs separately.
"""
from __future__ import annotations

import argparse
import math
import random
import statistics
import time
import tracemalloc
import unittest
from collections import deque
from dataclasses import dataclass
from typing import Callable, Iterable

ALPHA = 0.005
BETA = 0.5
DEFAULT_THRESHOLD = 1.0


def transfer_score(x: float, alpha: float = ALPHA, beta: float = BETA) -> float:
    """Monotone saturating score function supplied by the user."""
    if x < 0:
        raise ValueError("x must be >= 0")
    if alpha <= 0 or beta <= 0:
        raise ValueError("alpha and beta must be > 0")
    return 1.0 - (1.0 + alpha * x) ** (-beta)


@dataclass(frozen=True)
class Transaction:
    tx_id: int
    internal_score: float


class GraphStore:
    """Small graph adapter: parents[v] are direct parents; children[v] reverse index."""
    def __init__(self) -> None:
        self.internal: dict[int, float] = {}
        self.parents: dict[int, tuple[int, ...]] = {}
        self.children: dict[int, list[int]] = {}

    def add_transaction(self, tx_id: int, internal_score: float,
                        parents: Iterable[int] = ()) -> None:
        if tx_id in self.internal:
            raise ValueError(f"duplicate tx_id: {tx_id}")
        if not math.isfinite(internal_score):
            raise ValueError(f"non-finite internal_score for {tx_id}")
        ps = tuple(parents)
        if len(set(ps)) != len(ps):
            raise ValueError(f"duplicate parent for {tx_id}")
        if tx_id in ps:
            raise ValueError("self-loop creates a cycle")
        missing = [p for p in ps if p not in self.internal]
        if missing:
            raise ValueError(f"parents must be inserted before child: {missing[:5]}")
        self.internal[tx_id] = float(internal_score)
        self.parents[tx_id] = ps
        self.children[tx_id] = []
        for p in ps:
            self.children[p].append(tx_id)

    def validate_dag(self) -> None:
        """Validate acyclicity. The orientation is child -> parent."""
        remaining = {v: len(self.children[v]) for v in self.internal}
        ready = deque(v for v, n in remaining.items() if n == 0)
        done = 0
        while ready:
            v = ready.popleft()
            done += 1
            for p in self.parents[v]:
                remaining[p] -= 1
                if remaining[p] == 0:
                    ready.append(p)
        if done != len(self.internal):
            raise ValueError("graph contains a cycle or inconsistent edges")


@dataclass
class ScoreResult:
    trigger_id: int
    max_depth: int | None
    threshold: float
    candidates: dict[int, float]              # only candidates whose score >= threshold
    all_candidate_scores: dict[int, float]    # all eligible ancestors, before threshold
    dependency_nodes: int
    dependency_edges: int
    elapsed_seconds: float
    algorithm: str

    @property
    def candidate_ids(self) -> set[int]:
        return set(self.candidates)


def find_candidates(graph: GraphStore, trigger_id: int,
                    max_depth: int | None) -> dict[int, int]:
    """Return unique ancestors -> minimum distance from trigger. Trigger is excluded."""
    if trigger_id not in graph.internal:
        raise KeyError(f"unknown trigger_id={trigger_id}")
    if max_depth is not None and (not isinstance(max_depth, int)
                                  or isinstance(max_depth, bool) or max_depth < 0):
        raise ValueError("max_depth must be a non-negative int or None")
    if max_depth == 0:
        return {}

    depth: dict[int, int] = {}
    queue: deque[int] = deque()
    for p in graph.parents[trigger_id]:
        if p not in depth:
            depth[p] = 1
            queue.append(p)
    while queue:
        node = queue.popleft()
        d = depth[node]
        if max_depth is not None and d >= max_depth:
            continue
        for p in graph.parents[node]:
            nd = d + 1
            if p not in depth or nd < depth[p]:
                depth[p] = nd
                queue.append(p)
    return depth


def dependency_closure(graph: GraphStore, candidates: Iterable[int]) -> set[int]:
    """All candidate nodes plus all their descendants, recursively down to leaves."""
    closure = set(candidates)
    stack = list(closure)
    while stack:
        v = stack.pop()
        for child in graph.children[v]:
            if child not in closure:
                closure.add(child)
                stack.append(child)
    return closure


def _result(graph: GraphStore, trigger_id: int, max_depth: int | None,
            threshold: float, candidate_depths: dict[int, int], scores: dict[int, float],
            closure: set[int], start: float, algorithm: str) -> ScoreResult:
    if not math.isfinite(threshold):
        raise ValueError("threshold must be finite")
    all_scores = {v: scores[v] for v in candidate_depths}
    selected = {v: s for v, s in all_scores.items() if s >= threshold}
    edge_count = sum(1 for child in closure for p in graph.parents[child] if p in closure)
    return ScoreResult(trigger_id, max_depth, threshold, selected, all_scores,
                       len(closure), edge_count, time.perf_counter() - start, algorithm)


def score_dfs_memo(graph: GraphStore, trigger_id: int,
                   max_depth: int | None = None,
                   threshold: float = DEFAULT_THRESHOLD) -> ScoreResult:
    """Algorithm 1: iterative DFS + memoization (safe for very deep DAGs).

    The explicit stack replaces Python recursion, so a long chain cannot fail merely
    because it exceeds sys.getrecursionlimit(). Each node is memoized once. The
    expanded/unexpanded stack frames implement post-order DFS: children are scored
    before their parent. A node already on the active DFS path indicates a cycle.
    """
    start = time.perf_counter()
    candidate_depths = find_candidates(graph, trigger_id, max_depth)
    closure = dependency_closure(graph, candidate_depths)
    memo: dict[int, float] = {}
    visiting: set[int] = set()

    for root in candidate_depths:
        if root in memo:
            continue
        # False = enter node; True = all dependencies should now be ready.
        stack: list[tuple[int, bool]] = [(root, False)]
        while stack:
            node, expanded = stack.pop()
            if node in memo:
                continue
            if expanded:
                value = graph.internal[node]
                for child in graph.children[node]:
                    if child in closure:
                        if child not in memo:
                            raise RuntimeError(f"DFS dependency not ready: {child}")
                        value += memo[child] / 3.0
                if not math.isfinite(value):
                    raise ArithmeticError(f"non-finite score for node {node}")
                memo[node] = value
                visiting.remove(node)
                continue

            if node in visiting:
                raise ValueError(f"cycle detected during DFS at node {node}")
            visiting.add(node)
            stack.append((node, True))
            # Reverse preserves deterministic traversal order with LIFO stack.
            for child in reversed(graph.children[node]):
                if child not in closure or child in memo:
                    continue
                if child in visiting:
                    raise ValueError(f"cycle detected during DFS at node {child}")
                stack.append((child, False))

    return _result(graph, trigger_id, max_depth, threshold, candidate_depths,
                   memo, closure, start, "dfs_memo_iterative")


def score_topological(graph: GraphStore, trigger_id: int,
                      max_depth: int | None = None,
                      threshold: float = DEFAULT_THRESHOLD) -> ScoreResult:
    """Algorithm 2: Kahn-style topological evaluation, descendants before parents."""
    start = time.perf_counter()
    candidate_depths = find_candidates(graph, trigger_id, max_depth)
    closure = dependency_closure(graph, candidate_depths)
    remaining = {v: sum(1 for c in graph.children[v] if c in closure) for v in closure}
    incoming = {v: 0.0 for v in closure}
    ready = deque(v for v, count in remaining.items() if count == 0)
    scores: dict[int, float] = {}

    while ready:
        v = ready.popleft()
        value = graph.internal[v] + incoming[v]
        if not math.isfinite(value):
            raise ArithmeticError(f"non-finite score for node {v}")
        scores[v] = value
        for parent in graph.parents[v]:
            if parent not in closure:
                continue
            incoming[parent] += value / 3.0
            remaining[parent] -= 1
            if remaining[parent] == 0:
                ready.append(parent)

    if len(scores) != len(closure):
        raise ValueError("cycle or incomplete dependency in topological pass")
    return _result(graph, trigger_id, max_depth, threshold, candidate_depths,
                   scores, closure, start, "topological")


def score_batch_iterative(graph: GraphStore, trigger_id: int,
                          max_depth: int | None = None,
                          threshold: float = DEFAULT_THRESHOLD,
                          batch_size: int = 1024) -> ScoreResult:
    """Algorithm 3: batch-frontier closure expansion + iterative ready-node waves.

    Here the graph is already in RAM, so batch_size controls logical batches. In a
    Memgraph adapter it should also cap IDs passed to each parameterized UNWIND query.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    start = time.perf_counter()
    candidate_depths = find_candidates(graph, trigger_id, max_depth)

    closure: set[int] = set()
    frontier = list(candidate_depths)
    while frontier:
        next_frontier: list[int] = []
        for offset in range(0, len(frontier), batch_size):
            for node in frontier[offset:offset + batch_size]:
                if node in closure:
                    continue
                closure.add(node)
                next_frontier.extend(c for c in graph.children[node] if c not in closure)
        # De-duplicate each wave while preserving deterministic insertion order.
        frontier = list(dict.fromkeys(v for v in next_frontier if v not in closure))

    remaining = {v: sum(1 for c in graph.children[v] if c in closure) for v in closure}
    incoming = {v: 0.0 for v in closure}
    ready = [v for v, count in remaining.items() if count == 0]
    scores: dict[int, float] = {}
    while ready:
        wave, ready = ready, []
        for offset in range(0, len(wave), batch_size):
            batch = wave[offset:offset + batch_size]
            # Calculate the whole batch first, then propagate its contributions.
            for v in batch:
                value = graph.internal[v] + incoming[v]
                if not math.isfinite(value):
                    raise ArithmeticError(f"non-finite score for node {v}")
                scores[v] = value
            for v in batch:
                value = scores[v]
                for parent in graph.parents[v]:
                    if parent not in closure:
                        continue
                    incoming[parent] += value / 3.0
                    remaining[parent] -= 1
                    if remaining[parent] == 0:
                        ready.append(parent)

    if len(scores) != len(closure):
        raise ValueError("cycle or incomplete dependency in batch pass")
    return _result(graph, trigger_id, max_depth, threshold, candidate_depths,
                   scores, closure, start, f"batch_iterative({batch_size})")


def generate_synthetic_graph(n: int, seed: int = 7,
                             alpha: float = ALPHA, beta: float = BETA,
                             max_parents: int = 3) -> GraphStore:
    """Generate a deterministic DAG and transaction amounts with tiered distribution.

    For each new transaction, parent IDs are selected only from older IDs, so graph
    construction is acyclic. The first few transactions naturally have fewer than
    three parents. The rarest amount tier takes precedence over more common tiers.
    """
    if n < 1:
        raise ValueError("n must be >= 1")
    if not 0 <= max_parents <= 3:
        raise ValueError("max_parents must be in 0..3")
    rng = random.Random(seed)
    graph = GraphStore()
    for tx_id in range(n):
        j = tx_id + 1
        if j % 100 == 0:
            amount = rng.randint(25_000, 100_000)
        elif j % 25 == 0:
            amount = rng.randint(5_000, 25_000)
        elif j % 5 == 0:
            amount = rng.randint(1_000, 5_000)
        else:
            amount = rng.randint(1, 1_000)
        parent_count = min(max_parents, tx_id)
        parents = rng.sample(range(tx_id), parent_count) if parent_count else ()
        graph.add_transaction(tx_id, transfer_score(amount, alpha, beta), parents)
    return graph


def assert_same_results(results: list[ScoreResult], tolerance: float = 1e-12) -> None:
    """Assert same candidate IDs and numerically equivalent ancestor scores."""
    if not results:
        return
    ref = results[0]
    for result in results[1:]:
        if result.candidate_ids != ref.candidate_ids:
            raise AssertionError(f"candidate sets differ: {ref.algorithm} / {result.algorithm}")
        if result.all_candidate_scores.keys() != ref.all_candidate_scores.keys():
            raise AssertionError("ancestor sets differ")
        for node, expected in ref.all_candidate_scores.items():
            actual = result.all_candidate_scores[node]
            if not math.isclose(actual, expected, rel_tol=tolerance, abs_tol=tolerance):
                raise AssertionError(f"score differs at {node}: {expected} vs {actual}")


def make_handcrafted_graph() -> GraphStore:
    """Small DAG with shared ancestors and converging paths."""
    g = GraphStore()
    g.add_transaction(0, 0.25)
    g.add_transaction(1, 0.10, (0,))
    g.add_transaction(2, 0.20, (0,))
    g.add_transaction(3, 0.15, (0,))
    g.add_transaction(4, 0.30, (1, 2, 3))
    g.add_transaction(5, 0.12, (4, 2, 3))
    g.add_transaction(6, 0.18, (5, 4, 1))  # trigger
    return g


class ScoreDagTests(unittest.TestCase):
    def setUp(self) -> None:
        self.graph = make_handcrafted_graph()
        self.algorithms = (score_dfs_memo, score_topological, score_batch_iterative)

    def test_transfer_score(self) -> None:
        self.assertEqual(transfer_score(0), 0.0)
        self.assertGreater(transfer_score(100), 0)
        self.assertLess(transfer_score(100), 1)
        self.assertGreater(transfer_score(100_000), transfer_score(100))

    def test_algorithms_agree_for_depths(self) -> None:
        for depth in (None, 0, 1, 2, 3):
            results = [fn(self.graph, 6, max_depth=depth, threshold=0.0)
                       for fn in self.algorithms]
            assert_same_results(results)

    def test_trigger_never_candidate(self) -> None:
        result = score_topological(self.graph, 6, threshold=0.0)
        self.assertNotIn(6, result.candidates)

    def test_depth_one_only_selects_direct_parents(self) -> None:
        result = score_topological(self.graph, 6, max_depth=1, threshold=0.0)
        self.assertEqual(set(result.all_candidate_scores), set(self.graph.parents[6]))
        self.assertGreaterEqual(result.dependency_nodes, len(result.all_candidate_scores))

    def test_threshold(self) -> None:
        self.assertGreater(len(score_topological(self.graph, 6, threshold=-1).candidates), 0)
        self.assertEqual(len(score_topological(self.graph, 6, threshold=1e9).candidates), 0)

    def test_bad_depth_and_missing_trigger(self) -> None:
        with self.assertRaises(ValueError):
            score_topological(self.graph, 6, max_depth=-1)
        with self.assertRaises(KeyError):
            score_topological(self.graph, 999)

    def test_cycle_detection(self) -> None:
        g = GraphStore()
        g.add_transaction(1, 0.1)
        g.add_transaction(2, 0.2, (1,))
        # Deliberately corrupt the graph to create 1 -> 2 -> 1.
        g.parents[1] = (2,)
        g.children[2].append(1)
        with self.assertRaises(ValueError):
            score_topological(g, 2)

    def test_synthetic_agreement(self) -> None:
        g = generate_synthetic_graph(300, seed=123)
        results = [
            score_dfs_memo(g, 299, max_depth=4, threshold=0),
            score_topological(g, 299, max_depth=4, threshold=0),
            score_batch_iterative(g, 299, max_depth=4, threshold=0, batch_size=13),
        ]
        assert_same_results(results)

    def test_repeat_run_is_deterministic(self) -> None:
        a = score_topological(self.graph, 6, threshold=0)
        b = score_topological(self.graph, 6, threshold=0)
        self.assertEqual(a.candidates, b.candidates)
        self.assertEqual(a.all_candidate_scores, b.all_candidate_scores)

    def test_deep_chain_does_not_depend_on_python_recursion(self) -> None:
        # A 1,500-node chain is deeper than the common default recursion limit.
        # All three algorithms must still complete because DFS uses an explicit stack.
        g = GraphStore()
        g.add_transaction(0, 0.1)
        for node in range(1, 1500):
            g.add_transaction(node, 0.1, (node - 1,))
        results = [fn(g, 1499, max_depth=2, threshold=0.0) for fn in self.algorithms]
        assert_same_results(results)


def run_benchmark(graph: GraphStore, trigger_id: int, repeats: int = 3,
                  max_depth: int | None = 4, threshold: float = DEFAULT_THRESHOLD
                  ) -> list[dict[str, float | int | str]]:
    """Run each algorithm identically; tracemalloc measures Python allocations only."""
    algorithms: list[tuple[str, Callable[..., ScoreResult]]] = [
        ("DFS + memo", score_dfs_memo),
        ("Topological", score_topological),
        ("Batch iterative", score_batch_iterative),
    ]
    rows: list[dict[str, float | int | str]] = []
    results: list[ScoreResult] = []
    for label, fn in algorithms:
        durations: list[float] = []
        peaks: list[int] = []
        last: ScoreResult | None = None
        for _ in range(repeats):
            tracemalloc.start()
            if fn is score_batch_iterative:
                result = fn(graph, trigger_id, max_depth=max_depth,
                            threshold=threshold, batch_size=1024)
            else:
                result = fn(graph, trigger_id, max_depth=max_depth, threshold=threshold)
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            durations.append(result.elapsed_seconds)
            peaks.append(peak)
            last = result
        assert last is not None
        results.append(last)
        rows.append({
            "algorithm": label,
            "median_seconds": statistics.median(durations),
            "max_seconds": max(durations),
            "peak_python_mib": max(peaks) / (1024 * 1024),
            "dependency_nodes": last.dependency_nodes,
            "dependency_edges": last.dependency_edges,
            "candidate_count": len(last.candidates),
        })
    assert_same_results(results)
    return rows


def print_benchmark(rows: list[dict[str, float | int | str]]) -> None:
    columns = ("algorithm", "median_seconds", "max_seconds", "peak_python_mib",
               "dependency_nodes", "dependency_edges", "candidate_count")
    print("\nBenchmark (time is Python end-to-end; memory is tracemalloc):")
    print(" | ".join(columns))
    print("-" * 118)
    for row in rows:
        print(" | ".join(f"{row[c]:.6f}" if isinstance(row[c], float) else str(row[c])
                         for c in columns))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--bench-repeats", type=int, default=3)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--skip-benchmark", action="store_true")
    args = parser.parse_args()

    print("Running unit tests...")
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ScoreDagTests)
    test_result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not test_result.wasSuccessful():
        return 1
    if args.skip_benchmark:
        return 0
    if args.nodes < 1 or args.bench_repeats < 1:
        parser.error("--nodes and --bench-repeats must be >= 1")

    print(f"\nGenerating DAG: nodes={args.nodes}, seed={args.seed}")
    graph = generate_synthetic_graph(args.nodes, seed=args.seed)
    trigger_id = args.nodes - 1
    edges = sum(len(ps) for ps in graph.parents.values())
    print(f"nodes={len(graph.internal):,}; edges={edges:,}; trigger={trigger_id}; "
          f"max_depth={args.max_depth}")
    rows = run_benchmark(graph, trigger_id, args.bench_repeats,
                         args.max_depth, args.threshold)
    print_benchmark(rows)
    print("\nRecommendation: topological traversal is the default production candidate "
          "for a loaded dependency subgraph: O(V+E), iterative, no Python recursion limit. "
          "Use batch iterative processing as the Memgraph integration strategy if batched "
          "UNWIND reads reduce query/network overhead. Confirm with real Memgraph benchmarks.")
    print("Limit: this benchmark starts with the graph already in Python memory. It does "
          "not measure Cypher latency, network transfer, or Memgraph's own memory.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
