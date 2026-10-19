"""Тесты формулы transfer_score и рекурсивного расчёта score (см. .docs/TODO_004.md).

Score транзакции t:
    score(t) = internal_score(t) + SUM( score(child) / 3 )
      по всем непосредственным детям t, рекурсивно по поддереву. Порог: score >= 1.

``internal_score = transfer_score(amount)`` precomputed consumer'ом.
``compute_scores``/``find_ancestors`` — чистая арифметика поверх значений графа.
"""

import pytest

from service.confirmer import CHILD_DIVISOR, SCORE_THRESHOLD, compute_scores, find_ancestors
from src.utils.fn import alpha, beta, transfer_score


class TestTransferScore:
    """Формула transfer_score(x, alpha, beta) в src/utils/fn.py."""

    def test_zero_amount_gives_zero(self):
        assert transfer_score(0, alpha, beta) == pytest.approx(0.0)

    def test_monotonic_growth(self):
        prev = 0.0
        for x in (1, 10, 100, 1000, 10_000, 100_000):
            s = transfer_score(x, alpha, beta)
            assert s > prev
            prev = s

    def test_asymptote_below_one(self):
        # transfer_score стремится к 1, но никогда не достигает его:
        # транзакция без детей не может пройти порог score >= 1.
        assert transfer_score(10**12, alpha, beta) < 1.0

    def test_formula_values(self):
        # Проверка на конкретных значениях: 1 - (1 + 0.0005*x)^(-0.5)
        assert transfer_score(100, alpha, beta) == pytest.approx(1 - (1 + 0.05) ** (-0.5))
        assert transfer_score(1000, alpha, beta) == pytest.approx(1 - (1 + 0.5) ** (-0.5))


class TestComputeScores:
    """Рекурсивный расчёт score по поддереву графа.

    compute_scores(internal, children, roots) — НЕ вызывает transfer_score,
    только суммирует и делит на CHILD_DIVISOR.
    """

    def test_divisor_is_three(self):
        # Делитель — фиксированный литерал 3 (число родителей), не config.
        assert CHILD_DIVISOR == 3

    def test_leaf_score_is_internal(self):
        # Лист без детей: score = internal_score.
        scores = compute_scores({"A": 0.5}, {"A": []}, ["A"])
        assert scores["A"] == pytest.approx(0.5)

    def test_direct_children_contribute_divided_by_three(self):
        # Вклад каждого ребёнка делится ровно на 3.
        scores = compute_scores({"A": 0.6, "B": 0.9}, {"A": ["B"], "B": []}, ["A"])
        assert scores["A"] == pytest.approx(0.6 + 0.9 / 3)

    def test_multiple_children_sum(self):
        # Все дети суммируются, каждый со своим /3.
        internal = {"A": 0.1, "B": 0.3, "C": 0.6, "D": 0.9}
        children = {"A": ["B", "C", "D"], "B": [], "C": [], "D": []}
        scores = compute_scores(internal, children, ["A"])
        assert scores["A"] == pytest.approx(0.1 + 0.3 / 3 + 0.6 / 3 + 0.9 / 3)

    def test_recursive_chain(self):
        # Цепочка из нескольких уровней: score тянется снизу вверх.
        internal = {"A": 0.1, "B": 0.2, "C": 0.3}
        children = {"A": ["B"], "B": ["C"], "C": []}
        scores = compute_scores(internal, children, ["A"])
        assert scores["C"] == pytest.approx(0.3)
        assert scores["B"] == pytest.approx(0.2 + 0.3 / 3)
        assert scores["A"] == pytest.approx(0.1 + (0.2 + 0.3 / 3) / 3)

    def test_independent_branch_included(self):
        # Ветвь поддерева, не проходящая через триггер, тоже учитывается.
        internal = {"A": 0.1, "B": 0.2, "X": 0.9}
        children = {"A": ["B", "X"], "B": [], "X": []}
        scores = compute_scores(internal, children, ["A"])
        assert scores["A"] == pytest.approx(0.1 + 0.2 / 3 + 0.9 / 3)

    def test_dag_convergence_counts_each_branch_once(self):
        # Схождение путей: общий потомок входит через каждую ветвь, но
        # собственный internal_score узла добавляется ровно один раз.
        internal = {"A": 0.1, "B": 0.2, "C": 0.3, "D": 0.9}
        children = {"A": ["B", "C"], "B": ["D"], "C": ["D"], "D": []}
        scores = compute_scores(internal, children, ["A"])
        d = 0.9
        b = 0.2 + d / 3
        c = 0.3 + d / 3
        assert scores["D"] == pytest.approx(d)
        assert scores["A"] == pytest.approx(0.1 + b / 3 + c / 3)

    def test_shared_subtree_memoized(self):
        # Общее поддерево двух корней считается один раз.
        internal = {"A": 0.1, "B": 0.1, "S": 0.9}
        children = {"A": ["S"], "B": ["S"], "S": []}
        scores = compute_scores(internal, children, ["A", "B"])
        assert scores["S"] == pytest.approx(0.9)
        assert scores["A"] == pytest.approx(0.1 + 0.9 / 3)
        assert scores["B"] == pytest.approx(0.1 + 0.9 / 3)

    def test_cycle_raises(self):
        # Цикл в графе — ValueError, частичный результат не публикуется.
        internal = {"A": 0.1, "B": 0.2, "C": 0.3}
        children = {"A": ["B"], "B": ["C"], "C": ["A"]}
        with pytest.raises(ValueError):
            compute_scores(internal, children, ["A"])

    def test_score_can_reach_threshold(self):
        # Порог достижим: родитель с крупным internal_score + один крупный ребёнок.
        parent = transfer_score(100_000, alpha, beta)   # ≈ 0.86
        child = transfer_score(50_000, alpha, beta)     # ≈ 0.80
        scores = compute_scores({"P": parent, "C": child}, {"P": ["C"], "C": []}, ["P"])
        assert scores["P"] >= SCORE_THRESHOLD

    def test_small_amounts_below_threshold(self):
        # Мелкие суммы не дают прохода: score < 1.
        parent = transfer_score(100, alpha, beta)       # ≈ 0.22
        child = transfer_score(100, alpha, beta)
        scores = compute_scores(
            {"P": parent, "C": child}, {"P": ["C"], "C": []}, ["P"]
        )
        assert scores["P"] < SCORE_THRESHOLD

    def test_many_children_accumulate(self):
        # Много детей накапливают score за счёт суммирования вкладов.
        parent = transfer_score(50_000, alpha, beta)    # ≈ 0.80
        child = transfer_score(50_000, alpha, beta)
        kids = [f"C{i}" for i in range(10)]
        internal = {"P": parent, **{k: child for k in kids}}
        children = {"P": kids, **{k: [] for k in kids}}
        scores = compute_scores(internal, children, ["P"])
        assert scores["P"] >= SCORE_THRESHOLD


class TestFindAncestors:
    """Поиск предков триггера с ограничением MAX_DEPTH."""

    # DAG: (child)->[parents]. T зависит от A,B напрямую; A от C; B от C,D.
    PARENTS = {"T": ["A", "B"], "A": ["C"], "B": ["C", "D"], "C": [], "D": []}

    def test_depth_one_direct_parents(self):
        assert find_ancestors("T", self.PARENTS, 1) == {"A": 1, "B": 1}

    def test_depth_two(self):
        got = find_ancestors("T", self.PARENTS, 2)
        assert got == {"A": 1, "B": 1, "C": 2, "D": 2}

    def test_unlimited(self):
        got = find_ancestors("T", self.PARENTS, None)
        assert got == {"A": 1, "B": 1, "C": 2, "D": 2}

    def test_shortest_path_depth(self):
        # C достижим через A (2) и B (2); кратчайший — 2.
        assert find_ancestors("T", self.PARENTS, None)["C"] == 2

    def test_zero_depth_empty(self):
        assert find_ancestors("T", self.PARENTS, 0) == {}

    def test_trigger_excluded(self):
        assert "T" not in find_ancestors("T", self.PARENTS, None)

    def test_unique_nodes(self):
        # Узел, достижимый несколькими путями, в результате один раз.
        got = find_ancestors("T", self.PARENTS, None)
        assert len(got) == len(set(got))
