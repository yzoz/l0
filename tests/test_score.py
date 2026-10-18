"""Тесты формулы transfer_score и расчёта score кандидата.

Score транзакции t (см. .docs/TODO_003.md):
    score(t) = internal_score(t) + SUM( internal_score(child) / 3 )
      по прямым детям t. Внуки не участвуют. Порог: score >= 1.

``internal_score = transfer_score(amount)`` precomputed consumer'ом.
``candidate_score`` — чистая арифметика поверх сохранённых значений.
"""

import pytest

from service.confirmer import CHILD_DIVISOR, SCORE_THRESHOLD, candidate_score
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


class TestCandidateScore:
    """Арифметика объединения precomputed internal_score.

    candidate_score(parent_internal, children_internal) — НЕ вызывает
    transfer_score, только суммирует и делит на CHILD_DIVISOR.
    """

    def test_divisor_is_three(self):
        # Делитель — фиксированный литерал 3 (число родителей), не config.
        assert CHILD_DIVISOR == 3

    def test_no_children_score_is_parent_internal(self):
        # Без детей score = parent_internal (не больше 1, т.к. transfer_score < 1).
        assert candidate_score(0.5, []) == pytest.approx(0.5)
        assert candidate_score(0.99, []) < SCORE_THRESHOLD

    def test_children_contribute_divided_by_three(self):
        # Вклад каждого ребёнка делится ровно на 3.
        score = candidate_score(0.6, [0.9])
        assert score == pytest.approx(0.6 + 0.9 / 3)

    def test_multiple_children_sum(self):
        # Все дети суммируются, каждый со своим /3.
        score = candidate_score(0.1, [0.3, 0.6, 0.9])
        assert score == pytest.approx(0.1 + 0.3 / 3 + 0.6 / 3 + 0.9 / 3)

    def test_score_can_reach_threshold(self):
        # Порог достижим: родитель с крупным internal_score + один крупный ребёнок.
        parent = transfer_score(100_000, alpha, beta)   # ≈ 0.86
        child = transfer_score(50_000, alpha, beta)     # ≈ 0.80
        assert candidate_score(parent, [child]) >= SCORE_THRESHOLD

    def test_small_amounts_below_threshold(self):
        # Мелкие суммы не дают прохода: score < 1.
        parent = transfer_score(100, alpha, beta)       # ≈ 0.22
        children = [transfer_score(100, alpha, beta)] * 3
        assert candidate_score(parent, children) < SCORE_THRESHOLD

    def test_many_children_accumulate(self):
        # Много детей накапливают score за счёт суммирования вкладов.
        parent = transfer_score(50_000, alpha, beta)    # ≈ 0.80
        children = [transfer_score(50_000, alpha, beta)] * 10
        assert candidate_score(parent, children) >= SCORE_THRESHOLD

    def test_direct_children_only_nonlinearity(self):
        # Формула принимает precomputed internal_score — нелинейность перенесена
        # в transfer_score. Один крупный ребёнок и три поменьше дают разный вклад.
        big = transfer_score(3000, alpha, beta) / 3
        three = 3 * (transfer_score(1000, alpha, beta) / 3)
        assert big != three  # неэквивалентны: transfer_score нелинейна
