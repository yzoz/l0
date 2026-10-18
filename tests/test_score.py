"""Тесты формулы transfer_score и расчёта score кандидата.

Score транзакции t (см. .docs/TODO_003.md):
    score(t) = transfer_score(amount(t))
             + SUM( transfer_score(amount(child)) / 3 )
      по прямым детям t. Внуки не участвуют. Порог: score >= 1.
"""

import pytest

from service.confirmer import CHILD_DIVISOR, SCORE_THRESHOLD, candidate_score
from src.utils.fn import alpha, beta, transfer_score


class TestTransferScore:
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
    def test_no_children_below_threshold(self):
        # Без детей score = transfer_score(own) < 1 при любом amount.
        for amount in (1, 100, 10_000, 10**9):
            assert candidate_score(amount, []) < SCORE_THRESHOLD

    def test_children_contribute_divided_by_three(self):
        # Вклад ребёнка делится ровно на CHILD_DIVISOR (=3), а не на config.
        assert CHILD_DIVISOR == 3
        own = transfer_score(50000, alpha, beta)
        child = transfer_score(50000, alpha, beta)
        score = candidate_score(50000, [50000])
        assert score == pytest.approx(own + child / 3)

    def test_direct_children_only(self):
        # Формула принимает только суммы прямых детей — никакой рекурсии нет.
        # Один большой ребёнок даёт вклад, три по трети — столько же суммарно.
        big = candidate_score(1000, [3000])
        three = candidate_score(1000, [1000, 1000, 1000])
        # Вклад: transfer_score(3000)/3 vs 3 * transfer_score(1000)/3 = transfer_score(1000).
        assert big != three  # неэквивалентны: transfer_score нелинейна

    def test_score_can_reach_threshold(self):
        # Порог достижим: родитель с крупным amount и один крупный ребёнок.
        assert candidate_score(100_000, [50_000]) >= SCORE_THRESHOLD

    def test_small_amounts_below_threshold(self):
        # Мелкие суммы не дают прохода: score < 1.
        assert candidate_score(100, [100, 100, 100]) < SCORE_THRESHOLD

    def test_many_children_accumulate(self):
        # Много детей накапливают score за счёт суммирования вкладов.
        children = [50_000] * 10
        assert candidate_score(50_000, children) >= SCORE_THRESHOLD
