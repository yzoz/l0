"""Tip-selection для /pool.

Вместо жёсткого топ-3 по internal_score выбираем PARENTS родителей
взвешенным случайным образом.

Цель: сломать rich-get-richer.
Жирные tx (internal близко к 1.0) и так быстро подтверждаются
одним-двумя детьми. Мелкие и средние без целенаправленной помощи
никогда не дотягивают до порога и копятся в DAG.

Формула веса одного кандидата:

    need  = max(0, 1.0 - internal_score)     # сколько не хватает до 1.0
    age_sec = (now - t.data.time) / 1e6      # возраст в секундах
    fresh = 1 / (1 + age_sec / FRESH_HALF)   # 1.0 → 0.0

    weight = (need ** NEED_POWER) * (BASE + FRESH_BONUS * fresh)

- need ** NEED_POWER сильно предпочитает тех, кому далеко до порога
- BASE гарантирует, что даже очень старые tx сохраняют минимальный шанс
- FRESH_BONUS даёт небольшой приоритет относительно свежим
"""

import random
import time

from fastapi import APIRouter, Depends, Request

from src.config import PARENTS

router = APIRouter()

# ---------------------------------------------------------------------------
# Коэффициенты tip-selection
# ---------------------------------------------------------------------------

# Насколько агрессивно предпочитаем «нуждающихся».
# 1.0 = линейно, 1.5 = умеренно выпукло, 2.0 = сильно душит всё > 0.6
NEED_POWER = 1.5

# Базовая доля веса, которую сохраняет даже самый древний tx.
# 0.7 = старый получает минимум 70% от своего need-веса.
BASE = 0.7

# Дополнительный бонус за свежесть (доля от 0 до 1).
# BASE + FRESH_BONUS должно быть = 1.0, чтобы свежий tx получал полный вес.
FRESH_BONUS = 0.3

# Полураспад свежести в секундах.
# Через FRESH_HALF секунд fresh ≈ 0.5, дальше быстро падает к 0.
FRESH_HALF = 30.0


async def get_memgraph(request: Request):
    return request.app.state.graph.driver


@router.get("/pool")
async def pool(memgraph=Depends(get_memgraph)):
    async with memgraph.session() as session:
        result = await session.run(
            """
            MATCH (t:Transaction)
            RETURN t.hash AS hash,
                   t.internal_score AS score,
                   t.data.time AS ts
            """
        )
        rows = [
            {
                "hash": r["hash"],
                "score": float(r["score"] or 0.0),
                "ts": int(r["ts"] or 0),
            }
            async for r in result
        ]

    if not rows:
        return []

    now_us = int(time.time() * 1_000_000)
    weights = []
    for r in rows:
        need = max(0.0, 1.0 - r["score"])
        age_sec = max(0.0, (now_us - r["ts"]) / 1_000_000.0)
        fresh = 1.0 / (1.0 + age_sec / FRESH_HALF)
        w = (need ** NEED_POWER) * (BASE + FRESH_BONUS * fresh)
        weights.append(max(w, 1e-9))  # защита от нулевых весов

    # Выбираем PARENTS уникальных hash по весам
    chosen = []
    pool_idx = list(range(len(rows)))
    w = weights[:]

    for _ in range(min(PARENTS, len(rows))):
        idx = random.choices(pool_idx, weights=w, k=1)[0]
        chosen.append(rows[idx]["hash"])
        pos = pool_idx.index(idx)
        pool_idx.pop(pos)
        w.pop(pos)

    return chosen