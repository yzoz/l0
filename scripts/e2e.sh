#!/usr/bin/env bash
# Сквозной (E2E) прогон конвейера L0.
#
#   make genesis → сервисы (cons/conf/write + api) → tx_bot → осаждение → верификатор
#
# Верификатор — tests/test_e2e_pipeline.py (поадресная согласованность Redis==Mongo,
# сохранение суммы / отсутствие двойного кредита, минт ровно один раз).
#
# Использование:
#   make e2e                 # полный прогон
#   TX_T=400 TX_C=2 TX_N=3 make e2e   # больше транзакций
# Предполагается, что Docker-сервисы подняты (make up) и зависимости установлены.

set -euo pipefail
cd "$(dirname "$0")/.."

export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

LOGDIR="${LOGDIR:-/tmp/opencode}"
mkdir -p "$LOGDIR"

# Объём генерации транзакций (t c n → t*c*n транзакций).
TX_T="${TX_T:-200}"
TX_C="${TX_C:-2}"
TX_N="${TX_N:-2}"
SETTLE_TIMEOUT="${SETTLE_TIMEOUT:-180}"

PIDS=()
cleanup() {
  echo "[e2e] приборка сервисов..."
  for p in "${PIDS[@]:-}"; do
    kill "$p" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "[e2e] genesis (сброс MongoDB/Redis/Memgraph + pre-load)..."
poetry run python genesis.py

echo "[e2e] старт сервисов (прод-режим, run.py без file-watching)..."
poetry run python run.py cons >"$LOGDIR/e2e-cons.log" 2>&1 & PIDS+=($!)
poetry run python run.py conf >"$LOGDIR/e2e-conf.log" 2>&1 & PIDS+=($!)
poetry run python run.py write >"$LOGDIR/e2e-write.log" 2>&1 & PIDS+=($!)
poetry run fastapi run api.py --host 127.0.0.1 --port 8000 >"$LOGDIR/e2e-api.log" 2>&1 & PIDS+=($!)
sleep 3

echo "[e2e] генерация транзакций: tx_bot $TX_T $TX_C $TX_N ..."
poetry run python tx_bot.py "$TX_T" "$TX_C" "$TX_N"

echo "[e2e] ожидание осаждения конвейера (до ${SETTLE_TIMEOUT}s)..."
deadline=$(( $(date +%s) + SETTLE_TIMEOUT ))
ok=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  if poetry run pytest -m integration tests/test_e2e_pipeline.py -q --no-header -p no:cacheprovider >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 5
done

if [ "$ok" -ne 1 ]; then
  echo "[e2e] FAIL: конвейер не сошёлся за ${SETTLE_TIMEOUT}s." >&2
  echo "[e2e] --- cons ---" >&2; tail -n 20 "$LOGDIR/e2e-cons.log" >&2 || true
  echo "[e2e] --- write ---" >&2; tail -n 20 "$LOGDIR/e2e-write.log" >&2 || true
  echo "[e2e] --- conf ---" >&2; tail -n 20 "$LOGDIR/e2e-conf.log" >&2 || true
  exit 1
fi

echo "[e2e] конвейер сошёлся. Финальная верификация:"
poetry run pytest -m integration tests/test_e2e_pipeline.py -v --no-header
echo "[e2e] OK"
