#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-demo}"
SYMBOL="${2:-ETHUSDT}"

case "${PROFILE}" in
  demo|sim)
    ;;
  *)
    echo "Usage: $(basename "$0") [demo|sim] [SYMBOL]"
    exit 1
    ;;
esac

RUN_LOG_DIR="${ROOT_DIR}/.run/${PROFILE}/logs"
DECISION_DIR="${ROOT_DIR}/data/signal/decision/${SYMBOL}"
DECISION_FILE="${DECISION_DIR}/$(date +%F).jsonl"
STRATEGY_LOG="${RUN_LOG_DIR}/strategy.log"

mkdir -p "${RUN_LOG_DIR}" "${DECISION_DIR}"
touch "${STRATEGY_LOG}" "${DECISION_FILE}"

echo "[tail-strategy-decision] profile=${PROFILE} symbol=${SYMBOL}"
echo "[tail-strategy-decision] strategy log: ${STRATEGY_LOG}"
echo "[tail-strategy-decision] decision log: ${DECISION_FILE}"

exec tail -n 100 -F "${STRATEGY_LOG}" "${DECISION_FILE}"