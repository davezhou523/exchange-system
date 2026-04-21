#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/.run/demo/logs"

FILES=(
  "${LOG_DIR}/market.log"
  "${LOG_DIR}/execution.log"
  "${LOG_DIR}/order.log"
  "${LOG_DIR}/strategy.log"
  "${LOG_DIR}/gateway.log"
)

mkdir -p "${LOG_DIR}"

for file in "${FILES[@]}"; do
  touch "${file}"
done

echo "[tail-demo] following logs under ${LOG_DIR}"
exec tail -n 100 -F "${FILES[@]}"