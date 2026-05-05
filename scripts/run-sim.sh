#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${ROOT_DIR}/.run/sim"
PID_DIR="${RUN_DIR}/pids"
LOG_DIR="${RUN_DIR}/logs"

mkdir -p "${PID_DIR}" "${LOG_DIR}"

SERVICES=(
  "market|app/market/rpc/market.go|app/market/rpc/etc/market.sim.yaml"
  "execution|app/execution/rpc/execution.go|app/execution/rpc/etc/execution.sim.yaml"
  "order|app/order/rpc/order.go|app/order/rpc/etc/order.sim.yaml"
  "gateway|app/api/gateway/main.go|app/api/gateway/etc/gateway.yaml"
)

log() {
  printf '[run-sim] %s\n' "$*"
}

service_pid_file() {
  local name="$1"
  printf '%s/%s.pid' "${PID_DIR}" "${name}"
}

service_log_file() {
  local name="$1"
  printf '%s/%s.log' "${LOG_DIR}" "${name}"
}

is_pid_running() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

read_pid() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    tr -d '[:space:]' < "${pid_file}"
  fi
}

start_infra() {
  log "启动基础设施"
  if command -v docker-compose >/dev/null 2>&1; then
    (cd "${ROOT_DIR}/deploy" && docker-compose up -d)
    return
  fi
  if command -v docker >/dev/null 2>&1; then
    (cd "${ROOT_DIR}/deploy" && docker compose up -d)
    return
  fi
  log "未找到 docker-compose 或 docker compose"
  exit 1
}

start_service() {
  local name="$1"
  local entry="$2"
  local config="$3"
  local pid_file
  local log_file
  local existing_pid

  pid_file="$(service_pid_file "${name}")"
  log_file="$(service_log_file "${name}")"
  existing_pid="$(read_pid "${pid_file}")"

  if is_pid_running "${existing_pid}"; then
    log "${name} 已在运行 pid=${existing_pid}"
    return
  fi

  rm -f "${pid_file}"
  log "启动 ${name}"
  (
    cd "${ROOT_DIR}"
    nohup go run "${entry}" -f "${config}" >>"${log_file}" 2>&1 &
    echo $! > "${pid_file}"
  )

  sleep 2
  local new_pid
  new_pid="$(read_pid "${pid_file}")"
  if ! is_pid_running "${new_pid}"; then
    log "${name} 启动失败，请检查日志 ${log_file}"
    exit 1
  fi
  log "${name} 已启动 pid=${new_pid} log=${log_file}"
}

stop_service() {
  local name="$1"
  local pid_file
  local pid

  pid_file="$(service_pid_file "${name}")"
  pid="$(read_pid "${pid_file}")"

  if ! is_pid_running "${pid}"; then
    rm -f "${pid_file}"
    log "${name} 未运行"
    return
  fi

  log "停止 ${name} pid=${pid}"
  kill "${pid}" 2>/dev/null || true

  for _ in {1..20}; do
    if ! is_pid_running "${pid}"; then
      break
    fi
    sleep 0.5
  done

  if is_pid_running "${pid}"; then
    log "${name} 未在预期时间内退出，发送 SIGKILL"
    kill -9 "${pid}" 2>/dev/null || true
  fi

  rm -f "${pid_file}"
  log "${name} 已停止"
}

status_service() {
  local name="$1"
  local pid_file
  local pid

  pid_file="$(service_pid_file "${name}")"
  pid="$(read_pid "${pid_file}")"

  if is_pid_running "${pid}"; then
    printf '%-10s running  pid=%s  log=%s\n' "${name}" "${pid}" "$(service_log_file "${name}")"
    return
  fi
  printf '%-10s stopped\n' "${name}"
}

start_all() {
  start_infra
  for item in "${SERVICES[@]}"; do
    IFS='|' read -r name entry config <<<"${item}"
    start_service "${name}" "${entry}" "${config}"
  done
  log "模拟环境启动完成"
  log "查看状态: ${BASH_SOURCE[0]} status"
}

stop_all() {
  for (( idx=${#SERVICES[@]}-1; idx>=0; idx-- )); do
    IFS='|' read -r name _ <<<"${SERVICES[idx]}"
    stop_service "${name}"
  done
  log "模拟环境已停止"
}

status_all() {
  for item in "${SERVICES[@]}"; do
    IFS='|' read -r name _ <<<"${item}"
    status_service "${name}"
  done
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [start|stop|restart|status]

Commands:
  start    启动 sim 环境的基础设施和全部服务
  stop     停止全部服务
  restart  重启全部服务
  status   查看全部服务状态
EOF
}

cmd="${1:-start}"

case "${cmd}" in
  start)
    start_all
    ;;
  stop)
    stop_all
    ;;
  restart)
    stop_all
    start_all
    ;;
  status)
    status_all
    ;;
  *)
    usage
    exit 1
    ;;
esac