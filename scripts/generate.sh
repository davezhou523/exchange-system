#!/bin/bash

set -e

echo "=== 开始生成交易所微服务代码 ==="

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# 定义服务列表
SERVICES=("matching" "account" "risk" "settlement")

# 1. 生成 RPC 服务
for service in "${SERVICES[@]}"; do
    echo "生成 ${service} RPC 服务..."

    # 检查 proto 文件是否存在
    PROTO_FILE="${PROJECT_ROOT}/rpc/${service}/${service}.proto"
    if [[ ! -f "${PROTO_FILE}" ]]; then
        echo "警告: ${PROTO_FILE} 不存在，跳过"
        continue
    fi

    # 生成代码
    goctl rpc protoc "${PROTO_FILE}" \
        --proto_path="${PROJECT_ROOT}" \
        --go_out="${PROJECT_ROOT}/rpc/${service}" \
        --go-grpc_out="${PROJECT_ROOT}/rpc/${service}" \
        --zrpc_out="${PROJECT_ROOT}/rpc/${service}" \
        --style=goZero
done

# 2. 生成 API 网关
echo "生成 API 网关..."
API_FILE="${PROJECT_ROOT}/api/exchange.api"
if [[ -f "${API_FILE}" ]]; then
    goctl api go -api "${API_FILE}" -dir "${PROJECT_ROOT}/api" -style goZero
else
    echo "警告: ${API_FILE} 不存在，跳过 API 生成"
fi

# 3. 生成模型
echo "生成数据模型..."
goctl model mysql datasource \
    -url="root:password@tcp(127.0.0.1:3306)/exchange" \
    -table="orders,trades,accounts,balance_changes" \
    -dir "${PROJECT_ROOT}/model" \
    -cache=true

echo "=== 代码生成完成 ==="
echo "请检查生成的代码结构，并根据需要修改业务逻辑"