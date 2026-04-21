# exchange-system

一个基于 `Go + go-zero + gRPC + Kafka` 的加密货币合约自动交易系统，核心链路是：

`Market -> Strategy -> Execution -> Order -> Gateway`

当前仓库包含：

- 行情采集与多周期 K 线聚合
- 趋势跟踪策略与收割路径风险过滤
- 执行路由、风控、模拟撮合
- 订单查询与本地 JSONL 归档
- HTTP Gateway 与 gRPC 服务编排

详细架构说明见 [architecture.md](file:///Users/bytedance/GolandProjects/exchange-system/architecture.md)。

## 服务说明
- `app/market/rpc`: 行情服务，负责 Binance WebSocket、K 线聚合、指标计算、Kafka 发布
- `app/strategy/rpc`: 策略服务，消费 K 线/深度数据并生成交易信号
- `app/execution/rpc`: 执行服务，负责风控、幂等、下单、模拟撮合、订单事件发布
- `app/order/rpc`: 订单服务，负责订单/成交/持仓查询和本地快照
- `app/api/gateway`: HTTP 网关，对外暴露账户、订单、策略控制接口
- `common/proto`: protobuf 定义
- `common/pb`: protobuf 生成代码
- `deploy`: Kafka/etcd/Redis 基础设施

## 运行依赖
- Go `1.25.6+`
- Docker / Docker Compose
- Kafka
- etcd
- Redis

基础设施编排文件在 [docker-compose.yml](file:///Users/bytedance/GolandProjects/exchange-system/deploy/docker-compose.yml)。

## 关键 Topic
- `kline`: 市场 K 线数据
- `depth`: 市场深度数据
- `signal`: 策略交易信号
- `harvest_path_signal`: 收割路径风险信号
- `order`: 订单执行结果

注意：当前代码实际使用的是上述 topic 名称；如果你在阅读旧文档时看到 `market_data`、`strategy_signals`、`order_events`，请以当前配置文件为准。

## 配置分层
本仓库现在提供三套显式 profile：

- `dev`: 本地开发/调试用，保留更宽松的调试参数
- `sim`: 本地模拟撮合/联调用，关闭策略调试跳过项
- `prod`: 面向真实运行的模板，默认关闭调试参数，敏感凭证留空

已新增的配置文件：

- `app/market/rpc/etc/market.dev.yaml`
- `app/market/rpc/etc/market.sim.yaml`
- `app/market/rpc/etc/market.prod.yaml`
- `app/strategy/rpc/etc/strategy.dev.yaml`
- `app/strategy/rpc/etc/strategy.sim.yaml`
- `app/strategy/rpc/etc/strategy.prod.yaml`
- `app/execution/rpc/etc/execution.dev.yaml`
- `app/execution/rpc/etc/execution.sim.yaml`
- `app/execution/rpc/etc/execution.prod.yaml`
- `app/order/rpc/etc/order.dev.yaml`
- `app/order/rpc/etc/order.sim.yaml`
- `app/order/rpc/etc/order.prod.yaml`

说明：

- 仓库里原有的 `market.yaml`、`strategy.yaml`、`execution.yaml`、`order.yaml` 仍保留，避免打断现有本地实验
- 推荐后续统一改用 `-f ...*.dev.yaml` / `-f ...*.sim.yaml` / `-f ...*.prod.yaml` 启动
- `prod` 配置中的 `APIKey`、`SecretKey`、代理等字段默认留空，需要通过你自己的安全方式注入

## 快速启动
### 1. 启动基础设施

```bash
cd deploy
docker-compose up -d
```

### 2. 启动模拟联调环境
推荐先使用 `sim` profile 跑通完整链路。

终端 1：

```bash
go run app/market/rpc/market.go -f app/market/rpc/etc/market.sim.yaml
```

终端 2：

```bash
go run app/strategy/rpc/strategy.go -f app/strategy/rpc/etc/strategy.sim.yaml
```

终端 3：

```bash
go run app/execution/rpc/execution.go -f app/execution/rpc/etc/execution.sim.yaml
```

终端 4：

```bash
go run app/order/rpc/order.go -f app/order/rpc/etc/order.sim.yaml
```

终端 5：

```bash
go run app/api/gateway/main.go -f app/api/gateway/etc/gateway.yaml
```

## 环境说明
### dev
- 适合本地调试
- `strategy.dev.yaml` 仍保留 `debug_skip_*`
- 适合快速验证信号是否能从 Kafka 流转到执行层

### sim
- 适合本地模拟撮合与回放
- 策略参数回到更接近真实运行态
- 执行层默认使用 `simulated`
- 不依赖真实交易所密钥也能完成主链路验证

### prod
- 作为真实运行模板
- 默认关闭 `debug_skip_*`
- `execution.prod.yaml` 默认切回真实 Binance endpoint 模板
- 所有敏感配置需自行注入，不建议把密钥写回仓库

## 常用命令
### 运行测试

```bash
go test ./...
```

### 回放历史 K 线

```bash
go run app/market/rpc/market.go \
  -f app/market/rpc/etc/market.sim.yaml \
  -replay app/market/rpc/data/kline/ETHUSDT/2026-04-11.jsonl
```

### Mock 生成行情

```bash
go run app/market/rpc/market.go \
  -f app/market/rpc/etc/market.dev.yaml \
  -mock-kafka \
  -mock-count=200
```

## 当前实现与架构文档的差异
- 架构文档偏“目标形态”，代码配置是“当前可运行形态”
- 当前真实 topic 名称是 `kline/depth/signal/order`
- 默认 `strategy.yaml` 仍可能带有调试参数，不建议直接当作生产配置
- `Order` 服务当前将本地 JSONL 视为“每日查询快照”，不是永远追加的事件日志
- `sim` profile 是当前最适合联调和回归验证的入口

## 数据目录说明
- `app/market/rpc/data/kline`: 行情与聚合 K 线日志
- `app/strategy/rpc/data/signal`: 策略信号日志
- `app/execution/rpc/data/order`: 执行结果日志
- `app/order/rpc/data/futures`: 订单服务生成的查询快照

## 注意事项
- 不要把真实交易所密钥提交到仓库
- `gateway` 当前更适合内网使用，未默认加鉴权
- 如果你要做真实运行，先确认 `proxy`、`Binance BaseURL`、topic、风控参数是否都已切到目标环境
- 如果你要重构核心逻辑，建议先补 `execution` 和 `order` 的链路测试

## 后续建议
- 先用 `sim` profile 跑通一遍完整链路
- 再补 `execution/order` 集成测试
- 再把默认配置逐步收敛到 profile 体系
- 最后再拆大文件，如 `trend_following.go` 和 `execution/internal/svc/serviceContext.go`