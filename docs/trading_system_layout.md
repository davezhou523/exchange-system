# 交易系统目录设计

## 目标

- 保持 go-zero 的应用入口风格
- 将 Strategy3 从单目录实现演进为可扩展领域结构
- 保留市场、策略、执行三个服务的拆分空间
- 兼容当前 CLI、API、模拟盘和 Binance 永续合约接入

## 推荐目录树

```text
exchange-system/
├── api/
│   ├── execution.proto
│   ├── market.proto
│   └── strategy.proto
├── apps/
│   ├── api/
│   │   └── strategy3api/
│   │       ├── etc/
│   │       ├── internal/
│   │       │   ├── config/
│   │       │   ├── handler/
│   │       │   └── svc/
│   │       └── main.go
│   └── rpc/
│       ├── market/
│       │   └── etc/
│       ├── strategy/
│       │   └── etc/
│       └── execution/
│           └── etc/
├── internal/
│   ├── strategy3/
│   │   ├── engine/
│   │   ├── exchange/
│   │   │   ├── binance/
│   │   │   └── paper/
│   │   ├── indicator/
│   │   ├── model/
│   │   └── runtime/
│   ├── market-service/
│   ├── strategy-service/
│   └── execution-service/
└── docs/
```

## 分层职责

### apps

- `apps/api/strategy3api` 负责 HTTP API 入口、配置装载、路由注册、ServiceContext
- `apps/rpc/*` 负责未来的 zrpc 服务配置与部署入口

### internal/strategy3

- `model` 放 Candle、Decision、Account、Position 等通用模型
- `indicator` 放 EMA、RSI、ATR 等独立算法
- `engine` 放 Strategy3 参数、状态机、入场出场和风控决策
- `exchange/binance` 放 Binance 永续合约 REST/WebSocket 适配器
- `exchange/paper` 放模拟账户、模拟订单、持仓同步
- `runtime` 放 bootstrap、订阅编排、快照组装、运行态调度

### internal/*-service

- 保留现有市场、策略、执行服务实现
- 作为未来迁移到 `apps/rpc/*` 的过渡层

## 迁移原则

- 统一把 Strategy3 实现放入 `internal/strategy3/`，不再保留平级策略源码目录
- 应用入口统一放在 `apps/api` 与 `apps/rpc`
- `apps/rpc/*` 先提供标准配置位置，后续再补 zrpc 生成代码与正式服务入口

## 当前落地

- `internal/strategy3/*` 已承载实际 Strategy3 源码
- `apps/rpc/*/etc` 已提供标准配置目录
- `apps/api/strategy3api` 已切到 `internal/strategy3/runtime` 作为运行时依赖入口
