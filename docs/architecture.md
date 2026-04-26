# 实盘盈利系统架构图

> **项目名称**: exchange-system  
> **技术栈**: go-zero + gRPC + Kafka + etcd + Redis  
> **核心功能**: 加密货币合约交易自动化策略系统  

---

## 一、系统总览图

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          客户端层                                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐                                │
│  │   Web Dashboard  │  │   API Client     │  │   Mobile App     │                                │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘                                │
│           │                     │                     │                                          │
│           └─────────────────────┴─────────────────────┘                                          │
│                                 │                                                                │
│                              HTTP/REST                                                           │
└─────────────────────────────────┼────────────────────────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────┼────────────────────────────────────────────────────────────────┐
│                                 ▼                     接入层                                      │
│                    ┌─────────────────────────┐                                                   │
│                    │    API Gateway          │  go-zero HTTP Gateway                             │
│                    │    (gateway.yaml)       │  路由分发: 账户/订单/策略/状态                     │
│                    └────┬──┬──┬──┬───────────┘                                                   │
│                         │  │  │  │                                                              │
│                    ┌────┘  │  │  └─────┐                                                        │
│                    ▼       ▼  ▼        ▼                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ gRPC
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       微服务层                                                     │
│                                                                                                  │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐ │
│  │  Market Service    │  │  Strategy Service  │  │  Execution Service │  │   Order Service    │ │
│  │  (market.yaml)     │  │ (strategy.demo/    │  │  (execution.yaml)  │  │  (order.yaml)      │ │
│  │                    │  │  strategy.prod)    │  │                    │  │                    │ │
│  │                    │  │                    │  │                    │  │                    │ │
│  │ • WebSocket Client │  │ • Trend Following  │  │ • Exchange Router  │  │ • Order Query      │ │
│  │ • Kline Aggregator │  │ • HarvestPath LSTM │  │ • Risk Manager     │  │ • Position Query   │ │
│  │ • Indicators       │  │ • Signal Stream    │  │ • Order Manager    │  │ • Income History   │ │
│  │ • Depth Stream     │  │ • Kafka Consumer   │  │ • Position Manager │  │ • Funding Fees     │ │
│  └────────┬───────────┘  └────────┬───────────┘  └────────┬───────────┘  └────────┬───────────┘ │
│           │                      │                      │                      │               │
└───────────┼──────────────────────┼──────────────────────┼──────────────────────┼───────────────┘
            │                      │                      │                      │
            ▼                      ▼                      ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                      消息中间件层                                                  │
│                                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────┐    │
│  │                              Apache Kafka 3.8.1                                          │    │
│  │                                                                                          │    │
│  │  Topics:                                                                                 │    │
│  │  • market_data          → Market Service 发布聚合K线数据                                  │    │
│  │  • strategy_signals     → Strategy Service 发布交易信号                                   │    │
│  │  • order_events         → Execution Service 发布订单状态事件                              │    │
│  │  • execution_commands   → Strategy Service 向 Execution Service 发送执行命令               │    │
│  └──────────────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
            │                      │                      │                      │
            ▼                      ▼                      ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                      数据存储层                                                    │
│                                                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐         │
│  │      etcd        │  │      Redis       │  │  JSONL Files     │  │  LSTM Models     │         │
│  │                  │  │                  │  │                  │  │                  │         │
│  │ • 服务注册发现   │  │ • 幂等控制       │  │ • K线日志        │  │ • .pt 模型文件   │         │
│  │ • 配置中心       │  │ • 订单状态缓存   │  │ • 信号日志       │  │ • .json 配置     │         │
│  │ • 分布式锁       │  │ • 持仓状态       │  │ • 持仓记录       │  │ • Python 训练    │         │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘  └──────────────────┘         │
│                                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                     外部交易所 API                                                 │
│                                                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐                                                      │
│  │    Binance       │  │      OKX         │                                                      │
│  │                  │  │                  │                                                      │
│  │ • REST API       │  │ • REST API       │                                                      │
│  │ • WebSocket      │  │ • WebSocket      │                                                      │
│  │ • 合约交易       │  │ • 合约交易       │                                                      │
│  └──────────────────┘  └──────────────────┘                                                      │
│                                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、核心业务流程图

### 2.1 市场数据流 (Market Data Pipeline)

```
┌─────────────┐
│  Binance    │ WebSocket 原始行情
│  Exchange   │────────────────────────┐
└─────────────┘                        │
                                       ▼
                          ┌─────────────────────────┐
                          │  Market Service          │
                          │                          │
                          │  1. WebSocket Client     │
                          │     接收原始K线数据       │
                          │                          │
                          │  2. Kline Aggregator     │
                          │     聚合为多周期K线       │
                          │     • 1m (基准)          │
                          │     • 3m                 │
                          │     • 5m                 │
                          │     • 15m                │
                          │     • 1h                 │
                          │     • 4h                 │
                          │                          │
                          │  3. Technical Indicators │
                          │     计算技术指标          │
                          │     • EMA21 / EMA55      │
                          │     • RSI (14)           │
                          │     • ATR (14)           │
                          │                          │
                          │  4. Watermark 机制       │
                          │     • EmitWatermark 模式 │
                          │     • EmitImmediate 模式 │
                          │     • 延迟容忍: allowedLateness │
                          │                          │
                          │  5. History Warmup       │
                          │     预热历史数据(4500根) │
                          └────────┬─────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │        Kafka Topic           │
                    │    market_data               │
                    └────────┬─────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────┐
          │       Strategy Service 消费          │
          └──────────────────────────────────────┘
```

### 2.2 策略交易信号流 (Strategy Signal Pipeline)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Strategy Service                                   │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                        多周期趋势跟踪策略                                │  │
│  │                                                                        │  │
│  │   第1层: 4H趋势判断                                                     │  │
│  │   ┌──────────────────────────────────────────────────────────────┐     │  │
│  │   │  多头趋势: 价格 > EMA21 > EMA55                               │     │  │
│  │   │  空头趋势: 价格 < EMA21 < EMA55                               │     │  │
│  │   │  震荡: 不交易                                                  │     │  │
│  │   └──────────────────────────────────────────────────────────────┘     │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │   第2层: 1H回调确认                                                    │  │
│  │   ┌──────────────────────────────────────────────────────────────┐     │  │
│  │   │  多头回调: EMA21 > 价格 > EMA55 + RSI∈[42,60]                │     │  │
│  │   │  空头回调: EMA21 < 价格 < EMA55 + RSI∈[40,58]                │     │  │
│  │   │  深回调: |价格-EMA55|/EMA55 ≤ 0.3%                           │     │  │
│  │   └──────────────────────────────────────────────────────────────┘     │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │   第3层: 15M入场信号                                                   │  │
│  │   ┌──────────────────────────────────────────────────────────────┐     │  │
│  │   │  结构突破: 收盘价突破近N根K线高低点                            │     │  │
│  │   │  RSI信号: RSI穿越50中线 或 达到偏置阈值                        │     │  │
│  │   │  模式: OR (默认) / AND                                        │     │  │
│  │   └──────────────────────────────────────────────────────────────┘     │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │   风控层: Risk Management                                              │  │
│  │   ┌──────────────────────────────────────────────────────────────┐     │  │
│  │   │  • 连续亏损限制 (max_consecutive_losses)                      │     │  │
│  │   │  • 日亏损限制 (max_daily_loss_pct)                            │     │  │
│  │   │  • 最大回撤限制 (max_drawdown_pct)                            │     │  │
│  │   │  • 最大同时持仓数 (max_positions)                             │     │  │
│  │   │  • HarvestPath LSTM 风险过滤 (可选)                           │     │  │
│  │   └──────────────────────────────────────────────────────────────┘     │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │   仓位计算: Position Sizing                                            │  │
│  │   ┌──────────────────────────────────────────────────────────────┐     │  │
│  │   │  1. 风险仓位 = (权益 × 风险比例) ÷ (ATR × 止损倍数)           │     │  │
│  │   │  2. 现金限制 = (权益 × 最大仓位比例) ÷ 价格                   │     │  │
│  │   │  3. 杠杆限制 = (权益 × 杠杆 × 最大杠杆使用率) ÷ 价格          │     │  │
│  │   │  4. 基础仓位 = min(风险仓位, 现金限制, 杠杆限制)              │     │  │
│  │   │  5. 最终仓位 = 基础仓位 × 回撤缩放                            │     │  │
│  │   └──────────────────────────────────────────────────────────────┘     │  │
│  │                              │                                         │  │
│  └──────────────────────────────┼─────────────────────────────────────────┘  │
│                                 │                                            │
│                                 ▼                                            │
│                    ┌────────────────────────┐                                │
│                    │  发送交易信号到 Kafka   │                                │
│                    │  Topic: strategy_signals│                                │
│                    └────────────────────────┘                                │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                        信号模式切换                                     │  │
│  │                                                                        │  │
│  │  signal_mode = 0 (默认): 15m信号模式                                    │  │
│  │  • 入场/出场周期: 15m K线                                               │  │
│  │  • 判断依据: 4H趋势 + 1H回调 + 15M入场                                  │  │
│  │                                                                        │  │
│  │  signal_mode = 1: 1m信号模式                                            │  │
│  │  • 入场/出场周期: 1m K线                                                │  │
│  │  • 判断依据: 4H趋势 + 1H回调 + 1M入场                                   │  │
│  │  • 1m_trading_paused = 1: 暂停1m成交，降级到15m模式                     │  │
│  │                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 订单执行流 (Order Execution Pipeline)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Execution Service                                  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                          订单执行流程                                    │  │
│  │                                                                        │  │
│  │  1. 接收信号                                                            │  │
│  │     ┌──────────────────────────────────────────────────────────────┐   │  │
│  │     │  • Kafka: strategy_signals topic                              │   │  │
│  │     │  • gRPC: CreateOrder RPC                                      │   │  │
│  │     └──────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │  2. 风控校验 (Risk Manager)                                            │  │
│  │     ┌──────────────────────────────────────────────────────────────┐   │  │
│  │     │  • 仓位限制检查                                                │   │  │
│  │     │  • 资金风险校验                                                │   │  │
│  │     │  • 交易频率控制                                                │   │  │
│  │     │  • 符号/方向/数量验证                                          │   │  │
│  │     └──────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │  3. 幂等控制 (Idempotent)                                              │  │
│  │     ┌──────────────────────────────────────────────────────────────┐   │  │
│  │     │  • Redis 记录 client_order_id                                 │   │  │
│  │     │  • 防止重复下单                                                │   │  │
│  │     │  • TTL: 24h                                                   │   │  │
│  │     └──────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │  4. 交易所路由 (Exchange Router)                                       │  │
│  │     ┌──────────────────────────────────────────────────────────────┐   │  │
│  │     │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │   │  │
│  │     │  │   Binance    │  │     OKX      │  │  Simulated   │       │   │  │
│  │     │  │  Interface   │  │  Interface   │  │  Interface   │       │   │  │
│  │     │  └──────────────┘  └──────────────┘  └──────────────┘       │   │  │
│  │     └──────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │  5. 订单管理 (Order Manager)                                           │  │
│  │     ┌──────────────────────────────────────────────────────────────┐   │  │
│  │     │  • 跟踪订单生命周期                                            │   │  │
│  │     │  • 状态机: NEW→FILLED/CANCELED/REJECTED                       │   │  │
│  │     │  • 订单日志记录                                                │   │  │
│  │     └──────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │  6. 仓位管理 (Position Manager)                                        │  │
│  │     ┌──────────────────────────────────────────────────────────────┐   │  │
│  │     │  • 更新本地持仓状态                                            │   │  │
│  │     │  • 计算盈亏                                                    │   │  │
│  │     │  • 同步交易所实际仓位                                          │   │  │
│  │     └──────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │  7. 发布订单事件                                                        │  │
│  │     ┌──────────────────────────────────────────────────────────────┐   │  │
│  │     │  • Kafka: order_events topic                                  │   │  │
│  │     │  • Order Service 消费用于查询                                  │   │  │
│  │     └──────────────────────────────────────────────────────────────┘   │  │
│  │                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.4 完整交易闭环时序图

```
┌────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Client │  │ Gateway  │  │  Market  │  │ Strategy │  │Execution │  │ Exchange │
└───┬────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
    │            │             │             │             │             │
    │ 请求启动策略│             │             │             │             │
    │───────────>│             │             │             │             │
    │            │  gRPC调用   │             │             │             │
    │            │────────────>│             │             │             │
    │            │             │             │             │             │
    │            │             │ WebSocket连接│             │             │
    │            │             │<────────────────────────────────────────│
    │            │             │             │             │             │
    │            │             │ 推送K线数据 │             │             │
    │            │             │────────────>│             │             │
    │            │             │             │             │             │
    │            │             │    Kafka: market_data    │             │
    │            │             │─────────────────────────>│             │
    │            │             │             │             │             │
    │            │             │  策略判断   │             │             │
    │            │             │  4H+1H+15M  │             │             │
    │            │             │  三层过滤   │             │             │
    │            │             │             │             │             │
    │            │             │  生成交易信号│             │             │
    │            │             │             │             │             │
    │            │             │  Kafka: strategy_signals │             │
    │            │             │─────────────────────────>│             │
    │            │             │             │             │             │
    │            │             │             │   风控校验  │             │
    │            │             │             │   幂等控制  │             │
    │            │             │             │   交易所路由│             │
    │            │             │             │             │             │
    │            │             │             │  下单请求   │             │
    │            │             │             │────────────>│             │
    │            │             │             │             │             │
    │            │             │             │  订单回报   │             │
    │            │             │             │<────────────│             │
    │            │             │             │             │             │
    │            │             │  Kafka: order_events     │             │
    │            │             │<─────────────────────────│             │
    │            │             │             │             │             │
    │  查询订单状态│             │             │             │             │
    │<───────────│             │             │             │             │
    │            │             │             │             │             │
    │            │             │  持续监控持仓│             │             │
    │            │             │  止损/止盈检查│             │             │
    │            │             │             │             │             │
```

---

## 三、技术架构详情

### 3.1 微服务架构

| 服务名称 | 配置文件 | 端口类型 | 主要功能 |
|---------|---------|---------|---------|
| **API Gateway** | gateway.yaml | HTTP REST | 路由分发、请求聚合、认证鉴权 |
| **Market Service** | market.yaml | gRPC | 行情数据聚合、技术指标计算、WebSocket客户端 |
| **Strategy Service** | strategy.demo.yaml / strategy.prod.yaml | gRPC | 策略引擎、信号生成、风控管理 |
| **Execution Service** | execution.yaml | gRPC | 订单执行、交易所路由、仓位管理 |
| **Order Service** | order.yaml | gRPC | 订单查询、历史记录、收益统计 |

### 3.2 数据流矩阵

| 数据流 | 来源 | 目标 | 协议 | Topic/RPC |
|-------|------|------|------|-----------|
| 市场数据 | Binance WebSocket | Market Service | WebSocket | - |
| 聚合K线 | Market Service | Kafka | Kafka | market_data |
| 策略信号 | Strategy Service | Kafka | Kafka | strategy_signals |
| 订单事件 | Execution Service | Kafka | Kafka | order_events |
| 下单命令 | Strategy Service | Execution Service | gRPC | CreateOrder |
| 账户查询 | API Gateway | Execution Service | gRPC | GetAccountInfo |
| 订单查询 | API Gateway | Order Service | gRPC | GetOpenOrders |

### 3.3 核心组件说明

#### 3.3.1 Kline Aggregator (K线聚合器)

```
输入: 1分钟原始K线 (Binance WebSocket)
输出: 多周期聚合K线 (1m/3m/5m/15m/1h/4h)

特性:
• Watermark 机制: 保证数据完整性
• 允许延迟: allowedLateness 容忍迟到数据
• 历史预热: 启动时加载4500根历史K线
• 异步发送: Kafka发送与数据处理解耦
• 指标计算: EMA/RSI/ATR 独立按周期计算
```

#### 3.3.2 Strategy Engine (策略引擎)

```
架构: 多时间周期趋势跟踪 (4H + 1H + 15M/1M)

信号模式:
• signal_mode=0: 15m信号模式 (默认)
• signal_mode=1: 1m信号模式
• 1m_trading_paused=1: 暂停1m成交，降级到15m模式

风控:
• 连续亏损限制
• 日亏损限制
• 最大回撤限制
• 最大持仓数限制
• HarvestPath LSTM风险过滤 (可选)
```

#### 3.3.3 多币种策略调度架构

```
目标: 在同一套 market -> strategy -> execution -> order 主链路中并行运行多个交易对

当前交易对:
• ETHUSDT
• BTCUSDT
• BNBUSDT
• SOLUSDT
• XRPUSDT
```

按 symbol 路由:

```text
Market Service
-> Kafka(kline)
-> Strategy Consumer
-> 按 kline.Symbol 路由
-> strategies[symbol]
-> 对应 TrendFollowingStrategy 实例
```

说明:
- 同一个 `kline` topic 中可同时承载多个 symbol 的 K线
- `strategy` 消费后按 `symbol` 从实例表中选择对应策略
- 某个 symbol 没有注册策略时，该消息会被直接忽略，不影响其他交易对

策略实例隔离:

```text
每个 symbol 拥有独立的:
• latest4h / latest1h / latest15m / latest1m 快照
• 持仓状态
• 决策状态
• 信号输出
• 风控状态
```

说明:
- `ETHUSDT` 的趋势判断不会污染 `BTCUSDT`
- `SOLUSDT` 的持仓状态不会影响 `XRPUSDT`
- 多币种共用一个服务进程，但运行时状态按 symbol 隔离

日志隔离:

```text
market:
  data/kline/{symbol}/{interval}/{date}.jsonl

strategy:
  data/kline/{symbol}/{interval}/{date}.jsonl
  data/signal/{symbol}/{date}.jsonl
  data/signal/decision/{symbol}/{date}.jsonl
```

说明:
- 所有本地日志天然按 `symbol` 分目录
- 排查时可以直接按 symbol 横向对齐：
  market kline -> strategy kline -> decision -> signal
- 多币种不会把日志混写到同一个文件里

风控隔离:

```text
每个 symbol 独立评估:
• 趋势是否成立
• 回调是否成立
• 入场是否触发
• Harvest Path 是否阻断
• 是否已有持仓
```

说明:
- 决策层按 symbol 隔离
- 某个币种被 `h4_not_ready` 挡住，不会影响其他币种
- 某个币种被 `harvest_path_block` 阻断，也不会阻塞其他币种继续交易

后续扩展建议:

```text
1. 参数分币种配置
   目前多个 symbol 可共用同一组参数，后续建议支持 symbol 级单独调参

2. 风控分层
   增加组合级风控与单币种风控双层控制，避免多币种同时放大风险

3. 执行容量评估
   多币种同时发信号时，需要评估 execution / order 的吞吐与幂等处理能力

4. 指标与日志汇总
   增加按 symbol 的运行看板、告警和健康度统计

5. 动态上下线
   后续可支持不重启服务动态增减 symbol 和策略实例
```

#### 3.3.4 多币种上线 Checklist

目标: 新增或批量启用 symbol 时，确保 `market -> strategy -> execution -> order` 主链路稳定接入。

上线前配置检查:

```text
1. market 配置
   app/market/rpc/etc/market.demo.yaml
   app/market/rpc/etc/market.dev.yaml
   app/market/rpc/etc/market.sim.yaml
   -> Binance.Symbols 中加入新 symbol

2. strategy 配置
   app/strategy/rpc/etc/strategy.demo.yaml
   app/strategy/rpc/etc/strategy.prod.yaml
   -> Strategies 中增加对应 symbol 的策略实例

3. execution / order
   通常不需要为 symbol 单独加白名单
   但需要确认交易所侧支持该合约，且精度/下单规则可正常查询
```

推荐上线顺序:

```text
1. 先改 market 的 Symbols
2. 再改 strategy 的 Strategies
3. 重启 market
4. 等 market 开始产出 1m
5. 重启 strategy
6. 确认 decision / signal 开始按 symbol 分目录写入
7. 最后观察 execution / order 是否收到对应 symbol 的后续链路
```

需要重启的服务:

```text
必须重启:
• market
• strategy

按需观察:
• execution
• order
```

第一眼确认生效的日志:

```text
market:
• Connecting to Binance WebSocket: ... btcusdt@kline_1m ...
• [ws stats] total_messages=...
• [1m kline] BTCUSDT ...
• [aggregated] symbol=BTCUSDT interval=15m ...

strategy:
• [kafka consume] symbol=BTCUSDT interval=1m ...
• [decision][SKIP]/[BLOCK]/[SIGNAL] symbol=BTCUSDT ...
```

按 symbol 的落盘确认:

```text
market:
app/market/rpc/data/kline/{symbol}/1m/{date}.jsonl
app/market/rpc/data/kline/{symbol}/15m/{date}.jsonl
app/market/rpc/data/kline/{symbol}/1h/{date}.jsonl
app/market/rpc/data/kline/{symbol}/4h/{date}.jsonl

strategy:
app/strategy/rpc/data/kline/{symbol}/1m/{date}.jsonl
app/strategy/rpc/data/kline/{symbol}/15m/{date}.jsonl
app/strategy/rpc/data/kline/{symbol}/1h/{date}.jsonl
app/strategy/rpc/data/kline/{symbol}/4h/{date}.jsonl
app/strategy/rpc/data/signal/decision/{symbol}/{date}.jsonl
app/strategy/rpc/data/signal/{symbol}/{date}.jsonl
```

如何确认每个 symbol 的 1m / 15m / 1h / 4h 都开始产出:

```text
1. 先确认 1m
   看 market 侧 1m 文件是否出现新 symbol 目录和当天 jsonl

2. 再确认 15m
   等跨过一个 15m 周期后，看 market 侧 15m 是否开始写入

3. 再确认 1h / 4h
   看 market 侧是否产生对应高周期 jsonl
   同时看 isDirty / isFinal / dirtyReason

4. 再确认 strategy
   看 strategy/data/kline/{symbol}/...
   看 decision jsonl 是否开始出现该 symbol
```

建议命令:

```bash
# 以 BTCUSDT 为例，先看 market 侧 1m
sed -n '1,20p' app/market/rpc/data/kline/BTCUSDT/1m/$(date -u +%F).jsonl

# 看 market 侧 15m / 1h / 4h
sed -n '1,20p' app/market/rpc/data/kline/BTCUSDT/15m/$(date -u +%F).jsonl
sed -n '1,20p' app/market/rpc/data/kline/BTCUSDT/1h/$(date -u +%F).jsonl
sed -n '1,20p' app/market/rpc/data/kline/BTCUSDT/4h/$(date -u +%F).jsonl

# 看 strategy 侧是否消费到
sed -n '1,20p' app/strategy/rpc/data/kline/BTCUSDT/1m/$(date -u +%F).jsonl

# 看 decision / signal
sed -n '1,20p' app/strategy/rpc/data/signal/decision/BTCUSDT/$(date -u +%F).jsonl
sed -n '1,20p' app/strategy/rpc/data/signal/BTCUSDT/$(date -u +%F).jsonl
```

上线成功判定:

```text
1. market 已订阅新 symbol
2. market 的 1m 文件开始持续写入
3. market 的 15m/1h/4h 开始生成
4. strategy 开始消费该 symbol
5. decision 日志开始出现该 symbol
6. 若条件满足，signal 开始出现该 symbol
```

常见遗漏:

```text
• 只改了 market，没改 strategy
• 改了 strategy.demo.yaml，但实际跑的是 strategy.prod.yaml
• market 有 15m，但 strategy 侧因非 final 不落盘，被误判为“没消费到”
• 新 symbol 已有 1m，但还没跨到 15m / 1h / 4h 周期边界，被误判为“没生成”
• 代理有问题，导致新旧 symbol 都无流
```

#### 3.3.5 Universe 冷启动判读标准

适用场景:

```text
strategy 刚重启，Universe.Enabled=true，
需要快速判断当前是“正常冷启动观察期”，还是“Universe 实际上已经卡死”。
```

建议先看:

```text
app/strategy/rpc/data/signal/universe/_meta/{date}.jsonl
```

当前 _meta 关键字段:

```text
• candidate_count
• enabled_count
• bootstrap_observe_count
• healthy_count
• stale_count
• switch_count
• action_counts
• reason_counts
```

Universe 明细日志关键字段:

```text
路径:
app/strategy/rpc/data/signal/universe/{symbol}/{date}.jsonl

核心字段:
• base_template
  该 symbol 的基础静态模板
  例如:
  BTCUSDT -> btc-core
  SOLUSDT -> high-beta

• template
  Universe 当前这一轮最终决定使用的目标模板
  例如:
  BTCUSDT -> btc-trend
  SOLUSDT -> high-beta-safe

• action
  本轮实际动作
  例如: enable / disable / switch / keep / bootstrap_observe

• reason
  触发动作的原因
  例如: healthy_data / trend_strong / volatility_high / volatility_extreme

理解方式:
• 如果 base_template = template
  表示当前仍在使用基础模板
• 如果 base_template != template
  表示当前被 Universe 动态切到了另一套模板
```

正常冷启动:

```text
启动后的前 5 分钟内，如果看到：
• bootstrap_observe_count > 0
• stale_count 较高
• healthy_count 还很低

这通常是正常现象，表示 Universe 正在观察，但还没拿到足够的新 1m 快照。
```

正在恢复:

```text
如果随着时间推进出现：
• bootstrap_observe_count 下降
• healthy_count 上升
• enabled_count 上升
• switch_count 开始出现
• action_counts 中开始出现 enable / keep

说明 Universe 已经开始从冷启动观察期进入正常接管状态。
```

异常卡死:

```text
如果超过 BootstrapDuration 后仍长期出现：
• enabled_count = 0
• healthy_count = 0
• stale_count 持续很高
• reason_counts 主要是 no_snapshot / stale_data

则更可能不是冷启动，而是 market -> Kafka -> strategy 的输入链路没有真正恢复。
```

一眼判断口诀:

```text
前 5 分钟 bootstrap_observe 高：正常观察
5 分钟后 healthy_count 开始上升：正常恢复
switch_count > 0：开始发生动态模板切换
5 分钟后 stale_count 仍长期很高：异常卡死
```

建议命令:

```bash
cd /Users/bytedance/GolandProjects/exchange-system

# 看 Universe 总览
sed -n '1,20p' app/strategy/rpc/data/signal/universe/_meta/$(date -u +%F).jsonl

# 看某个 symbol 的明细
sed -n '1,20p' app/strategy/rpc/data/signal/universe/BTCUSDT/$(date -u +%F).jsonl

# 只看模板切换相关字段
grep '"base_template"\|"template"\|"action"\|"reason"' \
  app/strategy/rpc/data/signal/universe/BTCUSDT/$(date -u +%F).jsonl
```

排查顺序:

```text
1. 先看 _meta 的 bootstrap_observe_count / healthy_count / stale_count
2. 再看某个 symbol 的 universe 明细 jsonl
3. 再确认 strategy 是否有新的 1m 消费
4. 若仍无快照，再回到 market / Kafka 链路排查
```

#### 3.3.6 Market UniversePool 判读标准

适用场景:

```text
market 开启 UniversePool.Enabled=true，
需要快速判断动态币池状态机是否真的在推进，
以及当前是“有真实新流”，还是“只是缓存里还留着旧快照”。
```

建议先看:

```text
1. market 终端日志中的 [universepool] evaluate ...
2. app/market/rpc/data/universepool/_meta/{date}.jsonl
```

当前 _meta 关键字段:

```text
• global_state
• trend_count
• range_count
• breakout_count
• candidate_count
• snapshot_count
• fresh_count
• stale_count
• inactive_count
• pending_add_count
• warming_count
• active_count
• pending_remove_count
• cooldown_count
• state_counts
```

终端关键日志:

```text
[universepool] evaluate global_state=range candidates=5 snapshots=5 fresh=3 stale=2 inactive=0 pending_add=0 warming=1 active=4 pending_remove=0 cooldown=0

[universepool] state symbol=SOLUSDT from=inactive to=warming reason=score_pass warmup_ready=false incomplete=missing_15m_history
```

UniversePool selector 决策日志:

```text
除了状态迁移日志外，当前还会为每个动态币写 selector 决策快照：

app/market/rpc/data/universepool/{symbol}/{date}.jsonl

其中 action=selector_decision 的记录可直接看出：
• 当前全局状态是什么
• 这个 symbol 本轮是被放行还是被过滤
• 对应 score 和 reason 是什么
```

怎么理解 snapshot / fresh / stale:

```text
• snapshot_count
  表示当前缓存里总共有多少个 symbol 的快照记录

• fresh_count
  表示其中仍在新鲜窗口内、可被 selector 当成实时输入的 symbol 数量

• stale_count
  表示其中已经过期、不能再视为实时输入的 symbol 数量

关键区别:
• snapshot_count 高，不代表当前还有实时流
• 只有 fresh_count 高，才更说明当前真的还有新流在持续进入
• global_state 只有在 fresh snapshot 足够时才有意义
```

怎么理解 global_state / trend_count / range_count / breakout_count:

```text
• global_state
  当前轮 UniversePool selector 推断出的全局市场状态
  例如: trend / range / breakout / unknown

• trend_count / range_count / breakout_count
  当前 fresh candidate 中，分别命中三类最小状态规则的数量

关键区别:
• global_state=unknown 且三个 count 都为 0
  更像是“当前规则完全没命中”

• global_state=unknown 但某个 count > 0
  更像是“有局部命中，但还没形成多数”
```

怎么理解 selector_decision / state_preferred_score_pass / state_filtered:

```text
路径:
app/market/rpc/data/universepool/{symbol}/{date}.jsonl

核心字段:
• action=selector_decision
  表示这不是状态迁移，而是 selector 本轮决策快照

• global_state
  当前 UniversePool 正在跟随的全局状态

• reason=state_preferred_score_pass
  表示这个 symbol 属于当前状态的偏好币组，并且 score 已过 AddScoreThreshold

• reason=state_filtered
  表示当前已经进入状态驱动筛选，但该 symbol 不属于当前状态的偏好币组

• reason=score_pass
  表示当前仍在 fallback 路径，更多是“健康币直接通过”，
  而不是已进入明确的状态偏好筛选

• reason=stale_snapshot / no_snapshot
  表示当前问题更偏输入链路或快照新鲜度，而不是状态规则本身
```

正常启动:

```text
启动后如果看到：
• snapshot_count 开始增长
• warming_count > 0
• 某些 symbol 从 inactive -> warming

说明动态币池已经拿到真实 1m 输入，并开始推进状态机。
```

正常状态驱动选币:

```text
如果同时看到：
• _meta 中 global_state=range / trend / breakout
• 对应 count 字段不再全 0
• symbol 明细里出现 action=selector_decision
• 某些 symbol 出现 state_preferred_score_pass
• 某些 symbol 出现 state_filtered

说明 UniversePool 已经开始按市场结构做最小版状态驱动选币。
```

正常 warmup:

```text
如果看到：
• warming_count > 0
• state 日志里出现 warmup_ready=false
• incomplete=missing_15m_history / missing_1h_history / indicators_not_ready

这通常不是故障，而是 symbol 仍在正常预热。
```

进入 active:

```text
如果看到：
• 某个 symbol 从 warming -> active
• warmup_ready=true
• active_count 上升

说明该 symbol 已通过最小 warmup 要求，进入可用状态。
```

异常停流:

```text
如果看到：
• snapshot_count 仍然存在
• 但 fresh_count 持续下降、stale_count 持续上升
• 同时 ws stats 中 total_messages=0 / closed_1m_messages=0

则更像是“缓存里还有旧快照”，而不是“当前真的还有新流”。
```

状态规则未命中:

```text
如果看到：
• snapshot_count > 0
• fresh_count > 0
• 但 global_state=unknown
• 且 trend_count=0 / range_count=0 / breakout_count=0

则更像是当前 fresh snapshot 仍未命中最小状态规则，
而不是 UniversePool 没拿到输入。
```

一眼判断口诀:

```text
snapshot_count 高：说明曾经收到过
fresh_count 高：说明现在仍有新流
warming_count > 0：说明状态机正在预热
active_count 上升：说明 warmup 已开始转化为可用状态
stale_count 持续升高：说明更像停流或上游断流
global_state + count 不再全空：说明状态驱动选币开始落地
state_filtered 出现：说明非偏好币开始被动态池压制
```

建议命令:

```bash
cd /Users/bytedance/GolandProjects/exchange-system

# 看 UniversePool 总览
sed -n '1,20p' app/market/rpc/data/universepool/_meta/$(date -u +%F).jsonl

# 看某个 symbol 的状态变化
sed -n '1,20p' app/market/rpc/data/universepool/SOLUSDT/$(date -u +%F).jsonl

# 看 selector 决策快照
grep '"action":"selector_decision"' \
  app/market/rpc/data/universepool/{BNBUSDT,SOLUSDT,XRPUSDT}/$(date -u +%F).jsonl

# 只看关键聚合字段
grep '"global_state"\|"trend_count"\|"range_count"\|"breakout_count"\|"candidate_count"\|"snapshot_count"\|"fresh_count"\|"stale_count"\|"warming_count"\|"active_count"' \
  app/market/rpc/data/universepool/_meta/$(date -u +%F).jsonl
```

排查顺序:

```text
1. 先看终端 [universepool] evaluate 的 fresh / stale / warming / active
2. 再看 _meta 的 global_state / trend_count / range_count / breakout_count
3. 再看某个 symbol 的 selector_decision reason / score / desired
4. 再看某个 symbol 的 state 日志和 incomplete 原因
5. 若 fresh_count 持续为 0，再回头排查 websocket / proxy / 上游消息流
```

#### 3.3.7 Dynamic Evolution Roadmap

当前项目的动态化路线建议按下面 5 个阶段逐步推进：

```text
Phase 1: UniverseSelector
  解决“某个 symbol 当前要不要参与交易”

Phase 2: Template Switch
  解决“同一个 symbol 当前该用哪套模板”

Phase 3: Dynamic Pool
  解决“market 侧当前要不要真正订阅这个 symbol 的实时流”

Phase 4: Market State Engine
  解决“当前市场整体和局部处于什么状态”

Phase 5: Strategy Weight Engine
  解决“不同策略/模板/币种当前应该分配多少权重和风险预算”
```

这 5 个阶段的职责边界：

```text
UniverseSelector        -> 决定 symbol 是否启用
Template Switch         -> 决定 symbol 应使用的模板
Dynamic Pool            -> 决定 symbol 是否纳入实时订阅宇宙
Market State Engine     -> 决定当前市场所处 regime
Strategy Weight Engine  -> 决定不同策略与币种的资金权重
```

建议理解顺序：

```text
Phase 1 / 2 / 3 解决的是“做谁、怎么做、要不要订阅”
Phase 4 / 5 解决的是“为什么现在这样做、应该给多大权重”
```

#### 3.3.8 Phase 4: Market State Engine

核心目标:

```text
把系统从“按单个 symbol 局部健康度决策”
升级到“先判断市场状态，再驱动策略、模板和选币”
```

建议新增位置:

```text
Market Features -> Market State Engine -> UniverseSelector / Template Switch / Strategy Weight Engine
```

最小输入特征:

```text
• EMA21 / EMA55
• ATR 与 ATR 相对价格比例
• RSI
• 1m / 15m / 1h / 4h 的健康度
• volume / quoteVolume（后续可扩展）
• 可选: ADX（若后续补充）
```

建议输出状态:

```text
• TrendUp
• TrendDown
• Range
• Breakout
• Unknown
```

最小判断思路:

```text
1. 先看数据是否健康，不健康则输出 Unknown
2. EMA21 > EMA55 且 ATR/price 不过高 -> TrendUp
3. EMA21 < EMA55 且 ATR/price 不过高 -> TrendDown
4. ATR/price 很低 -> Range
5. ATR/price 很高 -> Breakout
```

与现有模块的衔接:

```text
• UniverseSelector
  不再只看 symbol 自己的健康度，也可读取 MarketState 决定是否启用

• Template Switch
  可根据 MarketState 做 template regime 切换
  例如:
  BTCUSDT 在 TrendUp 时更倾向 btc-trend
  高 beta 币在 Breakout 时可切到 high-beta-safe 或直接禁用

• Dynamic Pool
  可根据 MarketState 决定当前更偏向纳入哪些候选币
  例如:
  Range 时偏保守，只保留 BTC / ETH
  Breakout 时允许 SOL / BNB / XRP 进入 warming
```

建议新增的工程形态:

```text
app/strategy/rpc/internal/marketstate/
或
app/market/rpc/internal/marketstate/
```

最小输出形式:

```go
type MarketState string

const (
    MarketStateTrendUp   MarketState = "trend_up"
    MarketStateTrendDown MarketState = "trend_down"
    MarketStateRange     MarketState = "range"
    MarketStateBreakout  MarketState = "breakout"
    MarketStateUnknown   MarketState = "unknown"
)
```

最小代码目录建议:

```text
app/strategy/rpc/internal/marketstate/
├── state.go        # MarketState 枚举与结果结构
├── detector.go     # 状态识别主逻辑
├── features.go     # 输入特征定义与快照组装
├── logger.go       # state jsonl / _meta 日志
└── detector_test.go
```

最小结构体草案:

```go
type Features struct {
    Symbol        string
    Timeframe     string
    Close         float64
    Ema21         float64
    Ema55         float64
    Atr           float64
    AtrPct        float64
    Rsi           float64
    Healthy       bool
    LastReason    string
    UpdatedAt     time.Time
}

type Result struct {
    Symbol      string
    State       MarketState
    Confidence  float64
    Reason      string
    UpdatedAt   time.Time
}
```

最小接口草案:

```go
type Detector interface {
    Detect(now time.Time, features Features) Result
}

type SnapshotProvider interface {
    GetFeatures(symbol string) (Features, bool)
}
```

建议方法签名:

```go
func NewDetector(cfg Config) *DefaultDetector
func (d *DefaultDetector) Detect(now time.Time, features Features) Result
func BuildFeaturesFromSnapshotValues(symbol, timeframe string, close, ema21, ema55, atr float64, isDirty, isTradable, isFinal bool, updatedAt time.Time) Features
```

接入点建议:

```text
1. strategy 侧：
   在 UniverseSelector 评估前先拿 MarketState

2. market 侧：
   可选地把 state 作为 universepool selector 的上游信号

3. 日志侧：
   建议新增:
   app/strategy/rpc/data/signal/marketstate/{symbol}/{date}.jsonl
```

Phase 4 日志判读标准:

```text
适用场景:
strategy 已接入 MarketStateEngine，
需要快速判断每个 symbol 当前被识别成什么 regime，
以及这些状态识别是否足够健康、是否开始真正影响模板切换。
```

建议先看:

```text
1. app/strategy/rpc/data/signal/marketstate/_meta/{date}.jsonl
2. app/strategy/rpc/data/signal/marketstate/{symbol}/{date}.jsonl
```

当前 _meta 关键字段:

```text
• candidate_count
• result_count
• healthy_count
• unknown_count
• state_counts
• reason_counts
```

MarketState 明细日志关键字段:

```text
路径:
app/strategy/rpc/data/signal/marketstate/{symbol}/{date}.jsonl

核心字段:
• state
  当前识别出的 market state
  例如: trend_up / trend_down / range / breakout / unknown

• confidence
  当前状态识别的置信度

• reason
  当前状态识别的主要原因
  例如: ema_bull_alignment / atr_pct_high / stale_features

• healthy
  输入特征是否健康

• last_reason
  若输入不健康，对应的原始原因
  例如: dirty_data / not_final / no_snapshot
```

正常识别:

```text
如果看到：
• healthy_count > 0
• state_counts 中开始稳定出现 trend_up / range / breakout
• symbol 明细里 reason 与特征方向一致

说明 MarketStateEngine 已经开始输出可解释的状态结果。
```

正常影响模板切换:

```text
如果同时看到：
• marketstate/BTCUSDT 中 state=trend_up 或 trend_down
• universe/BTCUSDT 中 reason=market_state_trend

说明 Phase 4 已经开始真实影响 Phase 2 的模板切换。
```

异常或无效识别:

```text
如果看到：
• unknown_count 长期很高
• reason_counts 主要是 stale_features / missing_trend_features
• symbol 明细里 healthy=false

则更像是输入特征不足或快照不够新鲜，
而不是状态引擎本身识别出了明确的市场 regime。
```

一眼判断口诀:

```text
healthy_count 高：说明输入可用
state_counts 稳定：说明状态机开始成形
unknown_count 高：说明数据还不够好
universe reason=market_state_trend：说明 Phase 4 已开始影响 Phase 2
```

建议命令:

```bash
cd /Users/bytedance/GolandProjects/exchange-system

# 看 MarketState 总览
sed -n '1,20p' app/strategy/rpc/data/signal/marketstate/_meta/$(date -u +%F).jsonl

# 看 BTC 的状态识别明细
sed -n '1,20p' app/strategy/rpc/data/signal/marketstate/BTCUSDT/$(date -u +%F).jsonl

# 对照看 BTC 的 Universe 模板切换
grep '"reason"\|"template"\|"base_template"' \
  app/strategy/rpc/data/signal/universe/BTCUSDT/$(date -u +%F).jsonl
```

排查顺序:

```text
1. 先看 _meta 的 healthy_count / unknown_count / state_counts
2. 再看某个 symbol 的 state / confidence / reason
3. 再对照 universe 明细是否出现 market_state_trend
4. 若长期 unknown，再回头检查 1m 快照的新鲜度和特征完整性
```

工程建议:

```text
第一版先保持状态数很少，不要一开始就做过细的 regime 分类，
否则容易把系统做成“高频抖动 + 难解释”。
```

#### 3.3.9 Phase 5: Strategy Weight Engine

核心目标:

```text
不是只选一个策略或一个模板，
而是根据当前 MarketState、symbol score 和风控预算，
动态分配“策略权重 + 币种权重 + 风险权重”。
```

推荐分层:

```text
MarketState
  -> Strategy Weights
  -> Symbol Selection
  -> Position Budget
  -> Execution
```

最小输入:

```text
• MarketState
• UniverseSelector 产出的 symbol 集合
• Template Switch 产出的 symbol -> template
• symbol score（来自 dynamic pool / selector）
• 风控预算（单日止损、最大回撤、loss streak 等）
```

最小输出:

```text
• strategy_weight
• template_weight
• symbol_weight
• risk_scale
• trading_paused
```

建议的最小权重逻辑:

```text
TrendUp:
• 趋势模板权重提高
• high-beta-safe 权重降低

Range:
• 保守模板权重提高
• 高 beta 权重下降

Breakout:
• breakout / high-beta 权重提高
• 同时收紧风险上限，避免插针时过度暴露
```

与现有模块的衔接:

```text
• UniverseSelector
  先决定启用哪些 symbol

• Template Switch
  决定 symbol 的策略风格

• Strategy Weight Engine
  再决定：
  1. 哪类模板当前更重
  2. 哪些 symbol 分到更多风险预算
  3. 是否整体降低仓位或暂停交易
```

可以直接复用的现有风控能力:

```text
• max_consecutive_losses
• max_daily_loss_pct
• max_drawdown_pct
• drawdown_position_scale
• 1m_trading_paused
```

推荐新增的系统级控制:

```text
• market_pause_until
• strategy_class_weight
• symbol_risk_cap
• max_active_symbols
• high_beta_bucket_cap
```

一条推荐公式:

```text
position_budget
  = strategy_weight
  * symbol_weight
  * risk_scale
```

最小代码目录建议:

```text
app/strategy/rpc/internal/weights/
├── engine.go        # 权重计算主逻辑
├── types.go         # 输入/输出结构体
├── allocator.go     # 仓位预算分配
├── logger.go        # weights jsonl / _meta 日志
└── engine_test.go
```

最小结构体草案:

```go
type Inputs struct {
    MarketState      marketstate.Result
    Symbols          []string
    Templates        map[string]string
    SymbolScores     map[string]float64
    RiskScale        float64
    LossStreak       int
    DailyLossPct     float64
    DrawdownPct      float64
}

type Recommendation struct {
    Symbol             string
    Template           string
    StrategyWeight     float64
    SymbolWeight       float64
    RiskScale          float64
    PositionBudget     float64
    TradingPaused      bool
    PauseReason        string
}

type Output struct {
    Recommendations []Recommendation
    MarketPaused    bool
    MarketPauseReason string
    UpdatedAt       time.Time
}
```

最小接口草案:

```go
type Engine interface {
    Evaluate(now time.Time, in Inputs) Output
}
```

建议方法签名:

```go
func NewEngine(cfg Config) *DefaultEngine
func (e *DefaultEngine) Evaluate(now time.Time, in Inputs) Output
func (e *DefaultEngine) computeRiskScale(in Inputs) float64
func (e *DefaultEngine) allocateBudgets(in Inputs) []Recommendation
```

接入点建议:

```text
1. UniverseSelector / Template Switch 之后
   先确定 symbol 和 template

2. Signal 生成之前
   再由 weights engine 输出权重建议

3. TrendFollowingStrategy 内部
   根据 PositionBudget 调整下单数量或风险系数
```

建议落盘路径:

```text
app/strategy/rpc/data/signal/weights/_meta/{date}.jsonl
app/strategy/rpc/data/signal/weights/{symbol}/{date}.jsonl
```

Signal 日志联动观察:

```text
Phase 5 当前不仅会单独写 weights jsonl，
还会把最新一轮权重建议快照附加到 strategy 的本地 signal jsonl 中，
用于把“建议值”和“真实发出的信号”并排对照。

路径:
app/strategy/rpc/data/signal/{symbol}/{date}.jsonl

注意:
• 这里新增的是本地 signal 日志字段
• 当前不会修改 Kafka signal 负载
• 也不会影响 execution 输入协议
```

Phase 5 日志判读标准:

```text
适用场景:
strategy 已接入 Strategy Weight Engine，
需要快速判断当前轮到底给了哪些 symbol 权重建议、
是否被风险规则整体压缩、以及当前权重建议是否已经和 Phase 2 / Phase 4 对齐。
```

建议先看:

```text
1. app/strategy/rpc/data/signal/weights/_meta/{date}.jsonl
2. app/strategy/rpc/data/signal/weights/{symbol}/{date}.jsonl
3. strategy 终端日志中的 [weights] evaluate ...
4. app/strategy/rpc/data/signal/{symbol}/{date}.jsonl 中的 weights 字段
```

当前 _meta 关键字段:

```text
• symbol_count
• recommendation_count
• paused_count
• market_paused
• market_pause_reason
• market_state
• template_counts
```

Weights 明细日志关键字段:

```text
路径:
app/strategy/rpc/data/signal/weights/{symbol}/{date}.jsonl

核心字段:
• template
  当前这个 symbol 对应的模板

• strategy_weight
  当前市场状态下，策略类别获得的基础权重

• symbol_weight
  当前 symbol 在同一轮中的归一化权重

• risk_scale
  当前风险缩放系数

• position_budget
  当前轮建议的最终预算
  计算方式:
  strategy_weight * symbol_weight * risk_scale

• trading_paused / pause_reason
  当前建议是否整体暂停，以及暂停原因
```

Signal jsonl 里的 weights 字段:

```text
路径:
app/strategy/rpc/data/signal/{symbol}/{date}.jsonl

核心字段:
• weights.template
• weights.strategy_weight
• weights.symbol_weight
• weights.risk_scale
• weights.position_budget
• weights.trading_paused
• weights.pause_reason

理解方式:
• weights jsonl
  看的是“这一轮权重引擎给了什么建议”

• signal jsonl 的 weights
  看的是“这条真实信号发出时，当时附带的最新建议值是什么”
```

终端关键日志:

```text
[weights] evaluate symbols=3 recommendations=3 paused=false market_state=trend_up
```

正常建议输出:

```text
如果看到：
• recommendation_count > 0
• market_paused = false
• symbol 明细里 position_budget > 0

说明 Phase 5 已经开始输出真实可用的权重建议。
```

正常与真实信号对齐:

```text
如果同时看到：
• weights/BTCUSDT 中 position_budget > 0
• signal/BTCUSDT 中同样出现 weights.position_budget
• signal/BTCUSDT 的 weights.template 与 universe 当前 template 一致

说明 Phase 5 不只是单独输出建议，
而是已经开始能和真实 signal 做并排对照。
```

正常与 Phase 4 / Phase 2 对齐:

```text
如果同时看到：
• marketstate/_meta 中 market state 偏 trend_up / trend_down
• universe/BTCUSDT 中 reason=market_state_trend
• weights/_meta 中 market_state=trend_up 或 trend_down
• weights/BTCUSDT 中 template=btc-trend

说明 Phase 4 -> Phase 2 -> Phase 5 这条链已经开始对齐。
```

风险压缩生效:

```text
如果看到：
• risk_scale 明显下降
• paused_count 上升
• market_paused=true
• market_pause_reason=risk_limit_triggered

说明当前不是信号层问题，而是 Phase 5 在主动压缩风险预算。
```

异常或无效输出:

```text
如果看到：
• symbol_count > 0 但 recommendation_count = 0
• 或 weights 明细长期为空
• 或 template_counts 与 universe 当前模板明显不一致
• 或 weights jsonl 有值，但 signal jsonl 长期没有 weights 字段

则更像是权重引擎输入没有接好，
或 signal 本地日志还没有接到最新建议快照，
而不是当前轮真的没有预算建议。
```

一眼判断口诀:

```text
recommendation_count > 0：说明 Phase 5 已在输出
market_state 与 marketstate/_meta 对齐：说明上游状态已接通
template_counts 与 universe 对齐：说明模板输入已接通
risk_scale 下降 / market_paused=true：说明风险控制开始介入
position_budget > 0：说明权重建议已经可用于后续仓位预算
signal jsonl 出现 weights：说明建议值已开始贴近真实信号
```

建议命令:

```bash
cd /Users/bytedance/GolandProjects/exchange-system

# 看 Weights 总览
sed -n '1,20p' app/strategy/rpc/data/signal/weights/_meta/$(date -u +%F).jsonl

# 看 BTC 的权重建议
sed -n '1,20p' app/strategy/rpc/data/signal/weights/BTCUSDT/$(date -u +%F).jsonl

# 看 BTC 的真实 signal 以及附带的 weights 快照
sed -n '1,20p' app/strategy/rpc/data/signal/BTCUSDT/$(date -u +%F).jsonl

# 对照看 BTC 的 marketstate 和 universe
sed -n '1,20p' app/strategy/rpc/data/signal/marketstate/BTCUSDT/$(date -u +%F).jsonl
sed -n '1,20p' app/strategy/rpc/data/signal/universe/BTCUSDT/$(date -u +%F).jsonl
```

排查顺序:

```text
1. 先看 _meta 的 recommendation_count / market_paused / market_state
2. 再看某个 symbol 的 position_budget / risk_scale / template
3. 再看 signal jsonl 是否已经带出 weights 快照
4. 再对照 marketstate 和 universe，确认状态与模板输入是否一致
5. 若长期无建议，再回头检查 Phase 4 输出和 Universe enabled symbol 是否为空
```

工程建议:

```text
Phase 5 第一版不要直接改 execution 下单逻辑，
更稳的做法是先输出“权重建议”，
再由 strategy 在生成 signal 时把权重折算成 position size。
```

#### 3.3.10 Execution Engine (执行引擎)

```
流程: 信号接收 → 风控校验 → 幂等控制 → 交易所路由 → 下单 → 状态跟踪

交易所支持:
• Binance (币安)
• OKX (欧易)
• Simulated (模拟交易)

特性:
• 智能路由
• 幂等下单 (Redis)
• 订单状态机
• 仓位管理
```

### 3.4 部署架构

```
┌─────────────────────────────────────────────────────────────┐
│                        Docker Compose                        │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Gateway    │  │   Market    │  │  Strategy   │          │
│  │   Service    │  │   Service   │  │  Service    │          │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘          │
│         │                │                │                  │
│  ┌──────┴────────────────┴────────────────┴──────┐          │
│  │              gRPC + Kafka Bus                  │          │
│  └──────┬────────────────┬────────────────┬──────┘          │
│         │                │                │                  │
│  ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐          │
│  │    Order    │  │  Execution  │  │    Redis    │          │
│  │   Service   │  │   Service   │  │    Cache    │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │    Kafka    │  │    etcd     │  │ AI/ML Models │          │
│  │   Cluster   │  │   Service   │  │   (.pt/.json)│          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### 3.5 基础设施配置

| 组件 | 端口 | 用途 | 配置 |
|------|------|------|------|
| Kafka | 9092/19092/29092/39092 | 消息队列 | KRaft模式, replication_factor=1 |
| etcd | 2379/2380 | 服务注册发现 | 单节点, data_dir=/etcd-data |
| Redis | 6379 | 缓存/幂等控制 | AOF持久化, appendonly=yes |

### 3.6 Kafka Topics

| Topic | 生产者 | 消费者 | 消息类型 | 说明 |
|-------|--------|--------|---------|------|
| market_data | Market Service | Strategy Service | Kline JSON | 聚合K线数据 |
| strategy_signals | Strategy Service | Execution Service | Signal JSON | 交易信号 |
| order_events | Execution Service | Order Service | Order JSON | 订单状态事件 |

---

## 四、策略核心逻辑

### 4.1 三层过滤机制

```
第1层: 4H趋势判断
┌─────────────────────────────────────────────────────┐
│  多头趋势: 价格 > EMA21 > EMA55                      │
│  空头趋势: 价格 < EMA21 < EMA55                      │
│  震荡: 不交易                                        │
└─────────────────────────────────────────────────────┘
                      │
                      ▼
第2层: 1H回调确认
┌─────────────────────────────────────────────────────┐
│  多头回调:                                           │
│  • 价格位置: EMA21 > 价格 > EMA55                    │
│  • RSI过滤: RSI ∈ [42, 60]                          │
│  • 结构完整: 价格 > 前低点                           │
│  • EMA趋势: EMA21持续上升                            │
│                                                      │
│  空头回调:                                           │
│  • 价格位置: EMA21 < 价格 < EMA55                    │
│  • RSI过滤: RSI ∈ [40, 58]                          │
│  • 结构完整: 价格 < 前高点                           │
│  • EMA趋势: EMA21持续下降                            │
└─────────────────────────────────────────────────────┘
                      │
                      ▼
第3层: 15M入场信号
┌─────────────────────────────────────────────────────┐
│  多头入场:                                           │
│  • 结构突破: 收盘价 > 近期高点                        │
│  • RSI信号: RSI > 50 且 前值 ≤ 50                    │
│  • RSI偏置: RSI ≥ 52                                │
│                                                      │
│  空头入场:                                           │
│  • 结构突破: 收盘价 < 近期低点                        │
│  • RSI信号: RSI < 50 且 前值 ≥ 50                    │
│  • RSI偏置: RSI ≤ 48                                │
└─────────────────────────────────────────────────────┘
```

### 4.2 止损止盈机制

```
止损: ATR × 1.5 动态止损
  • 多头: 入场价 - (ATR × 1.5)
  • 空头: 入场价 + (ATR × 1.5)
0.09801
止盈:
  • TP1: 1.5×ATR (第一止盈位)
  • TP2: 3×ATR (第二止盈位)

移动止损:
  • 达到TP1后: 止损移至入场价 (保本)
  • 继续追踪TP2

EMA破位出场:
  • 持仓 ≥ 5根K线后检查
  • 缓冲带: 0.3×ATR
  • 连续2根K线确认
```

### 4.3 仓位计算

```
1. 风险仓位 = (权益 × 风险比例) ÷ (ATR × 止损倍数)
   • 默认: equity=10000, risk_per_trade=3%

2. 现金限制 = (权益 × 最大仓位比例) ÷ 价格
   • 默认: max_position_size=55%

3. 杠杆限制 = (权益 × 杠杆 × 最大杠杆使用率) ÷ 价格
   • 默认: leverage=7, max_leverage_ratio=92%

4. 基础仓位 = min(风险仓位, 现金限制, 杠杆限制)

5. 最终仓位 = 基础仓位 × 回撤缩放
   • 回撤 ≥ 15% 时: 缩放系数 = 50%
```

---

## 五、风控体系

### 5.1 风险限制

| 风控项 | 默认值 | 说明 |
|-------|--------|------|
| 最大连续亏损 | 3次 | 超过后暂停交易 |
| 最大日亏损 | 7% | 日亏损达到后暂停 |
| 最大回撤 | 15% | 触发后仓位减半 |
| 最大持仓数 | 1 | 同时持仓数量限制 |
| 单笔风险 | 3% | 每笔交易风险占权益比例 |

### 5.2 HarvestPath LSTM 风险过滤

```
组件:
• 规则引擎: 基于1m K线结构识别收割路径风险
• LSTM模型: Python训练, PyTorch推理
• 订单簿分析: 实时盘口数据风险评分

参数:
• harvest_path_model_enabled: 0=关闭 | 1=开启
• harvest_path_block_threshold: 高风险阻断阈值 (默认0.80)
• harvest_path_lstm_enabled: LSTM模型开关
• harvest_path_book_enabled: 订单簿分析开关

工作流程:
1. 入场信号生成
2. HarvestPath风险评分
3. 如果 prob ≥ threshold: 阻塞入场
4. 否则: 正常执行
```

---

## 六、数据持久化

### 6.1 JSONL 日志文件

```
K线日志:
  路径: data/kline/{symbol}/{interval}/{date}.jsonl
  内容: 聚合K线数据 (包含EMA/RSI/ATR指标)

信号日志:
  路径: data/signal/{symbol}/{date}.jsonl
  内容: 策略交易信号 (入场/出场/原因/指标)

订单日志:
  路径: data/futures/{type}/{symbol}/{date}.jsonl
  类型: all_orders / positions
```

### 6.2 值班排错速查表

按下面顺序排查最快：

```text
market 输入
-> market 聚合输出
-> strategy 消费
-> strategy 决策
-> execution 执行
-> order 落单结果
```

#### 1. market 没有任何实时 K线

| 项 | 内容 |
|----|------|
| 现象 | `app/market/rpc/data/kline/ETHUSDT/...` 没文件，或只有 `warmup` |
| 第一眼看 | `market` 终端日志里的 `[ws stats]` |
| 第二眼看 | `app/market/rpc/data/kline/ETHUSDT/1m/*.jsonl` |
| 最可能根因 | WebSocket 连上但没收流，常见是代理或交易所实时流没有真正进来 |

#### 2. WebSocket 连上了，但一直没数据

| 项 | 内容 |
|----|------|
| 现象 | 有 `WebSocket connected successfully`，但连续多次 `[ws stats] total_messages=0 kline_messages=0 closed_1m_messages=0` |
| 第一眼看 | `market` 终端日志里的 `[ws stats]` |
| 第二眼看 | `app/market/rpc/etc/market.demo.yaml` 的 `Proxy` 配置，和 `internal/websocket/binance_client.go` |
| 最可能根因 | 代理只完成握手，但没持续转发 WebSocket 数据帧 |

#### 3. 有 1m，但没有 15m/1h/4h

| 项 | 内容 |
|----|------|
| 现象 | `1m` 文件有数据，但 `15m/1h/4h` 很少或没有 |
| 第一眼看 | `market` 终端日志里的 `[aggregated]` |
| 第二眼看 | `app/market/rpc/data/kline/ETHUSDT/15m/*.jsonl`、`1h/*.jsonl`、`4h/*.jsonl` |
| 最可能根因 | `1m` 输入不连续、还没跨周期刷桶、或被 gap / watermark / worker GC 干扰 |

#### 4. 高周期生成了，但都是 dirty

| 项 | 内容 |
|----|------|
| 现象 | `isDirty=true`、`isTradable=false`、`isFinal=false` |
| 第一眼看 | `dirtyReason` |
| 第二眼看 | 对应时间段的 `market` `1m` 文件 |
| 最可能根因 | `incomplete_bucket`、`worker_gc`、`watermark_timeout`、`gap_detected` |

#### 5. 为什么某根 15m 还没生成

| 项 | 内容 |
|----|------|
| 现象 | 已经看到 `12:23` 的 `1m`，但还没看到 `12:15` 的 `15m` |
| 第一眼看 | 最新 `1m` 到了几点 |
| 第二眼看 | `internal/aggregator/kline_aggregator.go` 的 flush 时机 |
| 最可能根因 | 还没跨到下一个 `15m` 周期；`12:15` 这桶要等 `12:30` 那根 `1m` 到来后才会刷出 |

#### 6. market 有 15m，但 strategy 本地 15m 没有

| 项 | 内容 |
|----|------|
| 现象 | `market/data/kline/.../15m/...` 有，`strategy/data/kline/.../15m/...` 没有 |
| 第一眼看 | 这根 `15m` 的 `isFinal` 和 `isDirty` |
| 第二眼看 | `app/strategy/rpc/internal/kafka/consumer.go` 的落盘规则 |
| 最可能根因 | `strategy` 对 `15m` 默认只落 `isFinal=true`，非 final 即使消费到了也不写文件 |

#### 7. strategy 为什么没发信号

| 项 | 内容 |
|----|------|
| 现象 | 有 `kline`，但没有 `signal` |
| 第一眼看 | `app/strategy/rpc/data/signal/decision/ETHUSDT/*.jsonl` |
| 第二眼看 | `reason_code` 和 `extras.summary` |
| 最可能根因 | `h4_not_ready`、`h1_not_ready`、`h4_no_trend`、`h1_no_pullback`、`m15_no_entry`、`harvest_path_block` |

#### 8. decision 显示 h4_not_ready / h1_not_ready

| 项 | 内容 |
|----|------|
| 现象 | 决策被跳过，理由是高周期指标未就绪 |
| 第一眼看 | `decision jsonl` 里的 `extras.summary`、`h4_dirty_reason`、`h1_dirty_reason` |
| 第二眼看 | `strategy/data/kline/ETHUSDT/1h/*.jsonl` 和 `4h/*.jsonl` |
| 最可能根因 | 上游高周期 dirty，指标仍为 0，或尚未拿到稳定闭K |

#### 9. decision 显示 h4_no_trend

| 项 | 内容 |
|----|------|
| 现象 | 数据和指标都有，但策略认定当前没趋势 |
| 第一眼看 | `decision jsonl` 的 `h4_ema21`、`h4_ema55`、`h4_close` |
| 第二眼看 | `strategy/data/kline/ETHUSDT/4h/*.jsonl` |
| 最可能根因 | 不是链路问题，而是趋势条件本身未满足 |

#### 10. decision 显示 harvest_path_block

| 项 | 内容 |
|----|------|
| 现象 | 原本接近可开仓，但最后被挡住 |
| 第一眼看 | `[decision][BLOCK]` 和 `reason_code=harvest_path_block` |
| 第二眼看 | `trend_following.go` 和 Harvest Path 相关配置 |
| 最可能根因 | 风险门控启用且风险分数超过阈值 |

#### 11. strategy 有 SIGNAL，但 execution 没动

| 项 | 内容 |
|----|------|
| 现象 | `signal jsonl` 有记录，但执行层没反应 |
| 第一眼看 | `execution` 的 consumer 日志 |
| 第二眼看 | `app/execution/rpc/internal/kafka/consumer.go`、`internal/svc/serviceContext.go` |
| 最可能根因 | execution 没启动，Kafka topic / group 配错，或 signal 没被消费到 |

#### 12. execution 有动作，但 order 没结果

| 项 | 内容 |
|----|------|
| 现象 | 执行层看起来已下发订单事件，但 `order` 没记录 |
| 第一眼看 | `order` 终端日志、consumer 启动日志、Kafka lag |
| 第二眼看 | `app/order/rpc/internal/kafka/consumer.go`、`internal/svc/serviceContext.go`、`data/futures` |
| 最可能根因 | `order` consumer 没消费到，或 Binance 查询失败 |

#### 13. Kafka 报 i/o timeout

| 项 | 内容 |
|----|------|
| 现象 | `consume loop error ... i/o timeout` |
| 第一眼看 | 后面是否有 `setup group=...`、`claims=...`、`lag=0` |
| 第二眼看 | Kafka broker 地址和本地网络状态 |
| 最可能根因 | 本地 Kafka 短暂抖动；若后面自动恢复，一般不是业务故障 |

#### 14. worker_gc 出现

| 项 | 内容 |
|----|------|
| 现象 | `dirtyReason` 里有 `worker_gc`，或日志有 `GC: cleaning idle worker` |
| 第一眼看 | `market` 的高周期文件和 `dirtyReason` |
| 第二眼看 | `internal/aggregator/kline_aggregator.go` 和 `WorkerGC` 配置 |
| 最可能根因 | 对应 symbol 超过 `30m` 没有新数据，worker 被空闲回收，残桶被强制刷出 |

#### 15. incomplete_bucket 出现

| 项 | 内容 |
|----|------|
| 现象 | `dirtyReason=incomplete_bucket` |
| 第一眼看 | 对应周期 K线的 `openTime` |
| 第二眼看 | 把该周期应有的 `1m` 一根一根对出来 |
| 最可能根因 | bucket 期望的 `1m` 数量不够，常见于启动晚了或中间缺分钟 |

### 6.3 常用命令速查

以下命令默认在项目根目录执行：

```bash
cd /Users/bytedance/GolandProjects/exchange-system
```

#### 1. 看 Kafka Topic 和消费积压

```bash
# 列出所有 topics
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# 查看某个 topic 的 offset 情况
docker exec kafka /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic kline

# 查看某个 consumer group 的 lag
docker exec kafka /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group strategy-rpc-kline \
  --describe
```

#### 2. 看 market / strategy / execution / order 启动日志

```bash
# market
cd /Users/bytedance/GolandProjects/exchange-system/app/market/rpc
go run market.go

# strategy
cd /Users/bytedance/GolandProjects/exchange-system/app/strategy/rpc
go run strategy.go

# execution
cd /Users/bytedance/GolandProjects/exchange-system/app/execution/rpc
go run execution.go

# order
cd /Users/bytedance/GolandProjects/exchange-system/app/order/rpc
go run order.go
```

#### 3. 看某天某周期的 K线 jsonl

```bash
# market 侧 1m
sed -n '1,20p' app/market/rpc/data/kline/ETHUSDT/1m/2026-04-25.jsonl

# market 侧 15m
sed -n '1,20p' app/market/rpc/data/kline/ETHUSDT/15m/2026-04-25.jsonl

# strategy 侧 1h
sed -n '1,20p' app/strategy/rpc/data/kline/ETHUSDT/1h/2026-04-25.jsonl

# strategy 侧 4h
sed -n '1,20p' app/strategy/rpc/data/kline/ETHUSDT/4h/2026-04-25.jsonl
```

#### 4. 按时间 grep 某段 K线

```bash
# 找 15m 中 openTime=2026-04-25T11:45:00.000Z 的记录
grep '2026-04-25T11:45:00.000Z' app/market/rpc/data/kline/ETHUSDT/15m/2026-04-25.jsonl

# 找 1m 中 11:45-11:59 这 15 分钟
grep -E '2026-04-25T11:(4[5-9]|5[0-9]):00\.000Z' \
  app/market/rpc/data/kline/ETHUSDT/1m/2026-04-25.jsonl

# 找 strategy 侧同一根 1h
grep '2026-04-25T02:00:00.000Z' app/strategy/rpc/data/kline/ETHUSDT/1h/2026-04-25.jsonl
```

#### 5. 看 decision / signal 日志

```bash
# 看今天 decision 前 20 条
sed -n '1,20p' app/strategy/rpc/data/signal/decision/ETHUSDT/2026-04-25.jsonl

# 看今天 signal 前 20 条
sed -n '1,20p' app/strategy/rpc/data/signal/ETHUSDT/2026-04-25.jsonl

# grep 某个 reason_code
grep '"reason_code":"h4_no_trend"' \
  app/strategy/rpc/data/signal/decision/ETHUSDT/2026-04-25.jsonl

# grep 某个 summary
grep '"summary":"h4=dirty' \
  app/strategy/rpc/data/signal/decision/ETHUSDT/2026-04-25.jsonl
```

#### 6. 看 WebSocket 是否真的有流

```bash
# 关注 market 运行日志里的 ws stats
grep '\[ws stats\]' app/market/rpc/*.log

# 如果是直接前台运行，就盯住这类输出
# [ws stats] total_messages=... kline_messages=... closed_1m_messages=...
```

#### 7. 按 dirtyReason 排查脏 K线

```bash
# market 侧找 incomplete_bucket
grep '"dirtyReason":"incomplete_bucket"' \
  app/market/rpc/data/kline/ETHUSDT/15m/2026-04-25.jsonl

# strategy 侧找 worker_gc
grep '"dirtyReason":"worker_gc' \
  app/strategy/rpc/data/kline/ETHUSDT/1h/2026-04-25.jsonl
```

#### 8. 对同一时间窗口 cross-check market 和 strategy

```bash
# market 15m
grep '2026-04-25T12:00:00.000Z' app/market/rpc/data/kline/ETHUSDT/15m/2026-04-25.jsonl

# strategy 15m
grep '2026-04-25T12:00:00.000Z' app/strategy/rpc/data/kline/ETHUSDT/15m/2026-04-25.jsonl

# decision
grep '2026-04-25 12:15:59' app/strategy/rpc/data/signal/decision/ETHUSDT/2026-04-25.jsonl
```

#### 9. 看 Go 测试和快速校验

```bash
# market
go test ./app/market/rpc/...

# strategy
go test ./app/strategy/rpc/...

# execution + order
go test ./app/execution/rpc/... ./app/order/rpc/...
```

#### 10. 常用排查顺序

```text
1. 先看 market 的 [ws stats]
2. 再看 market 的 1m jsonl
3. 再看 market 的 15m/1h/4h jsonl
4. 再看 strategy 的 decision jsonl
5. 再看 strategy 的 signal jsonl
6. 再看 execution
7. 最后看 order
```

### 6.4 典型故障案例

#### 案例 1: `connection reset by peer` + `[ws stats] total_messages=0`

典型日志：

```text
2026/04/26 06:42:10 WebSocket read error: read tcp 192.168.10.6:54868->192.168.10.13:1080: read: connection reset by peer
2026/04/26 06:42:13 Connecting to Binance WebSocket: wss://fstream.binance.com/stream?streams=ethusdt@kline_1m
2026/04/26 06:42:15 WebSocket connected successfully
2026/04/26 06:43:15 [ws stats] total_messages=0 kline_messages=0 closed_1m_messages=0
2026/04/26 06:44:15 [ws stats] total_messages=0 kline_messages=0 closed_1m_messages=0
```

现象解释：

- 代理连接被对端主动 reset
- WebSocket 随后重连成功
- 但重连后长时间 `total_messages=0`
- 说明握手成功但没有任何业务帧进来

第一判断：

- 问题在代理链路，不在 `market` 聚合器、Kafka 或 `strategy`
- 这类现象已经可以排除“有流但没闭K”

为什么可以快速排除业务代码问题：

- 如果是聚合器问题，通常会看到 `closed_1m_messages > 0`
- 如果是 Kafka 问题，通常会看到 `1m` 已经落盘或有 `[1m kline]`
- 如果 `total_messages=0`，说明问题发生在最上游 WebSocket 输入层

优先排查项：

1. 检查 `app/market/rpc/etc/market.demo.yaml` 中的 `Proxy`
2. 临时把 `Proxy` 改为空字符串做无代理对照实验
3. 重启 `market`，观察 `[ws stats]` 是否立刻恢复为非 0
4. 如果无代理恢复正常，则可判定为代理不稳定或吞帧

建议命令：

```bash
# 看 ws stats
grep '\[ws stats\]' app/market/rpc/*.log

# 看最近是否有 reset
grep 'connection reset by peer' app/market/rpc/*.log

# 直接前台重启 market 观察
cd /Users/bytedance/GolandProjects/exchange-system/app/market/rpc
go run market.go
```

结论模板：

```text
WebSocket 已连接，但 total_messages 持续为 0。
说明当前不是聚合/Kafka/策略问题，而是代理或上游 WebSocket 数据流没有真正进入 market。
```

#### 案例 2: `incomplete_bucket`，高周期只收到了部分 1m

典型日志 / 文件：

```json
{"symbol":"ETHUSDT","interval":"15m","openTime":"2026-04-25T11:45:00.000Z","closeTime":"2026-04-25T11:59:59.999Z","eventTime":"2026-04-25T12:01:00.404Z","isDirty":true,"dirtyReason":"incomplete_bucket","isTradable":false,"isFinal":false}
```

现象解释：

- 某根 `15m/1h/4h` 已经生成
- 但 `dirtyReason=incomplete_bucket`
- 说明这一周期应有的 `1m` 没收全

第一判断：

- 不是策略问题
- 不是 Kafka topic 问题
- 问题在 `market` 聚合所依赖的 `1m` 输入不连续

怎么快速确认：

1. 先记下这根高周期的 `openTime`
2. 回到 `market` 的 `1m` 文件
3. 把该周期应有的 `1m` 一根一根 grep 出来
4. 看是否缺少前几根、末几根或中间某几根

典型成因：

- 服务启动晚了，前几根 `1m` 没接上
- 运行中间断流，导致中间缺分钟
- 代理抖动或 WebSocket 输入不连续

建议命令：

```bash
# 查这根 15m
grep '2026-04-25T11:45:00.000Z' app/market/rpc/data/kline/ETHUSDT/15m/2026-04-25.jsonl

# 对这 15 分钟应有的 1m 逐分钟核对
grep -E '2026-04-25T11:(4[5-9]|5[0-9]):00\.000Z' \
  app/market/rpc/data/kline/ETHUSDT/1m/2026-04-25.jsonl
```

结论模板：

```text
这根高周期 K线不是没生成，而是只收到了部分 1m。
当前根因是上游输入不连续，导致该 bucket 被标记为 incomplete_bucket。
```

#### 案例 3: `worker_gc`，停流过久导致残桶被强制刷出

典型日志 / 文件：

```text
GC: cleaning idle worker symbol=ETHUSDT idle=...
```

```json
{"symbol":"ETHUSDT","interval":"1h","dirtyReason":"worker_gc","isDirty":true,"isTradable":false,"isFinal":false}
```

现象解释：

- 某个 symbol worker 长时间没再收到新数据
- 超过 `MaxIdleTime` 后被 GC
- 手里的未完成 bucket 被强制刷出
- 因此会出现 `dirtyReason=worker_gc`

第一判断：

- 不是正常闭K
- 而是“停流后被动冲刷残桶”
- 说明上游输入已经停了一段时间

怎么快速确认：

1. 查对应高周期 K线的 `eventTime`
2. 看它是否明显晚于正常周期边界
3. 回看同时间段 `1m` 是否已经停流
4. 核对 `WorkerGC.MaxIdleTime` 配置

典型成因：

- WebSocket 断流
- 代理中断
- 程序长时间没收到该 symbol 的新 `1m`

建议命令：

```bash
# 查 worker_gc 记录
grep '"dirtyReason":"worker_gc' app/market/rpc/data/kline/ETHUSDT/1h/2026-04-25.jsonl

# 查 market 日志里的 GC 关键字
grep 'GC: cleaning idle worker' app/market/rpc/*.log
```

结论模板：

```text
这不是正常的周期闭合发射，而是因为上游停流过久，worker 被 GC 后把残桶强制刷出。
根因仍然在 WebSocket / 代理 / 输入流中断。
```

#### 案例 4: `h4_not_ready`，策略因高周期未就绪而跳过

典型日志 / 文件：

```json
{
  "decision":"skip",
  "reason_code":"h4_not_ready",
  "extras":{
    "summary":"h4=dirty(incomplete_bucket)",
    "h4_is_dirty":true,
    "h4_is_tradable":false,
    "h4_is_final":false,
    "h4_dirty_reason":"incomplete_bucket"
  }
}
```

现象解释：

- `strategy` 已经开始做决策
- 但在 `4h` 这一层就被挡住
- 说明策略并不是没收到数据
- 而是收到的 `4h` K线还不够稳定，不能用于交易判断

第一判断：

- 先不要怀疑 `strategy` 条件本身
- 先看 `4h` 上游状态是不是 dirty / non-final / non-tradable

怎么快速确认：

1. 打开 `decision jsonl`
2. 查 `reason_code=h4_not_ready`
3. 看 `extras.summary`
4. 再看 `h4_dirty_reason`、`h4_is_final`、`h4_is_tradable`
5. 回到 `strategy/data/kline/ETHUSDT/4h/*.jsonl` 看对应 `4h` 记录

典型成因：

- 上游 `4h` 本身 dirty
- `4h` 还不是 final
- `4h` 指标仍未准备好
- `market` 的高周期输入有缺口

建议命令：

```bash
# 查 h4_not_ready
grep '"reason_code":"h4_not_ready"' \
  app/strategy/rpc/data/signal/decision/ETHUSDT/2026-04-25.jsonl

# 查 strategy 侧 4h K线
sed -n '1,20p' app/strategy/rpc/data/kline/ETHUSDT/4h/2026-04-25.jsonl
```

结论模板：

```text
当前不是策略参数导致不开仓，而是 4h 上游数据尚未达到可交易条件。
根因优先回到 market 侧高周期 K线状态排查。
```

#### 案例 5: `harvest_path_block`，信号形成后被风险门控阻断

典型日志 / 文件：

```json
{
  "decision":"skip",
  "reason_code":"harvest_path_block",
  "extras":{
    "summary":"h4=final -> h1=final -> m15=final",
    "trend":"LONG",
    "pullback":"LONG",
    "entry":"LONG_BREAKOUT"
  }
}
```

现象解释：

- 多周期条件基本已经成立
- 按普通趋势逻辑原本接近可以开仓
- 但最终在风险门控层被挡住

第一判断：

- 这通常不是行情链路问题
- 也不是 `market` 没有产出 K线
- 而是策略侧额外风险控制主动阻断

怎么快速确认：

1. 看 `decision` 终端日志里的 `[decision][BLOCK]`
2. 查 `reason_code=harvest_path_block`
3. 看 `extras.summary` 是否已显示 `h4/h1/m15` 都正常
4. 回看 Harvest Path 相关配置和开关

典型成因：

- Harvest Path 风险门控已启用
- 风险分数超过阈值
- 当前行情结构被识别为不适合开仓

建议命令：

```bash
# 查 harvest_path_block
grep '"reason_code":"harvest_path_block"' \
  app/strategy/rpc/data/signal/decision/ETHUSDT/2026-04-25.jsonl

# 看 strategy 配置中的 Harvest Path 开关
grep -n 'HarvestPath' app/strategy/rpc/etc/strategy.demo.yaml
```

结论模板：

```text
主策略信号并非没有形成，而是在最终发单前被 Harvest Path 风险门控主动阻断。
根因在策略风险控制层，而不是 market 数据链路。
```

### 6.5 AI/ML 模型文件

```
LSTM模型:
  路径: tools/harvestpath_lstm/artifacts/
  文件:
    • ethusdt_harvest_path_lstm.pt (PyTorch模型)
    • ethusdt_harvest_path_lstm.json (模型配置)

训练工具:
  路径: tools/harvestpath_lstm/
  脚本:
    • data.py (数据准备)
    • train.py (模型训练)
    • predict.py (模型推理)
```

---

## 七、配置参数参考

### 7.1 Market Service 配置

```yaml
# market.yaml
Name: market.rpc
ListenOn: 0.0.0.0:8081
Etcd:
  Hosts:
    - etcd:2379
  Key: market.rpc
Kafka:
  Brokers:
    - kafka1:9092
  Topic: market_data
WatermarkDelay: 3000  # 3秒
IndicatorParams:
  Ema21Period: 21
  Ema55Period: 55
  RsiPeriod: 14
  AtrPeriod: 14
```

### 7.2 Strategy Service 配置

```yaml
# strategy.demo.yaml / strategy.prod.yaml
Name: strategy.rpc
ListenOn: 0.0.0.0:8082
Etcd:
  Hosts:
    - etcd:2379
  Key: strategy.rpc
Kafka:
  Brokers:
    - kafka1:9092
  Topic: strategy_signals
StrategyParams:
  signal_mode: 0
  1m_trading_paused: 0
  equity: 10000
  risk_per_trade: 0.03
  max_position_size: 0.55
  leverage: 7
  max_leverage_ratio: 0.92
  stop_loss_atr_multiplier: 1.5
  max_consecutive_losses: 3
  max_daily_loss_pct: 0.07
  max_drawdown_pct: 0.15
  max_positions: 1
```

#### 7.2.1 多币种模板化配置建议

当前问题:

```text
当交易对从 1 个扩展到 5 个以上时，
strategy.demo.yaml / strategy.prod.yaml 中会出现多份几乎完全相同的参数块，
后续新增第 6、第 7 个币时维护成本会快速上升。
```

建议目标:

```text
把“公共参数”与“symbol 列表”分开维护：
• 公共参数: 一份模板
• symbol 列表: 只维护有哪些币种启用
• 个别币种差异: 只覆盖差异字段
```

当前已落地方案: 三模板 + symbol 绑定

```yaml
# 当前代码已支持:
# • Templates
# • Strategies[].Template
# • Strategies[].Overrides
Templates:
  btc-core:
    risk_per_trade: 0.025
    max_position_size: 0.50
    leverage: 6.0
    max_leverage_ratio: 0.90
    equity: 10000
    stop_loss_atr_multiplier: 1.6
    ema_exit_buffer_atr: 0.35
    min_holding_bars: 6
    ema_exit_confirm_bars: 2
    deep_pullback_scale: 0.95
    pullback_deep_band: 0.0025
    require_both_entry_signals: 0
    h1_rsi_long_low: 44
    h1_rsi_long_high: 60
    h1_rsi_short_low: 40
    h1_rsi_short_high: 56
    m15_breakout_lookback: 7
    m15_rsi_bias_long: 53
    m15_rsi_bias_short: 47
    debug_skip_4h_trend: 0
    debug_skip_pullback: 0
    debug_skip_15m_entry: 0
    signal_mode: 0
    1m_trading_paused: 0
    1m_atr_multiplier: 1.6
    1m_breakout_lookback: 24
    1m_rsi_bias_long: 56
    1m_rsi_bias_short: 44
    1m_min_holding_bars: 6
    1m_ema_exit_buffer_atr: 0.35
    max_consecutive_losses: 3
    max_daily_loss_pct: 0.06
    max_drawdown_pct: 0.12
    drawdown_position_scale: 0.5
    max_positions: 1
  eth-core:
    risk_per_trade: 0.03
    max_position_size: 0.55
    leverage: 7.0
    max_leverage_ratio: 0.92
    equity: 10000
    stop_loss_atr_multiplier: 1.5
    ema_exit_buffer_atr: 0.30
    min_holding_bars: 5
    ema_exit_confirm_bars: 2
    deep_pullback_scale: 0.9
    pullback_deep_band: 0.003
    require_both_entry_signals: 0
    h1_rsi_long_low: 42
    h1_rsi_long_high: 60
    h1_rsi_short_low: 40
    h1_rsi_short_high: 58
    m15_breakout_lookback: 6
    m15_rsi_bias_long: 52
    m15_rsi_bias_short: 48
    debug_skip_4h_trend: 0
    debug_skip_pullback: 0
    debug_skip_15m_entry: 0
    signal_mode: 0
    1m_trading_paused: 0
    1m_atr_multiplier: 1.5
    1m_breakout_lookback: 20
    1m_rsi_bias_long: 55
    1m_rsi_bias_short: 45
    1m_min_holding_bars: 5
    1m_ema_exit_buffer_atr: 0.30
    max_consecutive_losses: 3
    max_daily_loss_pct: 0.07
    max_drawdown_pct: 0.15
    drawdown_position_scale: 0.5
    max_positions: 1
  high-beta:
    risk_per_trade: 0.018
    max_position_size: 0.40
    leverage: 5.0
    max_leverage_ratio: 0.85
    equity: 10000
    stop_loss_atr_multiplier: 1.8
    ema_exit_buffer_atr: 0.40
    min_holding_bars: 7
    ema_exit_confirm_bars: 3
    deep_pullback_scale: 0.80
    pullback_deep_band: 0.0045
    require_both_entry_signals: 1
    h1_rsi_long_low: 45
    h1_rsi_long_high: 58
    h1_rsi_short_low: 42
    h1_rsi_short_high: 55
    m15_breakout_lookback: 8
    m15_rsi_bias_long: 54
    m15_rsi_bias_short: 46
    debug_skip_4h_trend: 0
    debug_skip_pullback: 0
    debug_skip_15m_entry: 0
    signal_mode: 0
    1m_trading_paused: 0
    1m_atr_multiplier: 1.8
    1m_breakout_lookback: 24
    1m_rsi_bias_long: 57
    1m_rsi_bias_short: 43
    1m_min_holding_bars: 7
    1m_ema_exit_buffer_atr: 0.40
    max_consecutive_losses: 2
    max_daily_loss_pct: 0.05
    max_drawdown_pct: 0.12
    drawdown_position_scale: 0.4
    max_positions: 1

Strategies:
  - name: trend-following
    symbol: BTCUSDT
    enabled: true
    template: btc-core

  - name: trend-following
    symbol: ETHUSDT
    enabled: true
    template: eth-core

  - name: trend-following
    symbol: SOLUSDT
    enabled: true
    template: high-beta

  - name: trend-following
    symbol: BNBUSDT
    enabled: true
    template: high-beta

  - name: trend-following
    symbol: XRPUSDT
    enabled: true
    template: high-beta

  # 个别 symbol 如需微调，可只覆盖差异字段
  - name: trend-following
    symbol: DOGEUSDT
    enabled: false
    template: high-beta
    overrides:
      leverage: 4.0
      risk_per_trade: 0.015
```

适用场景:

- 已经进入多币种并行运行阶段
- 需要把 BTC / ETH / 高 beta 币种做分层参数管理
- 后续要继续扩展到第 6、第 7 个币种

兼容性说明

```text
1. 当前代码同时兼容:
   • 老写法: Strategies[].Parameters
   • 新写法: Templates + Strategies[].Template + Overrides

2. 参数合并优先级:
   template < parameters < overrides

3. 运行时策略实例化方式不变:
   • strategies[symbol] 仍按 symbol 路由
   • 变化只发生在配置解析阶段
```

这样做的好处:

```text
• 新增第 6 个币时，只需要加一条 symbol 配置
• 公共参数只改一处
• 个别币种可以做轻量差异化调参
• demo / prod 两份配置更容易保持一致
• 更适合后续扩展到 10+ 个交易对
```

当前阶段的务实建议:

```text
优先保持 demo / prod 的模板名称、symbol 绑定关系和分层原则一致，
只让少数环境开关（如 Harvest Path）在不同环境中分别控制。
```

### 7.3 Execution Service 配置

```yaml
# execution.yaml
Name: execution.rpc
ListenOn: 0.0.0.0:8083
Etcd:
  Hosts:
    - etcd:2379
  Key: execution.rpc
Kafka:
  Brokers:
    - kafka1:9092
  Topic: order_events
Exchange:
  Binance:
    ApiKey: xxx
    SecretKey: xxx
  OKX:
    ApiKey: xxx
    SecretKey: xxx
    Passphrase: xxx
Redis:
  Host: redis:6379
```

---

## 八、系统特性

### 8.1 高可用设计

| 特性 | 实现方式 |
|------|---------|
| 服务注册发现 | etcd 集群 |
| 消息队列 | Kafka 多broker |
| 缓存高可用 | Redis AOF持久化 |
| 数据完整性 | Watermark机制 + 允许延迟 |
| 幂等控制 | Redis记录client_order_id |
| 异步解耦 | Kafka生产者/消费者模式 |
| 背压保护 | 异步队列 + 丢弃策略 |

### 8.2 性能优化

| 优化项 | 实现方式 |
|-------|---------|
| 异步Kafka发送 | 4096缓冲队列 + 独立协程 |
| 历史数据预热 | 启动时加载4500根K线 |
| 指标计算优化 | Ring Buffer + 增量计算 |
| 并发处理 | Per-symbol worker goroutine |
| 内存管理 | Worker GC + 空闲清理 |
| 延迟容忍 | Flink lateness模型 |

### 8.3 可观测性

| 指标 | 说明 |
|------|------|
| Received1m | 接收1m K线数量 |
| Emitted1m/3m/15m/1h/4h | 各周期发射数量 |
| GapsDetected | 数据缺失检测 |
| KafkaSendErrors | Kafka发送失败次数 |
| WatermarkWaits | Watermark等待次数 |
| WatermarkFlushes | Watermark超时刷新次数 |

---

## 九、部署指南

### 9.1 环境要求

- **Go**: 1.25.6+
- **Docker**: 20.10+
- **Docker Compose**: 2.0+
- **Python**: 3.10+ (LSTM训练)

### 9.2 启动基础设施

```bash
cd deploy
docker-compose up -d
```

### 9.3 启动服务

```bash
# 启动 Market Service
cd app/market/rpc
go run market.go

# 启动 Strategy Service
cd app/strategy/rpc
go run strategy.go

# 启动 Execution Service
cd app/execution/rpc
go run execution.go

# 启动 Order Service
cd app/order/rpc
go run order.go

# 启动 API Gateway
cd app/api/gateway
go run main.go
```

### 9.4 验证系统

```bash
# 检查服务注册
curl http://localhost:2379/v2/keys/

# 检查 Kafka Topics
docker exec kafka1 kafka-topics.sh --bootstrap-server localhost:9092 --list

# 检查 Redis
docker exec redis redis-cli ping
```

---

## 十、开发指南

### 10.1 代码结构

```
exchange-system/
├── app/
│   ├── api/gateway/          # API网关
│   ├── execution/rpc/        # 执行服务
│   ├── market/rpc/           # 市场数据服务
│   ├── order/rpc/            # 订单服务
│   └── strategy/rpc/         # 策略服务
├── common/
│   ├── binance/              # 币安通用接口
│   ├── indicator/            # 技术指标库
│   ├── kafka/                # Kafka工具
│   ├── pb/                   # Protobuf生成代码
│   └── proto/                # Protobuf定义
├── deploy/
│   └── docker-compose.yml    # 基础设施编排
├── docs/                     # 文档
├── tools/                    # 工具脚本
│   └── harvestpath_lstm/     # LSTM训练工具
├── go.mod
└── go.sum
```

### 10.2 添加新策略

1. 在 `app/strategy/rpc/internal/strategy/` 创建策略文件
2. 实现 `OnKline` 方法接收K线数据
3. 实现信号生成逻辑
4. 通过 `producer.SendMarketData` 发送信号到Kafka

### 10.3 添加新交易所

1. 在 `app/execution/rpc/internal/exchange/` 创建交易所接口
2. 实现 `Exchange` 接口
3. 在 `router.go` 中注册新交易所
4. 更新配置文件

---

*本文档为 exchange-system 实盘盈利系统架构设计文档，涵盖系统总览、核心流程、技术架构、策略逻辑、风控体系、部署指南等内容。*