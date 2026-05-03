CREATE DATABASE IF NOT EXISTS exchange_analytics;

CREATE TABLE IF NOT EXISTS exchange_analytics.kline_fact
(
    event_time       DateTime64(3, 'UTC') COMMENT '事件写入时间',
    symbol           LowCardinality(String) COMMENT '交易对标识',
    interval         LowCardinality(String) COMMENT 'K 线周期',
    open_time        DateTime64(3, 'UTC') COMMENT 'K 线开始时间',
    close_time       DateTime64(3, 'UTC') COMMENT 'K 线结束时间',
    open             Decimal(20, 8) COMMENT '开盘价',
    high             Decimal(20, 8) COMMENT '最高价',
    low              Decimal(20, 8) COMMENT '最低价',
    close            Decimal(20, 8) COMMENT '收盘价',
    volume           Decimal(28, 8) COMMENT '成交量',
    quote_volume     Decimal(28, 8) COMMENT '计价货币成交额',
    taker_buy_volume Decimal(28, 8) COMMENT '主动买入成交量',
    is_closed        UInt8 COMMENT 'K 线是否闭合',
    is_dirty         UInt8 COMMENT '是否为脏数据',
    dirty_reason     LowCardinality(String) COMMENT '脏数据原因',
    is_tradable      UInt8 COMMENT '当前 K 线是否允许交易',
    is_final         UInt8 COMMENT '指标与状态是否最终确定',
    ema21            Decimal(20, 8) COMMENT '21 周期 EMA 指标值',
    ema55            Decimal(20, 8) COMMENT '55 周期 EMA 指标值',
    rsi              Decimal(10, 4) COMMENT 'RSI 指标值',
    atr              Decimal(20, 8) COMMENT 'ATR 指标值',
    source           LowCardinality(String) COMMENT '数据来源'
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, interval, open_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.decision_fact
(
    event_time   DateTime64(3, 'UTC') COMMENT '决策事件时间',
    symbol       LowCardinality(String) COMMENT '交易对标识',
    strategy_id  String COMMENT '策略唯一标识',
    template     LowCardinality(String) COMMENT '策略模板名称',
    interval     LowCardinality(String) COMMENT '决策使用的 K 线周期',
    stage        LowCardinality(String) COMMENT '决策阶段',
    decision     LowCardinality(String) COMMENT '决策结果',
    reason       String COMMENT '决策原因描述',
    reason_code  LowCardinality(String) COMMENT '决策原因编码',
    has_position UInt8 COMMENT '当前是否持仓',
    is_final     UInt8 COMMENT '决策结果是否最终确定',
    is_tradable  UInt8 COMMENT '当前市场是否允许交易',
    open_time    DateTime64(3, 'UTC') COMMENT '关联 K 线开始时间',
    close_time   DateTime64(3, 'UTC') COMMENT '关联 K 线结束时间',
    route_bucket LowCardinality(String) COMMENT '决策路由分桶',
    route_reason LowCardinality(String) COMMENT '决策路由原因',
    extras_json  String COMMENT '扩展上下文信息 JSON',
    trace_id     String COMMENT '链路追踪标识'
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, strategy_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.signal_fact
(
    event_time        DateTime64(3, 'UTC') COMMENT '信号事件时间',
    symbol            LowCardinality(String) COMMENT '交易对标识',
    strategy_id       String COMMENT '策略唯一标识',
    template          LowCardinality(String) COMMENT '策略模板名称',
    signal_id         String COMMENT '信号唯一标识',
    action            LowCardinality(String) COMMENT '信号动作',
    side              LowCardinality(String) COMMENT '交易方向',
    signal_type       LowCardinality(String) COMMENT '信号类型',
    quantity          Decimal(20, 8) COMMENT '计划下单数量',
    entry_price       Decimal(20, 8) COMMENT '期望入场价',
    stop_loss         Decimal(20, 8) COMMENT '止损价',
    take_profit_json  String COMMENT '止盈配置 JSON',
    reason            String COMMENT '信号触发原因描述',
    exit_reason_kind  LowCardinality(String) COMMENT '退出原因分类',
    exit_reason_label String COMMENT '退出原因标签',
    risk_reward       Decimal(20, 8) COMMENT '风险收益比',
    atr               Decimal(20, 8) COMMENT '生成信号时的 ATR 值',
    tags_json         String COMMENT '信号标签 JSON',
    trace_id          String COMMENT '链路追踪标识'
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, strategy_id, signal_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.execution_event_fact
(
    event_time      DateTime64(3, 'UTC') COMMENT '执行事件时间',
    account_id      String COMMENT '账户唯一标识',
    symbol          LowCardinality(String) COMMENT '交易对标识',
    strategy_id     String COMMENT '策略唯一标识',
    signal_id       String COMMENT '关联信号标识',
    order_id        String COMMENT '交易所订单标识',
    client_order_id String COMMENT '客户端订单标识',
    action          LowCardinality(String) COMMENT '执行动作',
    side            LowCardinality(String) COMMENT '交易方向',
    signal_type     LowCardinality(String) COMMENT '信号类型',
    requested_qty   Decimal(20, 8) COMMENT '请求执行数量',
    executed_qty    Decimal(20, 8) COMMENT '实际执行数量',
    price           Decimal(20, 8) COMMENT '执行价格',
    status          LowCardinality(String) COMMENT '执行状态',
    exchange        LowCardinality(String) COMMENT '交易所标识',
    latency_ms      UInt32 COMMENT '执行延迟毫秒数',
    error_code      String COMMENT '错误编码',
    error_message   String COMMENT '错误信息',
    trace_id        String COMMENT '链路追踪标识'
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (account_id, symbol, order_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.order_event_fact
(
    event_time         DateTime64(3, 'UTC') COMMENT '订单事件时间',
    account_id         String COMMENT '账户唯一标识',
    symbol             LowCardinality(String) COMMENT '交易对标识',
    order_id           String COMMENT '交易所订单标识',
    client_order_id    String COMMENT '客户端订单标识',
    position_cycle_id  String COMMENT '仓位周期标识',
    strategy_id        String COMMENT '策略唯一标识',
    template           LowCardinality(String) COMMENT '策略模板名称',
    action_type        LowCardinality(String) COMMENT '订单动作类型',
    side               LowCardinality(String) COMMENT '交易方向',
    order_type         LowCardinality(String) COMMENT '订单类型',
    price              Decimal(20, 8) COMMENT '委托价格',
    orig_qty           Decimal(20, 8) COMMENT '原始委托数量',
    executed_qty       Decimal(20, 8) COMMENT '累计成交数量',
    status             LowCardinality(String) COMMENT '订单状态',
    reduce_only        UInt8 COMMENT '是否仅减仓',
    reason             String COMMENT '订单原因描述',
    signal_reason_json String COMMENT '信号原因 JSON',
    trace_id           String COMMENT '链路追踪标识'
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (account_id, symbol, position_cycle_id, order_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.position_cycle_fact
(
    open_time                 DateTime64(3, 'UTC') COMMENT '仓位周期开始时间',
    close_time                Nullable(DateTime64(3, 'UTC')) COMMENT '仓位周期结束时间',
    account_id                String COMMENT '账户唯一标识',
    symbol                    LowCardinality(String) COMMENT '交易对标识',
    position_cycle_id         String COMMENT '仓位周期唯一标识',
    strategy_id               String COMMENT '策略唯一标识',
    template                  LowCardinality(String) COMMENT '策略模板名称',
    position_side             LowCardinality(String) COMMENT '持仓方向',
    cycle_status              LowCardinality(String) COMMENT '仓位周期状态',
    entry_price               Decimal(20, 8) COMMENT '平均开仓价',
    exit_price                Decimal(20, 8) COMMENT '平均平仓价',
    entry_qty                 Decimal(20, 8) COMMENT '累计开仓数量',
    exit_qty                  Decimal(20, 8) COMMENT '累计平仓数量',
    realized_pnl              Decimal(20, 8) COMMENT '已实现盈亏',
    max_profit                Decimal(20, 8) COMMENT '周期内最大浮盈',
    max_drawdown              Decimal(20, 8) COMMENT '周期内最大回撤',
    partial_close_order_count UInt32 COMMENT '部分平仓订单数量',
    final_close_order_count   UInt32 COMMENT '最终平仓订单数量',
    exit_reason_kind          LowCardinality(String) COMMENT '退出原因分类',
    exit_reason_label         String COMMENT '退出原因标签',
    trace_id                  String COMMENT '链路追踪标识'
)
ENGINE = MergeTree
PARTITION BY toDate(open_time)
ORDER BY (account_id, symbol, position_cycle_id)
SETTINGS index_granularity = 8192;