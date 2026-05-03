CREATE DATABASE IF NOT EXISTS exchange_analytics;

CREATE TABLE IF NOT EXISTS exchange_analytics.kline_fact
(
    event_time       DateTime64(3, 'UTC'),
    symbol           LowCardinality(String),
    interval         LowCardinality(String),
    open_time        DateTime64(3, 'UTC'),
    close_time       DateTime64(3, 'UTC'),
    open             Decimal(20, 8),
    high             Decimal(20, 8),
    low              Decimal(20, 8),
    close            Decimal(20, 8),
    volume           Decimal(28, 8),
    quote_volume     Decimal(28, 8),
    taker_buy_volume Decimal(28, 8),
    is_closed        UInt8,
    is_dirty         UInt8,
    dirty_reason     LowCardinality(String),
    is_tradable      UInt8,
    is_final         UInt8,
    ema21            Decimal(20, 8),
    ema55            Decimal(20, 8),
    rsi              Decimal(10, 4),
    atr              Decimal(20, 8),
    source           LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, interval, open_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.decision_fact
(
    event_time   DateTime64(3, 'UTC'),
    symbol       LowCardinality(String),
    strategy_id  String,
    template     LowCardinality(String),
    interval     LowCardinality(String),
    stage        LowCardinality(String),
    decision     LowCardinality(String),
    reason       String,
    reason_code  LowCardinality(String),
    has_position UInt8,
    is_final     UInt8,
    is_tradable  UInt8,
    open_time    DateTime64(3, 'UTC'),
    close_time   DateTime64(3, 'UTC'),
    route_bucket LowCardinality(String),
    route_reason LowCardinality(String),
    extras_json  String,
    trace_id     String
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, strategy_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.signal_fact
(
    event_time        DateTime64(3, 'UTC'),
    symbol            LowCardinality(String),
    strategy_id       String,
    template          LowCardinality(String),
    signal_id         String,
    action            LowCardinality(String),
    side              LowCardinality(String),
    signal_type       LowCardinality(String),
    quantity          Decimal(20, 8),
    entry_price       Decimal(20, 8),
    stop_loss         Decimal(20, 8),
    take_profit_json  String,
    reason            String,
    exit_reason_kind  LowCardinality(String),
    exit_reason_label String,
    risk_reward       Decimal(20, 8),
    atr               Decimal(20, 8),
    tags_json         String,
    trace_id          String
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, strategy_id, signal_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.execution_event_fact
(
    event_time      DateTime64(3, 'UTC'),
    account_id      String,
    symbol          LowCardinality(String),
    strategy_id     String,
    signal_id       String,
    order_id        String,
    client_order_id String,
    action          LowCardinality(String),
    side            LowCardinality(String),
    signal_type     LowCardinality(String),
    requested_qty   Decimal(20, 8),
    executed_qty    Decimal(20, 8),
    price           Decimal(20, 8),
    status          LowCardinality(String),
    exchange        LowCardinality(String),
    latency_ms      UInt32,
    error_code      String,
    error_message   String,
    trace_id        String
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (account_id, symbol, order_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.order_event_fact
(
    event_time         DateTime64(3, 'UTC'),
    account_id         String,
    symbol             LowCardinality(String),
    order_id           String,
    client_order_id    String,
    position_cycle_id  String,
    strategy_id        String,
    template           LowCardinality(String),
    action_type        LowCardinality(String),
    side               LowCardinality(String),
    order_type         LowCardinality(String),
    price              Decimal(20, 8),
    orig_qty           Decimal(20, 8),
    executed_qty       Decimal(20, 8),
    status             LowCardinality(String),
    reduce_only        UInt8,
    reason             String,
    signal_reason_json String,
    trace_id           String
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (account_id, symbol, position_cycle_id, order_id, event_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS exchange_analytics.position_cycle_fact
(
    open_time                 DateTime64(3, 'UTC'),
    close_time                Nullable(DateTime64(3, 'UTC')),
    account_id                String,
    symbol                    LowCardinality(String),
    position_cycle_id         String,
    strategy_id               String,
    template                  LowCardinality(String),
    position_side             LowCardinality(String),
    cycle_status              LowCardinality(String),
    entry_price               Decimal(20, 8),
    exit_price                Decimal(20, 8),
    entry_qty                 Decimal(20, 8),
    exit_qty                  Decimal(20, 8),
    realized_pnl              Decimal(20, 8),
    max_profit                Decimal(20, 8),
    max_drawdown              Decimal(20, 8),
    partial_close_order_count UInt32,
    final_close_order_count   UInt32,
    exit_reason_kind          LowCardinality(String),
    exit_reason_label         String,
    trace_id                  String
)
ENGINE = MergeTree
PARTITION BY toDate(open_time)
ORDER BY (account_id, symbol, position_cycle_id)
SETTINGS index_granularity = 8192;