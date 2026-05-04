-- 把 kline_fact 从普通 MergeTree 迁移为 ReplacingMergeTree(event_time)。
-- 迁移目标：
-- 1. 同一组 (symbol, interval, open_time) 最终只保留 event_time 最新版本；
-- 2. 分区改为 toDate(open_time)，避免同一根 K 线跨天补写时落到不同分区导致无法收敛。
-- 执行说明：
-- 1. 该脚本会保留旧表为 kline_fact_backup_20260504；
-- 2. 若该备份表名已存在，请先按实际环境调整表名后再执行。

CREATE TABLE exchange_analytics.kline_fact_v2
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
ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toDate(open_time)
ORDER BY (symbol, interval, open_time)
SETTINGS index_granularity = 8192;

INSERT INTO exchange_analytics.kline_fact_v2
(
    event_time,
    symbol,
    interval,
    open_time,
    close_time,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    taker_buy_volume,
    is_closed,
    is_dirty,
    dirty_reason,
    is_tradable,
    is_final,
    ema21,
    ema55,
    rsi,
    atr,
    source
)
SELECT
    max(event_time),
    symbol,
    interval,
    open_time,
    argMax(close_time, event_time) AS close_time,
    argMax(open, event_time) AS open,
    argMax(high, event_time) AS high,
    argMax(low, event_time) AS low,
    argMax(close, event_time) AS close,
    argMax(volume, event_time) AS volume,
    argMax(quote_volume, event_time) AS quote_volume,
    argMax(taker_buy_volume, event_time) AS taker_buy_volume,
    argMax(is_closed, event_time) AS is_closed,
    argMax(is_dirty, event_time) AS is_dirty,
    argMax(dirty_reason, event_time) AS dirty_reason,
    argMax(is_tradable, event_time) AS is_tradable,
    argMax(is_final, event_time) AS is_final,
    argMax(ema21, event_time) AS ema21,
    argMax(ema55, event_time) AS ema55,
    argMax(rsi, event_time) AS rsi,
    argMax(atr, event_time) AS atr,
    argMax(source, event_time) AS source
FROM exchange_analytics.kline_fact
GROUP BY
    symbol,
    interval,
    open_time;

RENAME TABLE
    exchange_analytics.kline_fact TO exchange_analytics.kline_fact_backup_20260504,
    exchange_analytics.kline_fact_v2 TO exchange_analytics.kline_fact;