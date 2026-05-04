-- kline_fact 迁移后的核对 SQL。
-- 使用前提：
-- 1. 已执行 20260504_02_exchange_analytics_kline_fact_replacing_merge_tree.sql；
-- 2. 当前正式表名为 exchange_analytics.kline_fact；
-- 3. 迁移前旧表已备份为 exchange_analytics.kline_fact_backup_20260504。

-- 1) 检查新表是否还存在重复键。
-- 期望：
-- - duplicate_key_count = 0
-- - duplicate_row_count = 0
SELECT
    count() AS duplicate_key_count,
    sum(row_count - 1) AS duplicate_row_count
FROM
(
    SELECT
        symbol,
        interval,
        open_time,
        count() AS row_count
    FROM exchange_analytics.kline_fact
    GROUP BY
        symbol,
        interval,
        open_time
    HAVING row_count > 1
);

-- 2) 对比备份表与新表的逻辑唯一键数量是否一致。
-- 期望：
-- - backup_unique_key_count = active_unique_key_count
-- - unique_key_delta = 0
SELECT
    backup_unique_key_count,
    active_unique_key_count,
    active_unique_key_count - backup_unique_key_count AS unique_key_delta
FROM
(
    SELECT count() AS backup_unique_key_count
    FROM
    (
        SELECT
            symbol,
            interval,
            open_time
        FROM exchange_analytics.kline_fact_backup_20260504
        GROUP BY
            symbol,
            interval,
            open_time
    )
) AS backup
CROSS JOIN
(
    SELECT count() AS active_unique_key_count
    FROM
    (
        SELECT
            symbol,
            interval,
            open_time
        FROM exchange_analytics.kline_fact
        GROUP BY
            symbol,
            interval,
            open_time
    )
) AS active;

-- 3) 全量抽查：对比“备份表中按 event_time 最新版本”与“新表当前版本”是否一致。
-- 期望：
-- - mismatch_key_count = 0
SELECT
    count() AS mismatch_key_count
FROM
(
    SELECT
        symbol,
        interval,
        open_time,
        max(event_time) AS expected_event_time
    FROM exchange_analytics.kline_fact_backup_20260504
    GROUP BY
        symbol,
        interval,
        open_time
) AS latest_from_backup
LEFT JOIN exchange_analytics.kline_fact AS active
    ON active.symbol = latest_from_backup.symbol
   AND active.interval = latest_from_backup.interval
   AND active.open_time = latest_from_backup.open_time
WHERE active.event_time != latest_from_backup.expected_event_time
   OR active.event_time IS NULL;

-- 4) 指定 symbol + interval + open_time 核对是否保留了最新 event_time 版本。
-- 使用方式：
-- - 把下面 WITH 中的 symbol / interval / open_time 改成你要核对的值。
WITH
    'BTCUSDT' AS target_symbol,
    '1h' AS target_interval,
    toDateTime64('2026-05-04 07:00:00', 3, 'UTC') AS target_open_time
SELECT
    latest_from_backup.symbol,
    latest_from_backup.interval,
    latest_from_backup.open_time,
    latest_from_backup.expected_event_time,
    active.event_time AS active_event_time,
    active.is_dirty AS active_is_dirty,
    active.dirty_reason AS active_dirty_reason,
    active.is_final AS active_is_final,
    active.is_tradable AS active_is_tradable,
    active.close AS active_close,
    active.volume AS active_volume
FROM
(
    SELECT
        symbol,
        interval,
        open_time,
        max(event_time) AS expected_event_time
    FROM exchange_analytics.kline_fact_backup_20260504
    WHERE symbol = target_symbol
      AND interval = target_interval
      AND open_time = target_open_time
    GROUP BY
        symbol,
        interval,
        open_time
) AS latest_from_backup
LEFT JOIN exchange_analytics.kline_fact AS active
    ON active.symbol = latest_from_backup.symbol
   AND active.interval = latest_from_backup.interval
   AND active.open_time = latest_from_backup.open_time;

-- 5) 查看指定键在备份表中的完整版本历史，便于人工核对迁移前后的保留版本。
-- 使用方式：
-- - 与上面同样替换 target_symbol / target_interval / target_open_time。
WITH
    'BTCUSDT' AS target_symbol,
    '1h' AS target_interval,
    toDateTime64('2026-05-04 07:00:00', 3, 'UTC') AS target_open_time
SELECT
    event_time,
    symbol,
    interval,
    open_time,
    close_time,
    is_dirty,
    dirty_reason,
    is_final,
    is_tradable,
    close,
    volume,
    source
FROM exchange_analytics.kline_fact_backup_20260504
WHERE symbol = target_symbol
  AND interval = target_interval
  AND open_time = target_open_time
ORDER BY event_time DESC;