# kline_fact ReplacingMergeTree 上线执行清单

本文用于指导 `kline_fact` 从 `MergeTree` 迁移到 `ReplacingMergeTree(event_time)`。

相关文件：

- 迁移脚本：`deploy/clickhouse/migrations/20260504_02_exchange_analytics_kline_fact_replacing_merge_tree.sql`
- 核对脚本：`deploy/clickhouse/migrations/20260504_03_exchange_analytics_kline_fact_replacing_merge_tree_verify.sql`

## 迁移目标

- 同一组 `(symbol, interval, open_time)` 只保留 `event_time` 最新版本
- 分区改为 `toDate(open_time)`，避免跨天补写时同一根 K 线分散到不同分区

## 迁移前

1. 确认业务低峰窗口

- 选择 `market` 写 ClickHouse 压力较低的时间段执行
- 预留足够时间完成迁移和核对

2. 确认写入侧进入可控状态

- 建议暂停 `market` 写入，或至少确保迁移窗口内不再向 `exchange_analytics.kline_fact` 持续追加数据
- 如果不能停写，不建议直接执行该迁移，因为回填和切表期间会产生新旧表不一致

3. 确认备份表名未占用

- 迁移脚本默认把旧表重命名为 `exchange_analytics.kline_fact_backup_20260504`
- 若线上已存在同名表，先修改迁移脚本里的备份表名，再执行

4. 记录迁移前基线

- 建议先执行以下 SQL，保存当前重复情况，便于迁移后对比

```sql
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
    GROUP BY symbol, interval, open_time
    HAVING row_count > 1
);
```

## 迁移中

1. 执行迁移脚本

- 执行 `deploy/clickhouse/migrations/20260504_02_exchange_analytics_kline_fact_replacing_merge_tree.sql`
- 该脚本会完成以下动作：

```text
创建 kline_fact_v2
按 event_time 最新版本回填旧数据
把旧表改名为 kline_fact_backup_20260504
把新表切换为正式表名 kline_fact
```

2. 关注执行结果

- `CREATE TABLE exchange_analytics.kline_fact_v2` 成功
- `INSERT INTO exchange_analytics.kline_fact_v2 ... SELECT ... GROUP BY ...` 成功
- `RENAME TABLE ...` 成功

3. 若迁移失败

- 如果失败发生在 `RENAME TABLE` 之前：
  - 正式表仍是旧表 `exchange_analytics.kline_fact`
  - 可先排查并修复，再重新执行
- 如果失败发生在 `RENAME TABLE` 期间：
  - 先执行 `SHOW TABLES FROM exchange_analytics`
  - 确认当前正式表名、备份表名和 `kline_fact_v2` 的实际状态
  - 再决定是否补执行 rename

## 迁移后

1. 核对新表重复键数量

- 执行核对脚本第 1 段
- 期望结果：

```text
duplicate_key_count = 0
duplicate_row_count = 0
```

2. 核对备份表与新表逻辑唯一键数量

- 执行核对脚本第 2 段
- 期望结果：

```text
backup_unique_key_count = active_unique_key_count
unique_key_delta = 0
```

3. 核对新表是否保留了备份表中的最新 event_time 版本

- 执行核对脚本第 3 段
- 期望结果：

```text
mismatch_key_count = 0
```

4. 对现场关注样本做点查

- 执行核对脚本第 4 段，把以下参数替换成你要检查的键：
  - `target_symbol`
  - `target_interval`
  - `target_open_time`
- 期望结果：

```text
active_event_time = expected_event_time
```

5. 回看某个键的历史版本链路

- 执行核对脚本第 5 段
- 用于人工判断：
  - 旧表里是否确实存在 `incomplete_bucket` / `worker_gc` / 最终版本
  - 新表保留的是否真的是最新 `event_time` 对应版本

## 恢复写入

1. 恢复 `market` 写入前再做一次抽查

- 建议随机抽查几组历史上重复较多的键
- 确认新表查询结果符合预期后，再恢复写入

2. 恢复写入后观察

- 重点观察新写入是否正常
- 重点观察是否仍有大量重复键持续出现

3. 关于 ReplacingMergeTree 的预期

- `ReplacingMergeTree(event_time)` 不是插入瞬间立即物理去重
- 在后台 merge 前，极少数情况下直接扫表仍可能短暂看到重复版本
- 若查询要求强一致，可按场景使用 `FINAL` 或继续使用按 `event_time` 取最新版本的查询方式

## 回滚说明

1. 若迁移后发现异常，旧表仍保留在：

```text
exchange_analytics.kline_fact_backup_20260504
```

2. 回滚思路

- 暂停写入
- 把当前 `exchange_analytics.kline_fact` 改名为异常表
- 再把 `exchange_analytics.kline_fact_backup_20260504` 改回 `exchange_analytics.kline_fact`

3. 回滚前确认

- 先确认迁移后新增写入是否已经进入新表
- 如已进入，回滚前需要先评估是否要补导这些新数据