# 旧 3m/5m 数据清理方案

当前 `market` 聚合器只继续产出 `1m/15m/1h/4h`，不再生成新的 `3m/5m` K 线。

为了避免历史遗留目录和 ClickHouse 数据继续误导排查，建议在确认线上已完成版本切换后，按下面步骤清理旧 `3m/5m` 数据。

## 清理前

1. 确认新版本已上线

- 确认 `market` 已使用去掉 `3m/5m` 聚合的版本
- 确认近期日志里不再出现 `interval=3m` 或 `interval=5m`

2. 暂停写入

- 建议先暂停 `market` 服务或至少暂停其对 ClickHouse 的写入
- 避免清理过程中又写入新的历史残留数据

3. 评估保留期

- 如果还需要保留一段时间做审计，可先只做“逻辑屏蔽”，暂不物理删除
- 如果确认不再需要 `3m/5m`，再执行下面的删除动作

## 本地目录清理

### market 本地 K 线目录

```bash
find /Users/bytedance/GolandProjects/exchange-system/app/market/rpc/data/kline \
  -type d \( -name 3m -o -name 5m \) -prune -print
```

确认输出无误后执行删除：

```bash
find /Users/bytedance/GolandProjects/exchange-system/app/market/rpc/data/kline \
  -type d \( -name 3m -o -name 5m \) -prune -exec rm -rf {} +
```

### strategy 本地 K 线目录

```bash
find /Users/bytedance/GolandProjects/exchange-system/app/market/rpc/data/kline \
  -type d \( -name 3m -o -name 5m \) -prune -print
```

确认输出无误后执行删除：

```bash
find /Users/bytedance/GolandProjects/exchange-system/app/market/rpc/data/kline \
  -type d \( -name 3m -o -name 5m \) -prune -exec rm -rf {} +
```

### 共享 warmup 目录

当前 shared warmup 目录的启动行为如下：

```text
demo:
• market.demo.yaml 默认 WarmupCleanupOnStartup: true
• 每次启动 market 会先清空 runtime/shared/kline/warmup
• 适合排查 warmup 分页、连续性和 strategy 启动恢复问题

prod:
• market.prod.yaml 默认 WarmupCleanupOnStartup: false
• 启动 market 不会主动清空共享 warmup
• 适合保留上一轮可用快照，降低 warmup 失败时的恢复风险
```

如果历史上曾把 `3m/5m` 预热数据写入共享目录，也建议一起清理：

```bash
find /Users/bytedance/GolandProjects/exchange-system/runtime/shared/kline/warmup \
  -type d \( -name 3m -o -name 5m \) -prune -print
```

确认输出无误后执行删除：

```bash
find /Users/bytedance/GolandProjects/exchange-system/runtime/shared/kline/warmup \
  -type d \( -name 3m -o -name 5m \) -prune -exec rm -rf {} +
```

## ClickHouse 数据清理

### 先做数量确认

```sql
SELECT
    interval,
    count() AS row_count,
    min(open_time) AS min_open_time,
    max(open_time) AS max_open_time
FROM exchange_analytics.kline_fact
WHERE interval IN ('3m', '5m')
GROUP BY interval
ORDER BY interval;
```

### 执行删除

如果确认这些历史数据不再保留，可执行：

```sql
ALTER TABLE exchange_analytics.kline_fact
    DELETE WHERE interval IN ('3m', '5m');
```

如果希望同步等待 mutation 完成，可在支持的执行器里附带：

```sql
SET mutations_sync = 1;

ALTER TABLE exchange_analytics.kline_fact
    DELETE WHERE interval IN ('3m', '5m');
```

### 删除后复核

```sql
SELECT
    interval,
    count() AS row_count
FROM exchange_analytics.kline_fact
WHERE interval IN ('3m', '5m')
GROUP BY interval
ORDER BY interval;
```

期望结果：

```text
0 rows
```

## 排查口径同步

完成清理后，建议排查时统一只关注以下周期：

- `1m`
- `15m`
- `1h`
- `4h`

常用日志过滤也建议统一成：

```bash
grep '\[ws\]\|\[aggregated\]\|\[universepool\]' \
  /Users/bytedance/GolandProjects/exchange-system/app/market/rpc/logs/market.log | tail -n 50
```

## 风险提示

- 本地目录删除是不可恢复操作，先确认没有再做历史追溯需求
- ClickHouse `ALTER TABLE ... DELETE` 是异步 mutation，大表执行时可能持续一段时间
- 若线上仍有旧版本服务在写 `3m/5m`，清理后它们还会继续产生残留数据