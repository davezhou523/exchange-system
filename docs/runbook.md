# 值班入口页

- 仓库首页见 [README.md](file:///Users/bytedance/GolandProjects/exchange-system/README.md)
- 完整架构说明见 [architecture.md](file:///Users/bytedance/GolandProjects/exchange-system/docs/architecture.md)

## UniversePool 速查卡

- 先看 `_meta`：先判断“没输入”还是“有输入但没命中”
- `snapshot_count=0` 或 `fresh_count=0`：优先怀疑断流、freshness 不一致或 snapshot 已过期
- `global_state=unknown` 且 count 全 0：优先怀疑状态规则没命中
- `global_state=range` 且 `reason=state_filtered`：通常不是故障，而是非偏好币被正常过滤
- `reason=stale_snapshot`：先查输入链路，不要先改 selector 阈值
- `reason=state_preferred_score_pass`：说明偏好币在当前状态下已被放行

## 常用命令

- 看 `_meta`

```bash
tail -n 20 app/market/rpc/data/universepool/_meta/$(date -u +%F).jsonl
```

- 看 `selector_decision`

```bash
grep '"action":"selector_decision"' \
  app/market/rpc/data/universepool/{BNBUSDT,SOLUSDT,XRPUSDT}/$(date -u +%F).jsonl | tail -n 30
```

- 看 `ws / aggregator / universepool`

```bash
grep '\[ws\]\|\[aggregated\]\|\[aggregated 5m emit\]\|\[universepool\]' \
  app/market/rpc/logs/market.log | tail -n 50
```

## 使用顺序

- 第一步：先看 `_meta`，确认有没有新输入、当前是不是 `unknown`
- 第二步：再看 `selector_decision`，确认是 `state_filtered`、`stale_snapshot` 还是 `state_preferred_score_pass`
- 第三步：最后看 `ws / aggregator`，确认真实消息、聚合输出、snapshot 更新是否连通

## 一句话原则

- 先查输入有没有进来，再查 selector 有没有命中，最后才考虑要不要调阈值。

## 详细版

- 完整值班手册见 [architecture.md](file:///Users/bytedance/GolandProjects/exchange-system/docs/architecture.md) 的 `3.3.6 Market UniversePool 判读标准`
- 仓库首页速查版见 [README.md](file:///Users/bytedance/GolandProjects/exchange-system/README.md)