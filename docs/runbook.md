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

## MarketState Reason 速查

- `global_reason=no_results`：这一轮没有可用于汇总的 marketstate 结果，通常先查输入链路或上游是否还没产出结果。
- `global_reason=all_unknown`：这一轮有结果，但全部落在 `unknown`，说明没有形成可用的全局状态。
- `global_reason=dominant_match_surface`：全局状态优先由统一判势命中面主导，健康且新鲜的 `analysis` 中某个形态命中数最多，例如 `range` 命中面覆盖了大多数币。
- `global_reason=dominant_state`：没有明显命中面优势时，退回按最终 `result.state` 分布选主导状态。

- `reason_counts=insufficient_features`：输入特征不足，通常是所需周期历史还没准备好。
- `reason_counts=unhealthy_data`：输入数据本身不健康，通常要回查上游快照完整性。
- `reason_counts=stale_features`：输入特征已过期，先查消息是否断流或 freshness 窗口是否过严。
- `reason_counts=missing_trend_features`：趋势判断所需字段不足，通常是高周期 EMA/ATR 等还没齐。
- `reason_counts=atr_pct_high`：波动率偏高，当前更偏向突破态。
- `reason_counts=atr_pct_low`：波动率偏低，当前更偏向震荡态。
- `reason_counts=ema_bull_alignment`：EMA 多头排列，当前更偏向上升趋势。
- `reason_counts=ema_bear_alignment`：EMA 空头排列，当前更偏向下降趋势。
- `reason_counts=fallback_range`：既不满足突破也不满足严格趋势时，回退归类到震荡。
- `reason_counts=m15_only`：统一判势主要由 `15m` 层单独给出结论，高周期确认还不强。
- `reason_counts=m15_confirm_override`：`15m` 结论得到确认后覆盖了原始主判断，说明短周期确认在这一轮占主导。

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
grep '\[ws\]\|\[aggregated\]\|\[universepool\]' \
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
- 旧 `3m/5m` 数据清理方案见 [kline_interval_cleanup.md](file:///Users/bytedance/GolandProjects/exchange-system/docs/kline_interval_cleanup.md)