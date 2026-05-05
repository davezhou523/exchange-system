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
- `reason_counts=fallback_1m_only`：高周期当前不可用，这一轮先用 `1m` 判态结果兜底，不代表异常，更像启动期或恢复期的过渡状态。
- `reason_counts=m15_only`：统一判势主要由 `15m` 层单独给出结论，高周期确认还不强。
- `reason_counts=m15_confirm_override`：`15m` 结论得到确认后覆盖了原始主判断，说明短周期确认在这一轮占主导。

- 查 `fallback_1m_only` 时，先看 `fallback_missing_interval_descs`：先确认到底是 `1h`、`15m` 还是两者同时不可用。
- 再看 `fallback_interval_reason_descs`：这里回答“为什么不可用”，常见就是 `快照仍为脏数据`、`快照尚未最终确认`、`快照当前不可交易`、`输入特征已过期`。
- 最后看 `fallback_interval_age_sec`：如果原因是 `输入特征已过期`，再用 age 秒数判断是短暂延迟还是已经明显断流；如果原因是 `脏数据/未最终确认`，就优先回查对应周期 K 线快照状态。

## Weights 速查

- `template=range-core`：当前 symbol 最终落到震荡核心模板。
- `route_bucket=range`：当前 symbol 被分配到震荡策略桶。
- `route_reason=market_state_range`：统一判态认为当前更适合走震荡策略桶。
- `score_source=symbol_score`：当前权重分数优先使用 symbol score，而不是退回 `regime_analysis` 或默认值。
- `position_budget`：这轮真正建议给该 symbol 的最终仓位预算，已经综合了策略桶权重、币种权重和风险缩放。

## Decision Reason 速查

- `reason_code=h4_not_ready`：4小时 EMA 结构还没准备好，趋势主方向暂时无法判定。
- `reason_code=h1_not_ready`：1小时 EMA 结构还没准备好，回调确认层暂时无法判定。
- `reason_code=h4_no_trend`：4小时没有形成明确趋势，普通趋势策略本轮不入场。
- `reason_code=h1_no_pullback`：4小时已有方向，但1小时还没走出符合要求的回调。

- `reason_code=m15_no_entry`：普通趋势 15 分钟入场总开关未通过，建议继续看 `reject_descs`。
- `reason_code=m15_history_insufficient`：15分钟历史K线不足，无法判断近期突破位。
- `reason_code=m15_long_structure_missing`：做多时价格没突破近期高点。
- `reason_code=m15_long_rsi_signal_missing`：做多时 RSI 没上穿 50，且也没达到偏强阈值。
- `reason_code=m15_short_structure_missing`：做空时价格没跌破近期低点。
- `reason_code=m15_short_rsi_signal_missing`：做空时 RSI 没下穿 50，且也没达到偏弱阈值。

- `reason_code=breakout_no_entry`：突破策略入场总开关未通过，建议继续看 `breakout_reject_descs` 或 `reject_descs`。
- `reason_code=breakout_no_price_break`：价格未真正突破前高/前低。
- `reason_code=breakout_volume_low`：量能不足，放量确认没通过。
- `reason_code=breakout_long_rsi_low`：多头突破时 RSI 强度不够。
- `reason_code=breakout_short_rsi_high`：空头突破时 RSI 弱度不够。
- `reason_code=breakout_ema_misaligned`：突破方向与均线结构不一致。

- `reason_code=range_no_entry`：震荡策略入场总开关未通过，建议继续看 `reject_descs`。
- `reason_code=range_h4_oscillation_missing`：4小时震荡评分不足，当前更像趋势环境。
- `reason_code=range_h1_adx_too_high`：1小时 ADX 偏高，区间环境不够干净。
- `reason_code=range_h1_boll_too_wide`：1小时布林带过宽，区间约束不足。
- `reason_code=range_h1_boll_too_narrow`：1小时布林带过窄，波动空间不足。
- `reason_code=range_h1_middle_zone`：价格在1小时区间中部，不是边缘反转位。
- `reason_code=range_long_rsi_signal_missing`：做多侧超卖或反弹信号不足。
- `reason_code=range_long_rsi_turn_missing`：做多侧 RSI 还没拐头向上。
- `reason_code=range_long_bb_bounce_missing`：做多侧还没出现下轨回收确认。
- `reason_code=range_long_signal_count_low`：做多侧聚合信号数不足。
- `reason_code=range_long_not_near_low`：价格还没贴近区间下沿。
- `reason_code=range_short_rsi_signal_missing`：做空侧超买或回落信号不足。
- `reason_code=range_short_rsi_turn_missing`：做空侧 RSI 还没拐头向下。
- `reason_code=range_short_bb_bounce_missing`：做空侧还没出现上轨回收确认。
- `reason_code=range_short_signal_count_low`：做空侧聚合信号数不足。
- `reason_code=range_short_not_near_high`：价格还没贴近区间上沿。

- grep `reason_code` 时，先看主码，再看同一行里的 `summary=...` 或 JSON 里的 `reject_descs`，通常比只盯一个总原因更快。

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