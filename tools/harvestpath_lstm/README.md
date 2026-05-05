# Harvest Path LSTM

这个目录提供一套独立的 LSTM 训练与离线风险识别脚本，用现有多周期 JSONL K 线识别未来短窗口内的流动性扫损风险。

## 目标

- 输入：最近一段以 `1m` 为主、`5m + 15m` 为上下文的多周期 K 线窗口
- 输出：未来 `lookahead` 根 `1m` K 线中出现明显插针并回收的危险时刻概率
- 结果字段：`harvest_path_probability`

## 标签定义

脚本会把当前 K 线后的未来 `lookahead` 根 1m 数据作为危险时刻识别目标。

当满足以下任一条件时，标记为正样本：

1. 未来最高价相对当前收盘价向上扩张超过 `spike_atr_multiple * ATR`
2. 并且未来窗口末端收盘价已经回收到 `reversal_atr_multiple * ATR` 以内

或者：

1. 未来最低价相对当前收盘价向下扩张超过 `spike_atr_multiple * ATR`
2. 并且未来窗口末端收盘价已经回收到 `reversal_atr_multiple * ATR` 以内

这是一版可训练的近似标签，适合先把模型闭环跑通。

## 特征

脚本会先以 `1m` 作为主时间轴，再把最近已闭合的 `5m`、`15m` K 线对齐到每个 `1m` 样本点。

每个周期都会提取这些特征：

- `log_return`
- `range_over_atr`
- `body_over_atr`
- `upper_wick_over_atr`
- `lower_wick_over_atr`
- `atr_pct`
- `ema_spread_pct`
- `close_to_ema21_pct`
- `close_to_ema55_pct`
- `rsi_centered`
- `volume_zscore`
- `quote_volume_zscore`
- `trade_count_log`
- `taker_buy_ratio`

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tools/harvestpath_lstm/requirements.txt
```

## 训练

```bash
python3 tools/harvestpath_lstm/train.py \
  --data-dir app/market/rpc/data/kline \
  --symbol ETHUSDT \
  --intervals 1m,5m,15m \
  --window-size 64 \
  --lookahead 8 \
  --rolling-train-ratio 0.6 \
  --rolling-val-ratio 0.2 \
  --rolling-step-ratio 0.1 \
  --max-windows 6 \
  --epochs 20
```

训练完成后会输出：

- `tools/harvestpath_lstm/artifacts/ethusdt_harvest_path_lstm.pt`
- `tools/harvestpath_lstm/artifacts/ethusdt_harvest_path_lstm.json`

默认训练方式是 `rolling window`：

- 每个窗口使用一段历史做训练
- 后面紧跟一段时间做验证
- 再向前滚动到下一窗口
- 最终会把最后一个滚动窗口的最佳模型作为上线 artifact
- 每个窗口都会输出 `accuracy / precision / recall / f1`，不只看 `loss`
- 每个窗口还会做阈值扫描报告，默认扫描 `0.30 / 0.40 / 0.50 / 0.60 / 0.70`
- metadata 会保存每个阈值下的指标，以及 `f1` 最优阈值
- metadata 还会生成 `threshold_stability_report`，汇总多个窗口的最佳阈值是否稳定
- 稳定性会给出 `STABLE / MODERATE / UNSTABLE`，方便直接判断上线阈值是否漂移
- metadata 还会生成 `threshold_recommendation_report`
- 会分别给出 `precision 优先 / recall 优先 / f1 平衡` 三种上线阈值建议
- metadata 还会生成 `segmented_performance_report`
- 会按 `月份` 和 `波动 regime（LOW / MID / HIGH）` 分层展示三种推荐阈值下的表现
- metadata 还会生成 `regime_aware_threshold_strategy`
- 会为 `LOW / MID / HIGH` 分别给出可直接上线使用的推荐阈值，并保留全局 fallback 阈值

## 风险识别

```bash
python3 tools/harvestpath_lstm/predict.py \
  --data-dir app/market/rpc/data/kline \
  --symbol ETHUSDT
```

输出示例：

```json
{
  "symbol": "ETHUSDT",
  "model": "harvest_path_liquidity_sweep_risk_v1",
  "harvest_path_probability": 0.742381,
  "window_size": 64,
  "lookahead": 8
}
```

## 当前定位

- 这是离线训练与离线风险识别版本
- 还没有直接接入 `market` 内嵌策略引擎的在线决策链
- 如果下一步要接入 Go 服务，建议把模型导出成 ONNX 或者先做一个 Python 风险识别 sidecar

## 先训练出 LSTM artifact：

```bash

python3 tools/harvestpath_lstm/train.py \
--data-dir app/market/rpc/data/kline \
--symbol ETHUSDT \
--intervals 1m,5m,15m \
--window-size 64 \
--lookahead 8 \
--rolling-train-ratio 0.6 \
--rolling-val-ratio 0.2 \
--rolling-step-ratio 0.1 \
--max-windows 6 \
--epochs 20


```