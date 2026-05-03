# 突破策略 (Breakout Strategy) 技术文档

> 版本: v1 | 目标: 当前 Golang 实现对齐说明 | 实现入口: `app/strategy/rpc/internal/strategy/breakout.go`

---

## 一、策略概述

突破策略的核心思想是：**在关键区间被真实突破时顺势跟进，而不是在震荡区间内做均值回归。**

当前实现不是独立 worker，而是作为 `trend_following` 的一个运行时变体存在。其最小闭环已经具备：

- 用最近一段历史窗口计算区间高点和低点
- 用 ATR 缓冲带过滤“刚刚碰到边界”的假信号
- 用 RSI、EMA 结构和量比进一步过滤假突破
- 复用通用的仓位、风控、决策日志和信号发送链路

---

## 二、当前实现结构

### 2.1 15m 模式

默认使用 `15m` 作为突破入场周期：

1. 读取当前 `latest15m`
2. 检查 `ATR` 和 `RSI` 是否就绪
3. 回看最近 N 根 `15m` 历史，排除当前 K 线后计算：
   - `recent_high`
   - `recent_low`
   - `avg_volume`
4. 生成突破统计量：
   - `breakout_buffer = ATR * breakout_entry_buffer_atr`
   - `volume_ratio = current_volume / avg_volume`
5. 满足向上或向下突破条件后开仓

### 2.2 1m 模式

当 `signal_mode = 1` 时，突破策略也支持 `1m` 入场：

- 主入场周期切到 `1m`
- 历史窗口改用 `1m` K 线
- 止损 ATR 倍数改读 `1m_atr_multiplier`
- 其余核心过滤逻辑与 `15m` 版本保持一致

---

## 三、入场判定

### 3.1 向上突破

当前实现要求同时满足：

- `close > recent_high + breakout_buffer`
- `RSI >= breakout_rsi_long_min`
- `close >= ema21 >= ema55`，当 `breakout_require_ema_trend = 1`
- `volume_ratio >= breakout_volume_ratio_min`

满足后生成 `LONG` 开仓信号。

### 3.2 向下突破

当前实现要求同时满足：

- `close < recent_low - breakout_buffer`
- `RSI <= breakout_rsi_short_max`
- `close <= ema21 <= ema55`，当 `breakout_require_ema_trend = 1`
- `volume_ratio >= breakout_volume_ratio_min`

满足后生成 `SHORT` 开仓信号。

### 3.3 不开仓时的可观测性

未满足条件时，`breakout.go` 会把下面这些结构化字段写进 decision log，方便排查：

- `breakout_recent_high`
- `breakout_recent_low`
- `breakout_avg_volume`
- `breakout_volume_ratio`
- `breakout_buffer_atr`
- `breakout_up`
- `breakout_down`
- `breakout_long_rsi_ok`
- `breakout_short_rsi_ok`
- `breakout_long_ema_ok`
- `breakout_short_ema_ok`
- `breakout_volume_ok`

---

## 四、风控与出场

突破策略当前复用通用开仓和出场链路，没有单独维护一套 exit worker：

- 止损距离：`ATR * stop_loss_atr_multiplier`
- `1m` 模式下止损距离：`ATR * 1m_atr_multiplier`
- `TP1 = 1 x stopDist`
- `TP2 = 2 x stopDist`
- 命中 `TP1` 后移动止损到保本
- 后续出场继续复用已有的止损、止盈、EMA 破位逻辑

同时仍复用通用能力：

- 仓位计算
- `weights` 预算缩放
- `HarvestPath` 风险过滤
- `signal` / `decision` 日志

---

## 五、关键参数

当前突破策略实现最关键的参数如下：

| 参数名 | 作用 | 默认值 |
|------|------|------|
| `strategy_variant` | 选择突破策略变体 | `1` |
| `signal_mode` | 选择 `15m` 或 `1m` 入场模式 | `0` |
| `m15_breakout_lookback` | `15m` 突破回看窗口 | `6` |
| `1m_breakout_lookback` | `1m` 突破回看窗口 | `20` |
| `breakout_volume_ratio_min` | 最低量比阈值 | `1.20` |
| `breakout_entry_buffer_atr` | 突破缓冲带 ATR 倍数 | `0.10` |
| `breakout_rsi_long_min` | 向上突破 RSI 下限 | `55` |
| `breakout_rsi_short_max` | 向下突破 RSI 上限 | `45` |
| `breakout_require_ema_trend` | 是否要求均线顺势排列 | `1` |
| `stop_loss_atr_multiplier` | `15m` 模式止损 ATR 倍数 | `1.5` |
| `1m_atr_multiplier` | `1m` 模式止损 ATR 倍数 | `1.5` |

---

## 六、当前实现边界

这份文档描述的是**当前真实代码实现**，不是目标架构里的理想版突破系统。当前仍有这些边界：

- `breakout.go` 仍作为 `trend_following` 的策略变体运行
- 还没有独立的 breakout worker / router / allocator
- 过滤条件仍偏最小可运行集，重点在降低假突破，而不是做复杂结构分类
- 上游仍依赖统一的 `trend / breakout / range` 路由和模板切换

---

## 七、代码落点

- 实现文件: [breakout.go](file:///Users/bytedance/GolandProjects/exchange-system/app/strategy/rpc/internal/strategy/breakout.go)
- 策略主实例: [trend_following.go](file:///Users/bytedance/GolandProjects/exchange-system/app/strategy/rpc/internal/strategy/trend_following.go)
- 架构入口说明: [architecture.md](file:///Users/bytedance/GolandProjects/exchange-system/docs/architecture.md)