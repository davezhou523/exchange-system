# Strategy5 v4 — 加密货币趋势跟踪策略技术规范

> **版本**: v4.1 | **适用**: 任意自动交易系统 | **更新**: 2026-05-05
> 
> 本文档是 Strategy5 v4 策略的**完整独立规范**。任何开发者可仅依据本文档，在任意编程语言和交易平台上实现该策略，无需参考原始代码。

---

## 目录

1. [策略概述](#一策略概述)
2. [系统接口规范](#二系统接口规范)
3. [数据模型](#三数据模型)
4. [指标计算规范](#四指标计算规范)
5. [核心算法](#五核心算法)
6. [主循环流程](#六主循环流程)
7. [交易后处理](#七交易后处理)
8. [多时间周期数据同步](#八多时间周期数据同步)
9. [入场决策矩阵](#九入场决策矩阵)
10. [持仓管理生命周期](#十持仓管理生命周期)
11. [参数完整参考](#十一参数完整参考)
12. [交易成本模型](#十二交易成本模型)
13. [初始化与预热](#十三初始化与预热)
14. [关键实现注意事项](#十四关键实现注意事项)
15. [回测验证结果](#十五回测验证结果)

---

## 一、策略概述

### 1.1 定位

多时间周期趋势跟踪策略，在趋势中通过回调或交叉寻找高盈亏比入场点。

**核心设计哲学**：收益/胜率/夏普必须优秀前提下提高交易次数。

### 1.2 交易框架

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1 — 4H 趋势方向判断                                    │
│  价格 > EMA21 > EMA55 → 只做多                                │
│  价格 < EMA21 < EMA55 → 只做空                                │
│  EMA缠绕              → 不交易                                │
└────────────────────────┬────────────────────────────────────┘
                         │ 趋势方向
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Layer 2 — 1H 入场位置判断（二选一）                            │
│                                                              │
│  模式A（标准回调）              模式C（趋势交叉）               │
│  价格在EMA21-EMA55带内          1H EMA21刚穿越EMA55           │
│  EMA21方向一致                  ADX ≥ 25 + MACD方向确认       │
│  RSI 42-60 / 40-58             RSI 42-60 / 40-58（相同）      │
│  仓位 1.0× / 0.9×(深回调)      仓位 0.9×（固定缩放）           │
└────────────────────────┬────────────────────────────────────┘
                         │ 入场模式 + 仓位缩放
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Layer 3 — 15M 入场信号触发                                    │
│  结构突破: 收盘破前高/前低                                      │
│  RSI穿越: RSI上/下穿50                                        │
│  RSI偏置: RSI ≥ 52(多) / ≤ 48(空)                            │
│  组合方式: OR（任一触发即可）                                   │
└────────────────────────┬────────────────────────────────────┘
                         │ 入场信号
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Layer 4 — 风控 + 仓位计算 + 执行                              │
│  止损 = 1.5 × ATR(15M)                                       │
│  单笔风险 ≤ 3%                                                │
│  仓位 = min(风险仓位, 现金上限, 杠杆上限) × 缩放系数             │
└────────────────────────┬────────────────────────────────────┘
                         │ 持仓中
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Layer 5 — 持仓管理                                           │
│  +1R → 止损移到成本（保本）                                     │
│  +3R → 止盈一半                                               │
│  EMA21破位（连续2根确认）→ 全部平仓                             │
│  回撤 ≥ 15% → 新仓位缩放至50%                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、系统接口规范

实现本策略需要提供以下接口。具体签名用通用描述表示，可映射到任意语言。



### 2.2 策略对外输出的信号

策略在每个 tick（15M K线收盘）输出**最多一个**信号，或无信号：

| 信号 | 触发条件 | 语义 |
|------|---------|------|
| `OPEN_LONG` | 入场做多 | 以当前收盘价买入指定数量 |
| `OPEN_SHORT` | 入场做空 | 以当前收盘价卖出指定数量 |
| `CLOSE_HALF` | +3R部分止盈 | 平掉当前持仓的50% |
| `CLOSE_ALL` | 止损/EMA破位全平 | 平掉当前全部持仓 |
| `null` | 无动作 | 本tick不操作 |




### 2.3 策略调用时序

```
系统启动
  │
  ├── 调用 StrategyInit(config)         // 初始化策略状态
  │
  └── 进入K线循环 ──────────────────────────────────────────┐
        │                                                    │
        ├── 15M K线收盘事件触发                                │
        │                                                    │
        ├── 同步1H/4H已完成K线指标                             │
        │                                                    │
        ├── signal = OnCandle(m15, h4_ind, h1_ind, m15_ind)  │
        │                                                    │
        ├── IF signal ≠ null:                                │
        │     执行信号 → 订单成交后调用 OnOrderFilled(pnl)     │
        │                                                    │
        └── 等待下一根15M K线 ───────────────────────────────┘
```

---

## 三、数据模型

### 3.1 K线（OHLCV）

| 字段 | 类型 | 说明 |
|------|------|------|
| `open_time` | datetime | K线开盘时间（UTC） |
| `open` | decimal | 开盘价 |
| `high` | decimal | 最高价 |
| `low` | decimal | 最低价 |
| `close` | decimal | 收盘价 |
| `volume` | decimal | 成交量 |

**约束**: 同一周期K线序列按 `open_time` 升序排列，无重复。

### 3.2 枚举类型

```
TrendDirection:
  NONE     = 0   // 数据不足
  BULLISH  = 1   // 多头（价格 > EMA21 > EMA55）
  BEARISH  = 2   // 空头（价格 < EMA21 < EMA55）
  SIDEWAYS = 3   // 震荡（EMA缠绕，不交易）

EntryMode:
  NONE      = 0
  PULLBACK  = 1   // 模式A：标准回调
  CROSSOVER = 2   // 模式C：趋势交叉

PositionSide:
  NONE  = 0
  LONG  = 1
  SHORT = 2

ExitReason:
  NONE       = 0
  STOP_LOSS  = 1   // 止损触发，全平
  BREAKEVEN  = 2   // +1R保本（止损移到成本，不出场）
  PARTIAL_TP = 3   // +3R部分止盈，平半仓
  EMA_BREAK  = 4   // EMA破位，全平
```

### 3.3 策略运行状态

#### 持仓状态

| 字段 | 类型 | 初始值 | 说明 |
|------|------|--------|------|
| `has_position` | bool | false | 是否持仓 |
| `direction` | PositionSide | NONE | 持仓方向 |
| `entry_price` | decimal | 0 | 入场价格 |
| `entry_time` | datetime | null | 入场时间 |
| `entry_mode` | EntryMode | NONE | 入场模式 |
| `stop_loss` | decimal | 0 | 当前止损价 |
| `take_profit_1` | decimal | 0 | +1R止盈价 |
| `take_profit_2` | decimal | 0 | +3R止盈价 |
| `stop_moved_to_cost` | bool | false | 止损是否已移到成本 |
| `partial_tp_done` | bool | false | 是否已执行+3R部分止盈 |
| `bars_since_entry` | int | 0 | 自入场以来15M K线数 |
| `ema_break_count` | int | 0 | EMA破位连续计数 |

#### 仓位缩放

| 字段 | 类型 | 初始值 | 说明 |
|------|------|--------|------|
| `pullback_scale` | float | 1.0 | 当前入场仓位缩放（1.0/0.9） |
| `drawdown_scale` | float | 1.0 | 回撤缩放值（1.0/0.5） |

#### 风险管理

| 字段 | 类型 | 初始值 | 说明 |
|------|------|--------|------|
| `trade_count` | int | 0 | 总交易次数 |
| `win_count` | int | 0 | 盈利交易次数 |
| `consecutive_losses` | int | 0 | 全局连续亏损次数 |
| `daily_consec_losses` | int | 0 | 当日连续亏损次数 |
| `current_day` | date | — | 当前交易日 |
| `daily_start_value` | decimal | — | 当日开始时权益 |
| `max_portfolio_value` | decimal | — | 历史最高权益 |

---

## 四、指标计算规范

> 所有指标使用**已完成K线**的收盘价计算，禁止使用实时未完成K线。

### 4.1 EMA — 指数移动平均

```
输入: 价格序列 price[], 周期 period
输出: ema[]

K = 2 / (period + 1)

初始化:
  ema[0..period-2] = null                    // 无效
  ema[period-1] = SMA(price[0..period-1])    // 首值用SMA

递推 (i ≥ period):
  ema[i] = price[i] × K + ema[i-1] × (1 - K)
```

### 4.2 RSI — 相对强弱指数

```
输入: 收盘价序列 close[], 周期 period (默认14)
输出: rsi[]

计算上涨/下跌:
  up[i]   = max(close[i] - close[i-1], 0)
  down[i] = max(close[i-1] - close[i], 0)

初始化 (i = period):
  avg_up   = SMA(up[1..period])
  avg_down = SMA(down[1..period])

递推 (i > period):
  avg_up   = (avg_up × (period-1) + up[i]) / period      // Wilder平滑
  avg_down = (avg_down × (period-1) + down[i]) / period

RSI计算:
  RS  = avg_up / avg_down
  rsi[i] = 100 - 100 / (1 + RS)

无效值: rsi[0..period-1] = null
```

### 4.3 ATR — 平均真实波幅

```
输入: high[], low[], close[], 周期 period (默认14)
输出: atr[]

真实波幅:
  TR[i] = max(
    high[i] - low[i],
    |high[i] - close[i-1]|,
    |low[i] - close[i-1]|
  )

初始化 (i = period):
  atr[period] = SMA(TR[1..period])

递推 (i > period):
  atr[i] = (atr[i-1] × (period-1) + TR[i]) / period

无效值: atr[0..period-1] = null
注意: TR[0] 无 close[-1]，需跳过或用 high-low 代替
```

### 4.4 MACD — 移动平均收敛发散

```
输入: 收盘价序列 close[]
参数: fast=12, slow=26, signal=9
输出: macd_line[], signal_line[], histogram[]

macd_line[i]   = EMA(close, fast)[i] - EMA(close, slow)[i]
signal_line[i]  = EMA(macd_line, signal)[i]
histogram[i]    = macd_line[i] - signal_line[i]

有效信号线起始: slow + signal - 2 根数据后
```

### 4.5 ADX — 平均趋向指数

```
输入: high[], low[], close[], 周期 period (默认14)
输出: adx[]

Step 1 — 方向移动:
  +DM[i] = high[i] - high[i-1]    若 > (low[i-1] - low[i]) 且 > 0, 否则 0
  -DM[i] = low[i-1] - low[i]      若 > (high[i] - high[i-1]) 且 > 0, 否则 0

Step 2 — 方向指标:
  +DI[i] = EMA(+DM, period)[i] / ATR(period)[i] × 100
  -DI[i] = EMA(-DM, period)[i] / ATR(period)[i] × 100

Step 3 — 趋向指数:
  DX[i] = |+DI[i] - -DI[i]| / (+DI[i] + -DI[i]) × 100

Step 4 — 平均趋向:
  ADX[i] = EMA(DX, period)[i]

有效起始: 2 × period - 1 根数据后
```

---

## 五、核心算法

### 5.1 趋势方向判断

```
函数 GetTrendDirection(h4_close, h4_ema21, h4_ema55) → TrendDirection

前提: 4H数据量 ≥ h4_ema_slow (55)

IF h4_close > h4_ema21 AND h4_ema21 > h4_ema55:
    RETURN BULLISH
IF h4_close < h4_ema21 AND h4_ema21 < h4_ema55:
    RETURN BEARISH
RETURN SIDEWAYS    // EMA缠绕，不交易
```

### 5.2 1H入场条件判断

```
函数 CheckEntryCondition(trend, h1, h4_adx, cfg) → (bool, EntryMode, float)

输入:
  trend     — 趋势方向 (BULLISH / BEARISH)
  h1 = {
    close, ema21, ema55, rsi,
    macd_hist, prev_macd_hist, atr,
    prev_ema21, prev_ema55,
    last_low, last_high         // 可为null
  }
  h4_adx    — 4H ADX值
  cfg       — 策略参数

输出: (是否满足条件, 入场模式, 仓位缩放系数)


IF trend == BULLISH:
    zone_low  = min(ema21, ema55)
    zone_high = max(ema21, ema55)

    // ── 结构完整性 ──
    IF close < zone_low:          RETURN (false, NONE, 0)    // 结构破坏
    IF last_low ≠ null AND close < last_low:
                                   RETURN (false, NONE, 0)    // 反转
    IF ema21 ≤ prev_ema21:        RETURN (false, NONE, 0)    // EMA21须上升

    // ── 模式A：标准回调 ──
    IF zone_low ≤ close ≤ zone_high:
        IF rsi < cfg.h1_rsi_long_low OR rsi > cfg.h1_rsi_long_high:
            RETURN (false, NONE, 0)
        scale = 1.0
        IF |close - ema55| / ema55 ≤ cfg.pullback_deep_band:
            scale = cfg.deep_pullback_scale      // 0.9
        RETURN (true, PULLBACK, scale)

    // ── 模式C：趋势交叉 ──
    IF atr > 0 AND ema21 > ema55 AND prev_ema21 ≤ prev_ema55:
        upper = ema21 + cfg.crossover_atr_distance × atr
        lower = ema21 - 0.3 × atr
        IF lower ≤ close ≤ upper:
            IF h4_adx ≥ cfg.crossover_adx_threshold:
                IF macd_hist > 0 AND macd_hist > prev_macd_hist:
                    IF cfg.h1_rsi_long_low ≤ rsi ≤ cfg.h1_rsi_long_high:
                        RETURN (true, CROSSOVER, cfg.crossover_position_scale)

    RETURN (false, NONE, 0)


IF trend == BEARISH:
    zone_low  = min(ema21, ema55)
    zone_high = max(ema21, ema55)

    // ── 结构完整性 ──
    IF close > zone_high:          RETURN (false, NONE, 0)
    IF last_high ≠ null AND close > last_high:
                                    RETURN (false, NONE, 0)
    IF ema21 ≥ prev_ema21:         RETURN (false, NONE, 0)    // EMA21须下降

    // ── 模式A：标准回调 ──
    IF zone_low ≤ close ≤ zone_high:
        IF rsi < cfg.h1_rsi_short_low OR rsi > cfg.h1_rsi_short_high:
            RETURN (false, NONE, 0)
        scale = 1.0
        IF |close - ema55| / ema55 ≤ cfg.pullback_deep_band:
            scale = cfg.deep_pullback_scale
        RETURN (true, PULLBACK, scale)

    // ── 模式C：趋势交叉 ──
    IF atr > 0 AND ema21 < ema55 AND prev_ema21 ≥ prev_ema55:
        lower = ema21 - cfg.crossover_atr_distance × atr
        upper = ema21 + 0.3 × atr
        IF lower ≤ close ≤ upper:
            IF h4_adx ≥ cfg.crossover_adx_threshold:
                IF macd_hist < 0 AND macd_hist < prev_macd_hist:
                    IF cfg.h1_rsi_short_low ≤ rsi ≤ cfg.h1_rsi_short_high:
                        RETURN (true, CROSSOVER, cfg.crossover_position_scale)

    RETURN (false, NONE, 0)


RETURN (false, NONE, 0)
```

### 5.3 15M入场信号判断

```
函数 CheckEntrySignal(trend, m15, cfg) → bool

输入:
  trend — 趋势方向
  m15 = {
    close, rsi, prev_rsi,
    recent_highs[], recent_lows[]   // 近lookback根K线（不含当前K线）
  }

IF trend == BULLISH:
    structure_trigger = close > max(recent_highs[])
    rsi_trigger       = rsi > 50 AND prev_rsi ≤ 50
    rsi_bias_ok       = rsi ≥ cfg.m15_rsi_bias_long     // 52
    RETURN structure_trigger OR rsi_trigger OR rsi_bias_ok

IF trend == BEARISH:
    structure_trigger = close < min(recent_lows[])
    rsi_trigger       = rsi < 50 AND prev_rsi ≥ 50
    rsi_bias_ok       = rsi ≤ cfg.m15_rsi_bias_short    // 48
    RETURN structure_trigger OR rsi_trigger OR rsi_bias_ok

RETURN false
```

### 5.4 仓位计算

```
函数 CalculatePositionSize(price, m15_atr, equity, pullback_scale, drawdown_scale, cfg) → float

前提: price > 0, equity > 0

Step 1 — 止损距离:
  stop_dist = m15_atr × cfg.stop_loss_atr_multiplier
  IF stop_dist ≤ 0: RETURN 0

Step 2 — 风险仓位:
  risk_pct    = min(cfg.risk_per_trade, 0.03)        // 硬上限3%
  risk_amount = equity × risk_pct
  risk_size   = risk_amount / stop_dist

Step 3 — 上限约束:
  cash_cap = (equity × cfg.max_position_size) / price
  lev_cap  = (equity × cfg.leverage × cfg.max_leverage_ratio) / price

Step 4 — 最终仓位:
  size = min(risk_size, cash_cap, lev_cap)
  size = size × drawdown_scale × pullback_scale

RETURN max(0, size)
```

### 5.5 止损止盈设置

```
函数 SetStopLossTakeProfit(direction, entry_price, m15_atr, cfg) → (sl, tp1, tp2)

R = m15_atr × cfg.stop_loss_atr_multiplier    // 风险单位

IF direction == LONG:
    sl  = entry_price - R
    tp1 = entry_price + R       // +1R
    tp2 = entry_price + 3 × R   // +3R

IF direction == SHORT:
    sl  = entry_price + R
    tp1 = entry_price - R       // -1R
    tp2 = entry_price - 3 × R   // -3R

RETURN (sl, tp1, tp2)
```

### 5.6 出场条件检查

```
函数 CheckExitCondition(state, m15_close, m15_ema21, m15_atr, cfg)
    → (should_exit: bool, reason: ExitReason, close_half: bool)

前提: state.has_position == true

buffer = m15_atr × cfg.ema_exit_buffer_atr


IF state.direction == LONG:

    // 1. 止损（最高优先级）
    IF m15_close ≤ state.stop_loss:
        RETURN (true, STOP_LOSS, false)

    // 2. +1R保本（不出场，仅修改止损）
    IF m15_close ≥ state.take_profit_1 AND NOT state.stop_moved_to_cost:
        state.stop_loss = state.entry_price
        state.stop_moved_to_cost = true
        RETURN (false, BREAKEVEN, false)

    // 3. +3R部分止盈
    IF m15_close ≥ state.take_profit_2 AND NOT state.partial_tp_done:
        state.partial_tp_done = true
        RETURN (true, PARTIAL_TP, true)        // 平半仓

    // 4. EMA破位
    IF state.bars_since_entry ≥ cfg.min_holding_bars
       AND m15_close < (m15_ema21 - buffer):
        state.ema_break_count += 1
        IF state.ema_break_count ≥ cfg.ema_exit_confirm_bars:
            RETURN (true, EMA_BREAK, false)
    ELSE:
        state.ema_break_count = 0


IF state.direction == SHORT:

    // 1. 止损
    IF m15_close ≥ state.stop_loss:
        RETURN (true, STOP_LOSS, false)

    // 2. +1R保本
    IF m15_close ≤ state.take_profit_1 AND NOT state.stop_moved_to_cost:
        state.stop_loss = state.entry_price
        state.stop_moved_to_cost = true
        RETURN (false, BREAKEVEN, false)

    // 3. +3R部分止盈
    IF m15_close ≤ state.take_profit_2 AND NOT state.partial_tp_done:
        state.partial_tp_done = true
        RETURN (true, PARTIAL_TP, true)

    // 4. EMA破位
    IF state.bars_since_entry ≥ cfg.min_holding_bars
       AND m15_close > (m15_ema21 + buffer):
        state.ema_break_count += 1
        IF state.ema_break_count ≥ cfg.ema_exit_confirm_bars:
            RETURN (true, EMA_BREAK, false)
    ELSE:
        state.ema_break_count = 0


RETURN (false, NONE, false)
```

### 5.7 风险管理检查

```
函数 RiskManagementCheck(state, equity, cfg) → bool

// 1. 日内连续亏损
IF state.daily_consec_losses ≥ cfg.max_consecutive_losses:    // ≥ 3
    RETURN false

// 2. 日亏损
IF state.daily_start_value > 0:
    daily_loss = (state.daily_start_value - equity) / state.daily_start_value
    IF daily_loss ≥ cfg.max_daily_loss_pct:                   // ≥ 7%
        RETURN false

// 3. 持仓数量
IF state.has_position:
    RETURN false    // max_positions = 1

RETURN true
```

```
函数 UpdateDrawdownScale(state, equity, cfg)

IF equity > state.max_portfolio_value:
    state.max_portfolio_value = equity

drawdown = (state.max_portfolio_value - equity) / state.max_portfolio_value

IF drawdown ≥ cfg.max_drawdown_pct:               // ≥ 15%
    state.drawdown_scale = cfg.drawdown_position_scale   // 0.5
ELSE:
    state.drawdown_scale = 1.0
```

### 5.8 价格结构更新

```
函数 UpdateLevels(m15_candles, h1_candles, state)

// 15M: 最近11根K线中前10根的最高/最低价（排除当前K线）
IF len(m15_candles) ≥ 11:
    state.m15_last_high = max(m15_candles[-11:-1].high[])
    state.m15_last_low  = min(m15_candles[-11:-1].low[])

// 1H: 最近21根K线中前20根的最高/最低价
IF len(h1_candles) ≥ 21:
    state.h1_last_high = max(h1_candles[-21:-1].high[])
    state.h1_last_low  = min(h1_candles[-21:-1].low[])
```

---

## 六、主循环流程

```
函数 OnCandle(m15_candle, h4_indicators, h1_indicators, m15_indicators) → Signal?

// ── 1. 日切重置 ──
IF m15_candle.open_time.date ≠ state.current_day:
    state.current_day = m15_candle.open_time.date
    state.daily_start_value = GetEquity()
    state.daily_consec_losses = 0

// ── 2. 更新价格结构 ──
UpdateLevels()

// ── 3. 有未完成订单 → 跳过 ──
IF HasPendingOrder(): RETURN null

// ── 4. 持仓管理 ──
IF state.has_position:
    state.bars_since_entry += 1

    (exit, reason, half) = CheckExitCondition(state, m15_candle.close,
        m15_indicators.ema21, m15_indicators.atr, cfg)

    IF exit:
        IF reason == BREAKEVEN:
            RETURN null                      // 仅修改止损，不出场
        IF half:
            RETURN Signal(CLOSE_HALF, reason=reason)
        RETURN Signal(CLOSE_ALL, reason=reason)

// ── 5. 已有持仓 → 不开新仓 ──
IF state.has_position: RETURN null

// ── 6. 风险检查 ──
IF NOT RiskManagementCheck(state, GetEquity(), cfg): RETURN null
UpdateDrawdownScale(state, GetEquity(), cfg)

// ── 7. 趋势判断 ──
trend = GetTrendDirection(h4_indicators.close, h4_indicators.ema21, h4_indicators.ema55)
IF trend == NONE OR trend == SIDEWAYS: RETURN null

// ── 8. 1H入场条件 ──
(ok, mode, scale) = CheckEntryCondition(trend, h1_indicators, h4_indicators.adx, cfg)
IF NOT ok: RETURN null
state.pullback_scale = scale
state.entry_mode = mode

// ── 9. 15M入场信号 ──
IF NOT CheckEntrySignal(trend, m15_indicators, cfg): RETURN null

// ── 10. 仓位计算 ──
size = CalculatePositionSize(m15_candle.close, m15_indicators.atr,
    GetEquity(), scale, state.drawdown_scale, cfg)
IF size ≤ 0: RETURN null

// ── 11. 执行入场 ──
IF trend == BULLISH:
    direction = LONG
    action = OPEN_LONG
ELSE:
    direction = SHORT
    action = OPEN_SHORT

(sl, tp1, tp2) = SetStopLossTakeProfit(direction, m15_candle.close, m15_indicators.atr, cfg)

// 更新状态
state.has_position       = true
state.direction          = direction
state.entry_price        = m15_candle.close
state.entry_time         = m15_candle.open_time
state.entry_mode         = mode
state.stop_loss          = sl
state.take_profit_1      = tp1
state.take_profit_2      = tp2
state.stop_moved_to_cost = false
state.partial_tp_done    = false
state.bars_since_entry   = 0
state.ema_break_count    = 0

RETURN Signal(action, size=size, stop_loss=sl,
    take_profit_1=tp1, take_profit_2=tp2,
    entry_mode=mode, timestamp=m15_candle.open_time)
```

---

## 七、交易后处理

```
函数 OnTradeClosed(pnl)

state.trade_count += 1

IF pnl > 0:
    state.win_count += 1
    state.consecutive_losses = 0
    state.daily_consec_losses = 0
ELSE:
    state.consecutive_losses += 1
    state.daily_consec_losses += 1

// 清除持仓状态
state.has_position       = false
state.direction          = NONE
state.entry_price        = 0
state.entry_time         = null
state.entry_mode         = NONE
state.stop_loss          = 0
state.take_profit_1      = 0
state.take_profit_2      = 0
state.stop_moved_to_cost = false
state.partial_tp_done    = false
state.pullback_scale     = 1.0
state.bars_since_entry   = 0
state.ema_break_count    = 0
```

---

## 八、多时间周期数据同步

### 8.1 时间对齐

```
4H K线时间: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC
1H K线时间: 每整点
15M K线时间: 每15分钟

对齐规则:
  15M K线触发时，取该时刻对应的最新一根已完成1H和4H K线
  "已完成" = 该K线周期已结束
    例: 4H 08:00-12:00 的K线在 12:00 收盘后才可用
    例: 1H 11:00-12:00 的K线在 12:00 收盘后才可用
```

### 8.2 时间映射

```
给定 15M K线时间 T:

1H已完成K线 = 1H序列中 open_time ≤ T - 1小时的最后一根
4H已完成K线 = 4H序列中 open_time ≤ T - 4小时的最后一根

注意:
  - 12:00 的15M K线，可用 11:00 的1H K线（已收盘）
  - 12:00 的15M K线，可用 08:00 的4H K线（已收盘）
  - 不可使用当前正在形成的K线
```

### 8.3 指标数据结构

每根15M K线触发时，策略需要以下指标值作为输入：

**4H指标**:

| 指标 | 字段名 | 说明 |
|------|--------|------|
| 收盘价 | `h4_close` | 最近一根已完成4H K线收盘价 |
| EMA21 | `h4_ema21` | 4H EMA(21) |
| EMA55 | `h4_ema55` | 4H EMA(55) |
| ADX | `h4_adx` | 4H ADX(14) |

**1H指标**:

| 指标 | 字段名 | 说明 |
|------|--------|------|
| 收盘价 | `h1_close` | 最近一根已完成1H K线收盘价 |
| EMA21 | `h1_ema21` | 1H EMA(21)，当前值 |
| EMA21前值 | `h1_prev_ema21` | 1H EMA(21)，前一根K线值 |
| EMA55 | `h1_ema55` | 1H EMA(55)，当前值 |
| EMA55前值 | `h1_prev_ema55` | 1H EMA(55)，前一根K线值 |
| RSI | `h1_rsi` | 1H RSI(14) |
| MACD柱 | `h1_macd_hist` | MACD线 - 信号线，当前值 |
| MACD柱前值 | `h1_prev_macd_hist` | MACD线 - 信号线，前一根值 |
| ATR | `h1_atr` | 1H ATR(14) |

**15M指标**:

| 指标 | 字段名 | 说明 |
|------|--------|------|
| 收盘价 | `m15_close` | 当前15M K线收盘价 |
| EMA21 | `m15_ema21` | 15M EMA(21) |
| ATR | `m15_atr` | 15M ATR(14) |
| RSI | `m15_rsi` | 15M RSI(14)，当前值 |
| RSI前值 | `m15_prev_rsi` | 15M RSI(14)，前一根值 |
| 近N根最高价 | `m15_recent_highs[]` | 前6根K线最高价数组（不含当前） |
| 近N根最低价 | `m15_recent_lows[]` | 前6根K线最低价数组（不含当前） |

---

## 九、入场决策矩阵

### 9.1 做多

| 条件 | 模式A（回调） | 模式C（交叉） | 都不满足 |
|------|:----------:|:----------:|:-------:|
| 4H趋势 = BULLISH | 必要 | 必要 | — |
| 1H价格在EMA带内 | ✅ | — | — |
| 1H EMA21刚上穿EMA55 | — | ✅ | — |
| 价格在EMA21附近0.5×ATR | — | ✅ | — |
| 1H EMA21上升 | ✅ | — | — |
| 价格 > 前低点 | ✅ | — | — |
| 4H ADX ≥ 25 | — | ✅ | — |
| MACD柱 > 0且上升 | — | ✅ | — |
| RSI ∈ [42, 60] | ✅ | ✅ | — |
| 15M信号触发 | ✅ | ✅ | — |
| **仓位缩放** | 1.0 / 0.9(深回调) | 0.9 | 不入场 |

### 9.2 做空

| 条件 | 模式A（回调） | 模式C（交叉） | 都不满足 |
|------|:----------:|:----------:|:-------:|
| 4H趋势 = BEARISH | 必要 | 必要 | — |
| 1H价格在EMA带内 | ✅ | — | — |
| 1H EMA21刚下穿EMA55 | — | ✅ | — |
| 价格在EMA21附近0.5×ATR | — | ✅ | — |
| 1H EMA21下降 | ✅ | — | — |
| 价格 < 前高点 | ✅ | — | — |
| 4H ADX ≥ 25 | — | ✅ | — |
| MACD柱 < 0且下降 | — | ✅ | — |
| RSI ∈ [40, 58] | ✅ | ✅ | — |
| 15M信号触发 | ✅ | ✅ | — |
| **仓位缩放** | 1.0 / 0.9(深回调) | 0.9 | 不入场 |

---

## 十、持仓管理生命周期

```
入场 ────────────────────────────────────────────────────────────→ 出场
  │                                                                │
  │  阶段1: 浮亏区                                                  │
  │  stop_loss = entry_price - 1.5×ATR (多头)                      │
  │  价格触及 → 全平(STOP_LOSS)                                     │
  │                                                                │
  │  阶段2: +1R保本                                                 │
  │  价格 ≥ entry_price + R (多头)                                  │
  │  stop_loss → entry_price                                       │
  │  stop_moved_to_cost = true                                     │
  │  注: 此操作仅一次，不出场                                        │
  │                                                                │
  │  阶段3: +3R部分止盈                                              │
  │  价格 ≥ entry_price + 3R (多头)                                 │
  │  平仓50%，剩余继续持有                                           │
  │  partial_tp_done = true                                        │
  │  注: 此操作仅一次                                                │
  │                                                                │
  │  阶段4: EMA破位出场                                              │
  │  价格 < ema21 - buffer (多头，连续2根15M确认)                    │
  │  剩余仓位全部平仓(EMA_BREAK)                                     │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
```

**出场优先级**: 止损 > 保本 > 部分止盈 > EMA破位

---

## 十一、参数完整参考

### 11.1 指标周期参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `h4_ema_fast` | int | 21 | 4H EMA快线周期 |
| `h4_ema_slow` | int | 55 | 4H EMA慢线周期 |
| `h4_adx_period` | int | 14 | 4H ADX周期 |
| `h1_ema_fast` | int | 21 | 1H EMA快线周期 |
| `h1_ema_slow` | int | 55 | 1H EMA慢线周期 |
| `h1_rsi_period` | int | 14 | 1H RSI周期 |
| `h1_macd_fast` | int | 12 | 1H MACD快线 |
| `h1_macd_slow` | int | 26 | 1H MACD慢线 |
| `h1_macd_signal` | int | 9 | 1H MACD信号线 |
| `h1_atr_period` | int | 14 | 1H ATR周期 |
| `m15_ema_period` | int | 21 | 15M EMA周期 |
| `m15_atr_period` | int | 14 | 15M ATR周期 |
| `m15_rsi_period` | int | 14 | 15M RSI周期 |
| `m15_breakout_lookback` | int | 6 | 15M结构突破回看K线数 |

### 11.2 风控与仓位参数

| 参数名 | 类型 | 默认值 | 硬限制 | 说明 |
|--------|------|--------|--------|------|
| `risk_per_trade` | float | 0.03 | ≤ 0.03 | 单笔风险比例 |
| `max_position_size` | float | 0.55 | — | 最大仓位/权益比 |
| `deep_pullback_scale` | float | 0.9 | — | 深回调仓位缩放 |
| `pullback_deep_band` | float | 0.003 | — | 深回调判定带（距EMA55 ≤ 0.3%） |
| `stop_loss_atr_multiplier` | float | 1.5 | — | 止损ATR倍数 |
| `min_holding_bars` | int | 5 | — | 最小持仓K线数（15M） |
| `ema_exit_confirm_bars` | int | 2 | — | EMA破位连续确认K线数 |
| `ema_exit_buffer_atr` | float | 0.30 | — | EMA破位缓冲ATR倍数 |

### 11.3 趋势交叉模式参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `crossover_atr_distance` | float | 0.5 | 交叉模式：EMA21附近ATR距离 |
| `crossover_position_scale` | float | 0.9 | 交叉模式仓位缩放 |
| `crossover_adx_threshold` | float | 25 | 交叉模式ADX阈值 |

### 11.4 杠杆与约束参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `leverage` | float | 7.0 | 杠杆倍数 |
| `max_leverage_ratio` | float | 0.92 | 最大杠杆使用率 |
| `max_positions` | int | 1 | 最大同时持仓数 |
| `max_consecutive_losses` | int | 3 | 日内连续亏损暂停阈值 |
| `max_daily_loss_pct` | float | 0.07 | 最大日亏损比例 |
| `max_drawdown_pct` | float | 0.15 | 触发回撤缩放的比例 |
| `drawdown_position_scale` | float | 0.5 | 回撤缩放系数 |

### 11.5 RSI过滤参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `h1_rsi_long_low` | float | 42 | 1H多头RSI下限 |
| `h1_rsi_long_high` | float | 60 | 1H多头RSI上限 |
| `h1_rsi_short_low` | float | 40 | 1H空头RSI下限 |
| `h1_rsi_short_high` | float | 58 | 1H空头RSI上限 |
| `m15_rsi_bias_long` | float | 52 | 15M多头RSI偏置 |
| `m15_rsi_bias_short` | float | 48 | 15M空头RSI偏置 |

### 11.6 币种级参数覆盖

| 参数名 | 默认值 | XRP覆盖值 |
|--------|--------|-----------|
| `risk_per_trade` | 0.03 | **0.02** |
| `stop_loss_atr_multiplier` | 1.5 | **2.0** |
| `ema_exit_buffer_atr` | 0.30 | **0.40** |
| `leverage` | 7.0 | **5.0** |
| `crossover_position_scale` | 0.9 | **0.85** |
| `crossover_adx_threshold` | 25 | **28** |

---

## 十二、交易成本模型

| 项目 | 值 | 说明 |
|------|-----|------|
| 手续费率 | 0.10% | Taker费率 |
| 保证金率 | 20% | 合约保证金 |
| 滑点 | 0.10% | 按价格百分比 |
| 成交模式 | 收盘价成交 | 信号K线收盘价执行 |

仓位已通过 `max_position_size`、`leverage`、`max_leverage_ratio` 三重约束限制。

---

## 十三、初始化与预热

### 13.1 策略初始化

```
函数 StrategyInit(config)

// 设置参数
cfg = config

// 初始化持仓状态
state.has_position       = false
state.direction          = NONE
state.entry_price        = 0
state.entry_time         = null
state.entry_mode         = NONE
state.stop_loss          = 0
state.take_profit_1      = 0
state.take_profit_2      = 0
state.stop_moved_to_cost = false
state.partial_tp_done    = false
state.bars_since_entry   = 0
state.ema_break_count    = 0
state.pullback_scale     = 1.0
state.drawdown_scale     = 1.0

// 初始化风险状态
state.trade_count          = 0
state.win_count            = 0
state.consecutive_losses   = 0
state.daily_consec_losses  = 0
state.current_day          = null
state.daily_start_value    = GetEquity()
state.max_portfolio_value  = GetEquity()
```

### 13.2 数据预热要求

策略启动时必须加载足够的历史K线使指标有效：

| 指标 | 最少K线数 | 等效时间 | 缺失影响 |
|------|----------|---------|---------|
| 4H EMA55 | 55根4H | ~9天 | 无趋势判断，不可交易 |
| 4H ADX(14) | 27根4H | ~4.5天 | 交叉模式不可用，仅模式A |
| 1H EMA55 | 55根1H | ~2.3天 | 入场条件不满足 |
| 1H MACD(12,26,9) | 34根1H | ~1.4天 | 交叉模式不可用 |
| 1H ATR(14) | 14根1H | ~14小时 | 交叉模式距离无法计算 |
| 15M EMA21 | 21根15M | ~5.25小时 | 出场判断不可用 |
| 15M ATR(14) | 14根15M | ~3.5小时 | 止损/仓位无法计算 |
| 15M RSI(14) | 15根15M | ~3.75小时 | 入场信号不可用 |
| 15M结构突破 | 7根15M | ~1.75小时 | 结构突破信号不可用 |

**建议**: 策略启动时至少加载 **60 根4H K线**（~10天）的历史数据。

### 13.3 预热期行为

在指标尚未收敛的预热期内：
- 趋势判断返回 `NONE` → 不交易
- 入场条件/信号判断返回 `false` → 不入场
- **不需要**额外标记预热状态，指标无效值自然会阻止交易

---

## 十四、关键实现注意事项

### 14.1 EMA交叉检测

```
1H EMA21上穿EMA55:
  当前帧: ema21[current] > ema55[current]
  上一帧: ema21[prev]    ≤ ema55[prev]

1H EMA21下穿EMA55:
  当前帧: ema21[current] < ema55[current]
  上一帧: ema21[prev]    ≥ ema55[prev]

关键: 必须使用已完成K线的指标值，不可用实时未完成K线
```

### 14.2 止损移动的幂等性

```
止损移到成本只能发生一次:
  IF NOT stop_moved_to_cost AND price达到+1R:
      stop_loss = entry_price
      stop_moved_to_cost = true
  后续即使价格回落再上涨，不再重复移动
```

### 14.3 部分止盈的仓位管理

```
+3R触发时:
  平仓数量 = 当前持仓量 / 2
  剩余仓位继续等EMA破位出场

注意:
  - 部分止盈后剩余仓位不重算止损（止损仍在entry_price）
  - EMA破位出场时平掉剩余全部仓位
  - 部分止盈仅执行一次
```

### 14.4 日级别风控重置

```
每日00:00 UTC:
  daily_start_value = 当前权益
  daily_consec_losses = 0

注意:
  daily_consec_losses 按日重置，不跨日累计
  consecutive_losses 跨日累计，盈利交易后归零
```

### 14.5 仓位缩放互斥

```
深回调缩放(0.9) 和 交叉模式缩放(0.9) 互斥，不叠加:
  - 模式A: scale = 1.0 或 0.9（深回调时）
  - 模式C: scale = 0.9（固定）
  交叉模式不检测深回调
```

### 14.6 模式A与模式C的互斥

```
同一根K线上，模式A和模式C不会同时触发:
  - 模式A要求: 价格在EMA21-EMA55带内（EMA21和EMA55已分离）
  - 模式C要求: EMA21刚穿越EMA55（交叉时刻，带极窄）
  - 实际中交叉时刻价格通常不在EMA带内
  - 算法先检查模式A再检查模式C，模式A满足则不进入模式C
```

### 14.7 出场优先级

```
每根15M K线按以下顺序检查出场:
  1. 止损触发 → 立即全平
  2. +1R保本 → 修改止损价（不出场）
  3. +3R部分止盈 → 平半仓
  4. EMA破位 → 全平（需连续2根确认）

止损优先级最高，一旦触发不检查其他条件。
```

### 14.8 空头部分止盈后的持仓量

```
+3R触发平半仓后:
  - 做多: 卖出当前持仓量的50%
  - 做空: 买入回补当前持仓量的50%
  - 剩余仓位等EMA破位出场
  - 止损价保持在 entry_price（保本位）
```

---

## 十五、回测验证结果

### 15.1 综合表现（5币种 × 6年 = 30个年度样本）

| 指标 | 值 | 达标标准 |
|------|-----|---------|
| 平均年收益率 | **17.11%** | — |
| 总交易次数 | **1,335** | — |
| 综合胜率 | **51.2%** | — |
| 平均夏普比率 | **1.90** | > 1.2 ✓ |
| 平均最大回撤 | **3.32%** | < 15% ✓ |
| 达标率 | **83.3%** (25/30) | — |

### 15.2 分币种年均表现

| 币种 | 年均收益 | 年均交易 | 胜率 | 年均夏普 | 年均回撤 |
|------|---------|---------|------|---------|---------|
| BTC | 12.87% | 249 | 55.0% | 2.16 | 1.74% |
| BNB | 14.65% | 276 | 50.4% | 1.86 | 2.99% |
| SOL | 23.98% | 251 | 53.8% | 2.15 | 3.54% |
| XRP | 14.31% | 280 | 43.9% | 1.08 | 5.79% |
| ETH | 19.73% | 279 | 53.4% | 2.24 | 2.56% |

### 15.3 回测参数

| 项目 | 值 |
|------|-----|
| 初始资金 | 10,000 USDT |
| 回测区间 | 2020-2025 |
| 手续费 | 0.10% (Taker) |
| 滑点 | 0.10% |
| 保证金率 | 20% |
| 品种 | BTC/BNB/SOL/XRP/ETH USDT永续合约 |

### 15.4 逐年逐币种详细结果

#### BTC

| 年份 | 收益率 | 交易数 | 胜率 | 夏普比率 | 最大回撤 |
|------|--------|--------|------|---------|---------|
| 2020 | 8.98% | 41 | 46.3% | 1.95 | 1.66% |
| 2021 | 24.03% | 40 | 65.0% | 2.37 | 2.59% |
| 2022 | 16.36% | 46 | 45.7% | 2.52 | 1.90% |
| 2023 | 8.10% | 43 | 60.5% | 2.02 | 1.40% |
| 2024 | 10.26% | 32 | 59.4% | 2.25 | 1.01% |
| 2025 | 9.47% | 47 | 55.3% | 1.85 | 1.85% |

#### BNB

| 年份 | 收益率 | 交易数 | 胜率 | 夏普比率 | 最大回撤 |
|------|--------|--------|------|---------|---------|
| 2020 | 19.72% | 34 | 64.7% | 2.83 | 4.45% |
| 2021 | 20.26% | 52 | 46.2% | 1.43 | 5.87% |
| 2022 | 18.81% | 40 | 50.0% | 2.55 | 1.91% |
| 2023 | 1.72% | 46 | 41.3% | 0.44 | 1.99% |
| 2024 | 22.05% | 47 | 55.3% | 2.61 | 1.58% |
| 2025 | 5.34% | 57 | 49.1% | 1.33 | 2.16% |

#### SOL

| 年份 | 收益率 | 交易数 | 胜率 | 夏普比率 | 最大回撤 |
|------|--------|--------|------|---------|---------|
| 2020 | 9.78% | 16 | 56.2% | 2.27 | 3.04% |
| 2021 | 36.27% | 53 | 47.2% | 2.07 | 5.20% |
| 2022 | 36.52% | 37 | 67.6% | 2.77 | 2.95% |
| 2023 | 6.24% | 36 | 41.7% | 0.99 | 2.83% |
| 2024 | 14.54% | 52 | 48.1% | 1.63 | 3.65% |
| 2025 | 40.52% | 57 | 63.2% | 3.14 | 3.56% |

#### XRP

| 年份 | 收益率 | 交易数 | 胜率 | 夏普比率 | 最大回撤 |
|------|--------|--------|------|---------|---------|
| 2020 | -4.21% | 46 | 23.9% | -0.45 | 13.37% |
| 2021 | 51.09% | 49 | 57.1% | 2.67 | 4.42% |
| 2022 | 15.21% | 43 | 55.8% | 1.70 | 2.83% |
| 2023 | -3.86% | 56 | 28.6% | -0.53 | 6.60% |
| 2024 | 7.08% | 40 | 50.0% | 1.09 | 3.57% |
| 2025 | 20.52% | 46 | 52.2% | 2.02 | 3.98% |

#### ETH

| 年份 | 收益率 | 交易数 | 胜率 | 夏普比率 | 最大回撤 |
|------|--------|--------|------|---------|---------|
| 2020 | 20.54% | 48 | 45.8% | 2.15 | 1.87% |
| 2021 | 34.90% | 41 | 61.0% | 2.71 | 3.30% |
| 2022 | 29.41% | 34 | 67.6% | 3.10 | 3.30% |
| 2023 | 6.72% | 51 | 45.1% | 1.26 | 2.91% |
| 2024 | 10.99% | 54 | 50.0% | 1.99 | 2.08% |
| 2025 | 15.81% | 51 | 56.9% | 2.25 | 1.90% |
