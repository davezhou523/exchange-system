# 震荡策略 (Volatility Strategy) 技术文档

> 版本: v4 | 目标: Golang 实现参考 | 标的: ETH/USDT 永续合约

---

## 一、策略概述

震荡策略的核心思想是 **"4H判断震荡 → 1H定位区间 → 15M执行入场"**，在价格区间震荡时利用布林带上下轨做低买高卖，配合假突破反向操作。

### 交易框架

```
① 4H 判断是否震荡（评分制，0-3分）
   ├── ADX < 20 → +1
   ├── EMA21 与 EMA55 接近(差距<0.5%) → +1
   ├── ATR 下降（当前 < 前一根） → +1
   └── 评分 >= 2 → 判定为震荡，允许交易

② 1H 定位价格区间（布林带 BB(20, 2.0)）
   ├── 上轨 = 压力位
   ├── 下轨 = 支撑位
   ├── 中轨 = 中性位
   └── 价格在区间中的位置决定方向

③ 15M 执行入场信号
   ├── 做多: 价格接近1H下轨(支撑) + 15M反转信号
   ├── 做空: 价格接近1H上轨(压力) + 15M反转信号
   └── 假突破: 价格突破1H轨道但4H ADX仍低 → 反向做

④ 风控与持仓管理
   ├── 止损 = 1H BB轨道 ± ATR偏移
   ├── 止盈 = 1H BB对侧轨道
   ├── 保本止损: 盈利达60%TP → SL移到成本
   ├── 阶梯追踪: 盈利达90%TP → SL移到TP距离的65%
   └── 仓位 = 允许亏损金额 / 止损距离
```

### 核心理念

> 震荡市中价格在区间内反复波动，布林带上下轨构成天然的支撑/压力。关键在于：**4H确认震荡 + 1H定位区间边界 + 15M确认反转**。

---

## 二、多时间周期架构

| 周期 | 数据源 | 主要功能 | 核心指标 | 预热所需K线数 |
|------|--------|---------|----------|-------------|
| 4H | `data_4h` | 震荡判断 | ADX(14), EMA(21), EMA(55), ATR(14) | 55根 (EMA55) |
| 1H | `data_1h` | 区间定位 | BB(20, 2.0), ATR(14), RSI(14) | 20根 (BB周期) |
| 15M | `data_15m` | 入场信号/止损/出场 | BB(20, 2.5), RSI(14), ATR(14) | 20根 (BB周期) |

**数据要求**: 需同时订阅三个周期的K线，15M为主周期（每15分钟触发一次逻辑判断）。

---

## 三、参数定义（最优配置）

### 3.1 完整参数表

```go
type StrategyConfig struct {
    // ===== 4H 震荡判断 =====
    H4AdxPeriod      float64  // ADX周期，默认 14
    H4AdxThreshold   float64  // ADX阈值，默认 20.0（ADX<20 → 震荡倾向）
    H4EmaFast        int      // 快速EMA周期，默认 21
    H4EmaSlow        int      // 慢速EMA周期，默认 55
    H4EmaCloseness   float64  // EMA接近度阈值，默认 0.005（差距<0.5% → 接近）
    H4AtrPeriod      int      // ATR周期，默认 14
    H4OscScoreMin    int      // 震荡最低评分，默认 2（0-3分制）

    // ===== 1H 区间（布林带）=====
    H1BBPeriod       int      // BB周期，默认 20
    H1BBDev          float64  // BB标准差倍数，默认 2.0

    // ===== 15M 入场信号 =====
    M15BBPeriod      int      // BB周期，默认 20
    M15BBDev         float64  // BB标准差倍数，默认 2.5（更宽，信号更可靠）
    M15RSIPeriod     int      // RSI周期，默认 14
    M15RSILong       float64  // 做多RSI超卖阈值，默认 30
    M15RSIShort      float64  // 做空RSI超买阈值，默认 70
    M15RSIConfirmLong  float64  // 做多RSI确认回升阈值，默认 32
    M15RSIConfirmShort float64  // 做空RSI确认回落阈值，默认 68

    // ===== 15M K线形态 =====
    UsePinBar        bool     // 启用Pin Bar信号，默认 true
    UseEngulfing     bool     // 启用吞没形态信号，默认 true
    PinBarRatio      float64  // Pin Bar影线/实体最小比，默认 2.0

    // ===== 入场信号强度 =====
    MinSignals       int      // 至少需要的15M信号数，默认 2
    RequireBBBounce  bool     // 必须有BB反弹确认，默认 true

    // ===== 1H 区间接近度 =====
    ZoneProximity    float64  // 接近1H BB轨的距离比例，默认 0.2（20%带宽内）

    // ===== 假突破 =====
    FakeBreakout     bool     // 启用假突破检测，默认 true

    // ===== 过滤器 =====
    UseH1CandleFilter bool    // 1H K线方向过滤，默认 true
    UseH4TrendFilter  bool    // 4H趋势方向过滤，默认 false（测试表明有害）
    UseH1RSIFilter    bool    // 1H RSI过滤，默认 false
    H1RSIPeriod       int     // 1H RSI周期，默认 14
    H1RSILongMax      float64 // 做多时1H RSI上限，默认 0（关闭）
    H1RSIShortMin     float64 // 做空时1H RSI下限，默认 0（关闭）
    MinH1BBWidth      float64 // 1H BB最小宽度(占价格%)，默认 0（关闭）

    // ===== 止损 =====
    SLMethod         string   // 止损方法: "h1bb" 或 "m15bb"，默认 "h1bb"
    SLATROffset      float64  // 止损距BB轨道的ATR偏移倍数，默认 0.3

    // ===== 移动止损（默认关闭）=====
    UseTrailingStop  bool     // 启用移动止损，默认 false
    TrailATRMult     float64  // 移动止损ATR倍数，默认 0
    TrailActivate    float64  // 激活移动止损的盈利ATR倍数，默认 0

    // ===== R:R 过滤（默认关闭）=====
    MinRR            float64  // 最低风险回报比，默认 0

    // ===== 时间止损（默认关闭）=====
    UseTimeStop      bool     // 启用时间止损，默认 false
    MaxBars          int      // 最大持仓K线数，默认 0

    // ===== 连亏过滤（默认关闭）=====
    UseConsecLossFilter bool  // 启用连亏过滤，默认 false
    MaxConsecLosses  int      // 连亏N次后暂停，默认 0

    // ===== 保本止损 =====
    UseBreakevenStop bool     // 启用保本止损，默认 true
    BreakevenActivate float64 // 盈利达TP的X%时移SL到成本，默认 0.6（60%）

    // ===== 阶梯追踪 =====
    UseSteppedTrail  bool     // 启用阶梯追踪，默认 true
    TrailStep1Pct    float64  // 第一阶梯: 价格达TP的X%，默认 0.9（90%）
    TrailStep1SL     float64  // 第一阶梯: SL移到TP距离的Y%，默认 0.65（65%）
    TrailStep2Pct    float64  // 第二阶梯(关闭)，默认 0
    TrailStep2SL     float64  // 第二阶梯(关闭)，默认 0

    // ===== 止盈 =====
    TPMethod         string   // 止盈方法: "band"|"mid"|"mid_atr"|"rsi"|"split"，默认 "band"
    TPATROffset      float64  // 止盈ATR偏移（mid_atr模式），默认 0
    SplitTPRatio     float64  // 分批止盈比例（split模式），默认 0

    // ===== 仓位管理 =====
    RiskPerTrade     float64  // 单笔风险占总资金比例，默认 0.0055（0.55%）
    RiskAfterWin     float64  // 赢后风险比例（动态仓位），默认 0（关闭）
    RiskAfterLoss    float64  // 亏后风险比例（动态仓位），默认 0（关闭）
    M15ATRPeriod     int      // 15M ATR周期，默认 14
    Leverage         float64  // 杠杆倍数，默认 2.0
    MaxLeverageRatio float64  // 最大杠杆使用率，默认 0.80
    MaxPositions     int      // 最大持仓数，默认 1

    // ===== ML信号过滤（默认关闭）=====
    UseMLFilter      bool     // 启用ML过滤，默认 false
    MLProbThreshold  float64  // ML概率阈值，默认 0.55
}
```

### 3.2 最优参数值（经验证的最佳配置）

```go
// OptimalConfig 已通过2020-2025年ETH/USDT回测验证
var OptimalConfig = StrategyConfig{
    // 4H 震荡判断
    H4AdxPeriod:    14,
    H4AdxThreshold: 20,
    H4EmaFast:      21,
    H4EmaSlow:      55,
    H4EmaCloseness: 0.005,
    H4AtrPeriod:    14,
    H4OscScoreMin:  2,

    // 1H 区间
    H1BBPeriod: 20,
    H1BBDev:    2.0,

    // 15M 入场
    M15BBPeriod:       20,
    M15BBDev:          2.5,
    M15RSIPeriod:      14,
    M15RSILong:        30,
    M15RSIShort:       70,
    M15RSIConfirmLong:  32,
    M15RSIConfirmShort: 68,

    // 15M 形态
    UsePinBar:   true,
    UseEngulfing: true,
    PinBarRatio: 2.0,

    // 信号强度
    MinSignals:      2,
    RequireBBBounce: true,
    ZoneProximity:   0.2,

    // 假突破
    FakeBreakout: true,

    // 过滤器
    UseH1CandleFilter: true,
    UseH4TrendFilter:  false,
    UseH1RSIFilter:    false,
    H1RSIPeriod:       14,
    H1RSILongMax:      0,
    H1RSIShortMin:     0,
    MinH1BBWidth:      0,

    // 止损
    SLMethod:    "h1bb",
    SLATROffset: 0.3,

    // 移动止损
    UseTrailingStop: false,
    TrailATRMult:    0,
    TrailActivate:   0,

    // R:R
    MinRR: 0,

    // 时间止损
    UseTimeStop: false,
    MaxBars:     0,

    // 连亏
    UseConsecLossFilter: false,
    MaxConsecLosses:     0,

    // 保本止损
    UseBreakevenStop: true,
    BreakevenActivate: 0.6,

    // 阶梯追踪
    UseSteppedTrail: true,
    TrailStep1Pct:   0.9,
    TrailStep1SL:    0.65,
    TrailStep2Pct:   0,
    TrailStep2SL:    0,

    // 止盈
    TPMethod:      "band",
    TPATROffset:   0,
    SplitTPRatio:  0,

    // 仓位
    RiskPerTrade:     0.0055,
    RiskAfterWin:     0,
    RiskAfterLoss:    0,
    M15ATRPeriod:     14,
    Leverage:         2.0,
    MaxLeverageRatio: 0.80,
    MaxPositions:     1,

    // ML
    UseMLFilter:     false,
    MLProbThreshold: 0.55,
}
```

---

## 四、信号判断逻辑（伪代码）

### 4.1 第一步：4H 震荡评分

```
函数 H4OscillationScore() → (score, isOscillation):

    score = 0

    // 条件1: ADX < 阈值
    if H4_ADX[0] < H4AdxThreshold:
        score += 1

    // 条件2: EMA21 与 EMA55 接近
    closeness = abs(H4_EMA21[0] - H4_EMA55[0]) / H4_EMA55[0]
    if closeness < H4EmaCloseness:
        score += 1

    // 条件3: ATR 下降
    if H4_ATR[0] < H4_ATR[-1]:
        score += 1

    isOscillation = (score >= H4OscScoreMin)  // 默认 >= 2
    return (score, isOscillation)
```

### 4.2 第二步：1H 区间定位

```
函数 H1ZonePosition() → (zone, positionPct):

    h1_top = H1_BB_Upper    // 1H BB上轨
    h1_bot = H1_BB_Lower    // 1H BB下轨
    price  = 当前15M收盘价

    bandWidth = h1_top - h1_bot
    positionPct = (price - h1_bot) / bandWidth   // 0=下轨, 1=上轨

    if price < h1_bot:
        return ("breakout_down", positionPct)
    else if price > h1_top:
        return ("breakout_up", positionPct)
    else if positionPct < ZoneProximity:          // 默认 < 0.2
        return ("lower", positionPct)             // 接近支撑
    else if positionPct > (1 - ZoneProximity):    // 默认 > 0.8
        return ("upper", positionPct)             // 接近压力
    else:
        return ("middle", positionPct)            // 中间区域
```

### 4.3 第三步：15M 入场信号检测

#### 4.3.1 信号类型定义

```
信号1: RSI反弹做多
    条件: RSI[-1] < 30 且 RSI[0] > 32    // 从超卖区回升确认

信号2: RSI反弹做空
    条件: RSI[-1] > 70 且 RSI[0] < 68    // 从超买区回落确认

信号3: RSI超卖/超买（辅助信号）
    做多: RSI[0] < 30
    做空: RSI[0] > 70

信号4: BB反弹做多
    条件: 近8根K线内15M low触及过15M BB下轨，且当前close > 15M BB下轨

信号5: BB反弹做空
    条件: 近8根K线内15M high触及过15M BB上轨，且当前close < 15M BB上轨

信号6: Pin Bar做多（看涨锤子线）
    条件: 下影线 >= PinBarRatio × 实体 且 上影线 < 实体
    其中: 下影线 = min(Open,Close) - Low, 上影线 = High - max(Open,Close)
          实体 = abs(Close - Open)，若<0.01按0.01计

信号7: Pin Bar做空（看跌射击之星）
    条件: 上影线 >= PinBarRatio × 实体 且 下影线 < 实体

信号8: 吞没做多
    条件: 前一根阴线(Close[-1] < Open[-1]) + 当前阳线(Close[0] > Open[0])
          且 Close[0] > Open[-1] 且 Open[0] < Close[-1]

信号9: 吞没做空
    条件: 前一根阳线(Close[-1] > Open[-1]) + 当前阴线(Close[0] < Open[0])
          且 Close[0] < Open[-1] 且 Open[0] > Close[-1]
```

#### 4.3.2 信号聚合逻辑

```
函数 M15LongSignals() → []string:

    signals = []

    if RSI反弹做多: signals.append("RSI_bounce")
    if BB反弹做多:  signals.append("BB_bounce")
    if PinBar做多:   signals.append("PinBar")
    if 吞没做多:     signals.append("Engulfing")
    if RSI超卖:      signals.append("RSI_oversold")

    // 必须有BB反弹确认
    if RequireBBBounce and "BB_bounce" not in signals:
        return []

    // 信号数不足
    if len(signals) < MinSignals:
        return []

    return signals

// 做空逻辑对称
函数 M15ShortSignals() → []string: (同上，方向相反)
```

### 4.4 假突破检测

```
函数 CheckFakeBreakoutLong() → bool:

    if not FakeBreakout: return false

    (zone, _) = H1ZonePosition()
    if zone != "breakout_down": return false    // 价格未跌破1H下轨

    if H4_ADX[0] >= H4AdxThreshold: return false   // 4H非震荡，真突破

    signals = M15LongSignals()
    return len(signals) > 0                         // 有15M反弹信号

函数 CheckFakeBreakoutShort() → bool: (对称逻辑)
    zone == "breakout_up" + H4_ADX低 + 15M做空信号
```

### 4.5 过滤器

```
// 1H K线方向过滤
函数 CheckH1Candle(direction) → bool:
    if not UseH1CandleFilter: return true
    if direction == "up":   return H1_Close >= H1_Open   // 做多须阳线
    if direction == "down": return H1_Close <= H1_Open   // 做空须阴线

// 4H趋势方向过滤（默认关闭）
函数 CheckH4Trend(direction) → bool:
    if not UseH4TrendFilter: return true
    closeness = abs(H4_EMA21 - H4_EMA55) / H4_EMA55
    if closeness < H4EmaCloseness: return true   // 震荡时允许双向
    if direction == "up":   return H4_EMA21 > H4_EMA55
    if direction == "down": return H4_EMA21 < H4_EMA55

// 1H RSI过滤（默认关闭）
函数 CheckH1RSI(direction) → bool:
    if not UseH1RSIFilter: return true
    if direction == "up":   return H1_RSI < H1RSILongMax
    if direction == "down": return H1_RSI > H1RSIShortMin

// 1H BB宽度过滤（默认关闭）
函数 CheckH1BBWidth() → bool:
    if MinH1BBWidth <= 0: return true
    bbWidth = (H1_BB_Upper - H1_BB_Lower) / H1_BB_Mid
    return bbWidth >= MinH1BBWidth

// 连亏过滤（默认关闭）
函数 CheckConsecLoss() → bool:
    if not UseConsecLossFilter: return true
    return consecLosses < MaxConsecLosses
```

---

## 五、主循环逻辑

每收到一根15M K线执行一次：

```
函数 OnBar(candle_15m):

    // 1. 更新BB触及追踪
    UpdateTouchTracking()

    // 2. 有持仓 → 执行持仓管理
    if HasOpenPosition():
        ManagePosition()
        return

    // 3. 指标预热检查
    if not AllIndicatorsReady():
        return

    // 4. 第一步: 4H震荡判断
    (_, isOsc) = H4OscillationScore()
    if not isOsc:
        return

    // 5. 第二步: 1H区间定位
    (zone, zonePct) = H1ZonePosition()

    // 6. 第三步: 入场判断（优先级从高到低）

    // 6a. 假突破做多
    if CheckFakeBreakoutLong():
        TryEnter("up", ["FakeBreakout"], zonePct)
        return

    // 6b. 假突破做空
    if CheckFakeBreakoutShort():
        TryEnter("down", ["FakeBreakout"], zonePct)
        return

    // 6c. 正常做多: 1H接近支撑 + 15M信号
    if zone == "lower":
        signals = M15LongSignals()
        if len(signals) > 0:
            TryEnter("up", signals, zonePct)
            return

    // 6d. 正常做空: 1H接近压力 + 15M信号
    if zone == "upper":
        signals = M15ShortSignals()
        if len(signals) > 0:
            TryEnter("down", signals, zonePct)
            return
```

---

## 六、入场执行逻辑

```
函数 TryEnter(direction, signals, zonePct):

    // === 依次检查过滤器 ===
    if not CheckConsecLoss():   return   // 连亏过滤
    if not CheckH1RSI(dir):     return   // 1H RSI过滤
    if not CheckH1BBWidth():    return   // 1H BB宽度过滤
    if not CheckH4Trend(dir):   return   // 4H趋势过滤
    if not CheckH1Candle(dir):  return   // 1H K线方向过滤

    entry = 当前15M收盘价

    // === 计算止损 ===
    if direction == "up":
        if SLMethod == "m15bb":
            sl = M15_BB_Lower - SLATROffset × M15_ATR
        else:  // "h1bb"
            sl = H1_BB_Lower - SLATROffset × H1_ATR
        slDistance = entry - sl
    else:  // "down"
        if SLMethod == "m15bb":
            sl = M15_BB_Upper + SLATROffset × M15_ATR
        else:
            sl = H1_BB_Upper + SLATROffset × H1_ATR
        slDistance = sl - entry

    if slDistance <= 0: return

    // === 计算仓位 ===
    size = CalcSize(slDistance)
    if size <= 0: return

    // 杠杆限制
    maxSize = AccountValue × Leverage × MaxLeverageRatio / entry
    size = min(size, maxSize)

    // === 计算止盈 ===
    tp = CalcTakeProfit(direction)

    // === R:R 过滤 ===
    if tp != nil:
        if direction == "up":
            tpDistance = tp - entry
        else:
            tpDistance = entry - tp
        rr = tpDistance / slDistance
        if MinRR > 0 and rr < MinRR:
            return

    // === 执行下单 ===
    OpenPosition(direction, size, sl, tp)
    entryPrice = entry
    entryBar = 当前K线索引
    highestProfit = 0
    partialClosed = false
```

---

## 七、止盈计算

```
函数 CalcTakeProfit(direction) → float64 | nil:

    switch TPMethod:
    case "band":
        // 做多止盈 = 1H BB上轨，做空止盈 = 1H BB下轨
        if direction == "up":   return H1_BB_Upper
        else:                   return H1_BB_Lower

    case "mid":
        // 止盈 = 1H BB中轨
        return H1_BB_Mid

    case "mid_atr":
        // 止盈 = 1H BB中轨 ± ATR偏移
        if direction == "up":   return H1_BB_Mid + TPATROffset × H1_ATR
        else:                   return H1_BB_Mid - TPATROffset × H1_ATR

    case "rsi":
        // RSI动态止盈（无固定TP价位，持仓管理中判断）
        return nil

    case "split":
        // 分批止盈（同band模式，第一批在中轨平仓SplitTPRatio比例）
        if direction == "up":   return H1_BB_Upper
        else:                   return H1_BB_Lower
```

---

## 八、仓位计算

```
函数 CalcSize(slDistance) → float64:

    accountValue = GetAccountValue()
    riskPct = RiskPerTrade    // 默认 0.0055 (0.55%)

    // 动态仓位（可选，默认关闭）
    if RiskAfterWin > 0 and RiskAfterLoss > 0 and tradeCount > 0:
        if consecLosses == 0:
            riskPct = RiskAfterWin    // 上次盈利 → 加仓
        else:
            riskPct = RiskAfterLoss   // 上次亏损 → 减仓

    riskAmount = accountValue × riskPct
    if slDistance <= 0:
        return 0

    // 仓位 = 允许亏损金额 / 止损距离
    return riskAmount / slDistance

// 示例: 账户10000 USDT, riskPct=0.0055, slDistance=2%
// → riskAmount = 55 USDT
// → size = 55 / 0.02 = 2750 USDT (合约名义价值)
// → 止损时恰好亏 55 USDT = 0.55%
```

---

## 九、持仓管理

每根15M K线对已有持仓执行以下检查，按优先级从高到低：

```
函数 ManagePosition():

    cur = 当前15M收盘价
    isLong = 持仓方向 == 多头

    // === 更新最大盈利跟踪 ===
    if isLong:  unrealizedProfit = cur - entryPrice
    else:       unrealizedProfit = entryPrice - cur
    highestProfit = max(highestProfit, unrealizedProfit)

    // === 1. 移动止损（默认关闭）===
    if UseTrailingStop:
        activateDist = TrailActivate × M15_ATR
        trailDist = TrailATRMult × M15_ATR
        if highestProfit >= activateDist:
            if isLong:
                newSL = cur - trailDist
                stopLoss = max(stopLoss, newSL)
            else:
                newSL = cur + trailDist
                stopLoss = min(stopLoss, newSL)

    // === 2. 保本止损 ===
    if UseBreakevenStop and takeProfit != nil:
        if isLong:
            tpDist = takeProfit - entryPrice
            if tpDist > 0 and cur >= entryPrice + tpDist × BreakevenActivate:
                // 盈利达60%TP → SL移到成本价
                if stopLoss < entryPrice:
                    stopLoss = entryPrice
        else:
            tpDist = entryPrice - takeProfit
            if tpDist > 0 and cur <= entryPrice - tpDist × BreakevenActivate:
                if stopLoss > entryPrice:
                    stopLoss = entryPrice

    // === 3. 阶梯追踪 ===
    if UseSteppedTrail and takeProfit != nil:
        // Step 1: 价格达TP的90% → SL移到TP距离的65%
        if TrailStep1Pct > 0 and TrailStep1SL > 0:
            if isLong:
                tpDist = takeProfit - entryPrice
                if tpDist > 0 and cur >= entryPrice + tpDist × TrailStep1Pct:
                    newSL = entryPrice + tpDist × TrailStep1SL
                    stopLoss = max(stopLoss, newSL)
            else:
                tpDist = entryPrice - takeProfit
                if tpDist > 0 and cur <= entryPrice - tpDist × TrailStep1Pct:
                    newSL = entryPrice - tpDist × TrailStep1SL
                    stopLoss = min(stopLoss, newSL)

        // Step 2: 同理（默认关闭）

    // === 4. 时间止损（默认关闭）===
    if UseTimeStop and MaxBars > 0:
        barsHeld = currentBarIndex - entryBar
        if barsHeld >= MaxBars:
            ClosePosition()
            return

    // === 5. 止损触发 ===
    if isLong and cur <= stopLoss:
        ClosePosition()
        return
    if not isLong and cur >= stopLoss:
        ClosePosition()
        return

    // === 6. 分批止盈（split模式）===
    if TPMethod == "split" and not partialClosed and SplitTPRatio > 0:
        h1Mid = H1_BB_Mid
        hitMid = (isLong and cur >= h1Mid) or (not isLong and cur <= h1Mid)
        if hitMid:
            closeSize = positionSize × SplitTPRatio
            PartialClose(closeSize)
            partialClosed = true
            stopLoss = entryPrice   // 剩余仓位保本
            return

    // 观测落点：
    // 1. order / all-orders 中会出现 PARTIAL_CLOSE_LONG 或 PARTIAL_CLOSE_SHORT
    // 2. gateway / position-cycles 中 cycle_status 会先进入 PARTIALLY_CLOSED
    // 3. 剩余仓位继续持有，直到最终 CLOSE 订单落地后才转为 CLOSED

    // === 7. 固定止盈 ===
    if takeProfit != nil:
        if isLong and cur >= takeProfit:
            ClosePosition()
            return
        if not isLong and cur <= takeProfit:
            ClosePosition()
            return

    // === 8. RSI动态止盈（rsi模式）===
    if TPMethod == "rsi":
        if isLong and M15_RSI > M15RSIShort:
            ClosePosition()
            return
        if not isLong and M15_RSI < M15RSILong:
            ClosePosition()
            return
```

### 持仓管理优先级总结

| 优先级 | 检查项 | 触发条件 | 动作 |
|--------|--------|---------|------|
| 1 | 移动止损 | 盈利 > TrailActivate × ATR | SL跟随价格移动 |
| 2 | 保本止损 | 盈利 > 60% TP距离 | SL → 成本价 |
| 3 | 阶梯追踪 | 盈利 > 90% TP距离 | SL → TP距离65%处 |
| 4 | 时间止损 | 持仓K线 > MaxBars | 平仓 |
| 5 | 止损 | 价格穿越SL | 平仓 |
| 6 | 分批止盈 | 价格到达1H BB中轨 | 部分平仓+保本 |
| 7 | 固定止盈 | 价格到达TP | 平仓 |
| 8 | RSI止盈 | RSI反向信号 | 平仓 |

分批止盈的查询验证方式：

- `GET /all-orders?symbol=...`：检查是否出现 `PARTIAL_CLOSE_LONG` / `PARTIAL_CLOSE_SHORT`
- `GET /position-cycles?symbol=...`：检查 `cycle_status=PARTIALLY_CLOSED`
- `GET /position-cycles?symbol=...`：关注 `remaining_qty`、`close_progress`、`partial_close_order_count`
- 最终剩余仓位平掉后，`final_close_order_count` 会增加，`cycle_status` 变为 `CLOSED`

分批止盈联调验收清单：

1. 看 `strategy` 信号日志
   预期：先出现 `OPEN`，命中 `TP1` 后出现 `PARTIAL_CLOSE`，最终离场再出现 `CLOSE`

2. 看 `execution` 订单日志
   预期：`PARTIAL_CLOSE` 订单的 `quantity` 是请求减仓数量，不会被放大成全平仓位

3. 看 `GET /all-orders?symbol=...`
   预期：同一个 `position_cycle_id` 下按时间先后能看到 `OPEN_LONG/SHORT`、`PARTIAL_CLOSE_LONG/SHORT`、`CLOSE_LONG/SHORT`

4. 看 `GET /position-cycles?symbol=...`
   预期：
   - 部分止盈后：`cycle_status=PARTIALLY_CLOSED`
   - 同时可见：`partial_close_order_count > 0`
   - 同时可见：`remaining_qty > 0`
   - 最终平仓后：`cycle_status=CLOSED` 且 `final_close_order_count > 0`

如果第 1 步有 `PARTIAL_CLOSE`，但第 3、4 步没有看到对应结果，优先排查 `order` consumer、execution 订单日志落盘、以及 query 是否使用了同一个 `symbol` 和时间范围。

---

## 十、BB触及追踪机制

此机制用于判断"BB反弹"信号——需要知道近期K线是否触及过BB轨道：

```
结构体 TouchTracker:
    touchedLower: bool      // 近期是否触及过15M BB下轨
    touchedUpper: bool      // 近期是否触及过15M BB上轨
    touchLowerBar: int      // 触及下轨时的K线编号
    touchUpperBar: int      // 触及上轨时的K线编号
    barCount: int           // K线计数器

函数 UpdateTouchTracking():

    barCount += 1

    // 记录触及
    if M15_Low <= M15_BB_Lower:
        touchedLower = true
        touchLowerBar = barCount

    if M15_High >= M15_BB_Upper:
        touchedUpper = true
        touchUpperBar = barCount

    // 超过8根K线未再次触及则重置
    if barCount - touchLowerBar > 8:
        touchedLower = false
    if barCount - touchUpperBar > 8:
        touchedUpper = false
```

---

## 十一、指标计算公式

### ADX (Average Directional Index)

```
ADX(period):
    // 标准ADX计算
    1. 计算 +DM, -DM
    2. 平滑得 +DI, -DI
    3. DX = |+DI - -DI| / (+DI + -DI) × 100
    4. ADX = EMA(DX, period)
```

### 布林带 (Bollinger Bands)

```
BB(period, dev):
    mid   = SMA(close, period)
    upper = mid + dev × StdDev(close, period)
    lower = mid - dev × StdDev(close, period)
    width = (upper - lower) / mid
```

### ATR (Average True Range)

```
ATR(period):
    TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    ATR = EMA(TR, period)
```

### RSI (Relative Strength Index)

```
RSI(period):
    gain = max(close - prevClose, 0)
    loss = max(prevClose - close, 0)
    avgGain = EMA(gain, period)
    avgLoss = EMA(loss, period)
    RS = avgGain / avgLoss
    RSI = 100 - 100 / (1 + RS)
```

### EMA (Exponential Moving Average)

```
EMA(period):
    multiplier = 2 / (period + 1)
    EMA = (close - prevEMA) × multiplier + prevEMA
```

---

## 十二、交易手续费与滑点

```
手续费率: 0.10% (taker)   // 合约taker费率
滑点:     0.10%            // 预估滑点
保证金率: 20%              // 5倍杠杆所需保证金率（实际使用2倍杠杆）
```

---

## 十三、回测绩效（2020-2025 ETH/USDT）

| 年份 | 收益率 | 夏普比率 | 最大回撤 | 交易次数 | 胜率 |
|------|--------|----------|----------|----------|------|
| 2020 | 14.64% | 0.71 | 10.09% | 90 | 25.6% |
| 2021 | 20.59% | 1.01 | 14.76% | 75 | 24.0% |
| 2022 | 15.00% | 0.83 | 10.68% | 69 | 30.4% |
| 2023 | 15.00% | 0.70 | 14.94% | 112 | 29.5% |
| 2024 | 39.77% | 1.63 | 9.18% | 73 | 28.8% |
| 2025 | 32.01% | 1.22 | 9.01% | 84 | 31.0% |
| **平均** | **22.84%** | **1.02** | **≤15%** | **84** | **28.2%** |

**关键特征**:
- 胜率低（24%-31%），但盈亏比高，靠大赚小亏盈利
- 最大回撤严格控制在15%以内
- 所有年份均为正收益
- 夏普比率 >1.0 的年份占 3/6

---

## 十四、Golang 实现要点

### 14.1 数据流架构

```
Exchange WebSocket
       │
       ▼
  K线聚合器 (聚合4H/1H/15M)
       │
       ├──→ 4H 指标缓存 (ADX, EMA21, EMA55, ATR)
       ├──→ 1H 指标缓存 (BB, ATR, RSI)
       └──→ 15M 指标缓存 (BB, RSI, ATR)
               │
               ▼
         策略引擎 (每15M触发)
               │
               ├──→ 信号判断
               ├──→ 仓位计算
               └──→ 订单执行
```

### 14.2 关键设计建议

1. **指标缓存**: 每个周期维护独立的指标计算器，K线更新时增量计算，避免全量重算
2. **K线同步**: 1根4H = 4根1H = 16根15M，需处理跨周期K线闭合同步
3. **订单管理**: 同一时间只允许1笔持仓（`MaxPositions=1`），新订单需检查现有持仓
4. **止损单**: 实盘中应使用交易所条件单而非本地判断，避免断线导致止损失效
5. **BB触及追踪**: 需维护最近8根15M K线的高低点与BB轨道的触及记录

### 14.3 Go 推荐库

```go
// 技术指标计算
github.com/markcheno/go-talib          // TA-Lib Go封装

// 交易所接口
github.com/adshao/go-binance            // Binance SDK

// WebSocket
github.com/gorilla/websocket            // WS连接
```

### 14.4 核心结构体设计

```go
type Strategy struct {
    Config    StrategyConfig

    // 指标
    H4Indicators  *Indicators4H   // ADX, EMA21, EMA55, ATR
    H1Indicators  *Indicators1H   // BB, ATR, RSI
    M15Indicators *Indicators15M  // BB, RSI, ATR

    // 触及追踪
    TouchTracker  *TouchTracker

    // 持仓状态
    Position      *Position       // nil = 无持仓
    StopLoss      float64
    TakeProfit    float64
    EntryPrice    float64
    HighestProfit float64
    EntryBarIndex int
    PartialClosed bool

    // 统计
    TradeCount    int
    Wins          int
    Losses        int
    ConsecLosses  int
}

type Position struct {
    Direction Direction  // Long / Short
    Size      float64
    EntryTime time.Time
}

type Direction int
const (
    Long  Direction = 1
    Short Direction = -1
)
```

---

## 十五、风险提示

1. 本策略基于历史回测，过往表现不代表未来收益
2. 胜率仅约28%，需承受频繁亏损的心理压力
3. 加密市场极端行情（如闪崩）可能导致滑点远超预期
4. 杠杆2倍在极端行情下仍有爆仓风险，实盘建议从1倍开始
5. 4H震荡判断在趋势启动时可能延迟识别，造成逆势交易