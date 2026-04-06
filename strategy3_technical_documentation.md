# Strategy3 技术指标文档
## 框架
语言:golang 1.25
框架:采用微服务架构go-zero
## 数据源
合约模拟交易 API 基础端点：https://demo-fapi.binance.com
合约 API 文档：https://developers.binance.com/docs/derivatives/
从网站:binance.com 获取实时永续合约数据，时区UTC0,模拟交易3个月

获取实时永续合约数据，时区UTC0
接入模拟交易下单、模拟账户同步
go-zero RPC/API 服务编排
## 正确的数据流
           WebSocket（Binance）
                    ↓
          market-service
                    ↓
                Kafka  ←（核心总线）
        ┌──────────┼──────────┐
        ↓          ↓          ↓
strategy-service  risk-service  storage-service
↓
Signal
↓
execution-service
↓
Binance API



API密钥
key:
XyN1sqCupDx2KjLsVF3oMrveIPL9SrJoBcSBGZRLuzSlA4u19ujPtVczjXHLBizF
secret:
8CORcIvrHqwotRofKmIqbNl2AVcUe57HniekMXRd3lUva64hD24grPMS8lWzIy0I

## 概述
策略是一个基于多时间周期分析的趋势跟踪策略，采用"4H趋势判断 + 1H回调确认 + 15M入场信号"的交易框架。

## 多时间周期架构

| 周期 | 主要功能 | 核心指标 |
|------|----------|----------|
| 4小时 | 趋势方向判断 | EMA21, EMA55 |
| 1小时 | 回调位置确认 | EMA21, EMA55, RSI |
| 15分钟 | 入场信号触发 | EMA21, ATR, RSI |

## 技术指标参数配置

### 1. 指标周期参数
```python
# 4小时图指标
h4_ema_fast = 21      # 快速EMA周期（短期趋势）
h4_ema_slow = 55      # 慢速EMA周期（长期趋势）

# 1小时图指标  
h1_ema_fast = 21      # 快速EMA周期
h1_ema_slow = 55      # 慢速EMA周期
h1_rsi_period = 14    # RSI周期

# 15分钟图指标
m15_ema_period = 21   # EMA周期（出场判断）
m15_atr_period = 14   # ATR周期（波动率计算）
m15_rsi_period = 14   # RSI周期（入场信号）
```

### 2. 风险与仓位管理参数
```python
# 风险控制
risk_per_trade = 0.03            # 单笔风险比例（3%）
max_position_size = 0.55         # 最大仓位规模（55%）

# 回调管理
deep_pullback_scale = 0.9        # 深回调仓位缩放系数（90%）
pullback_deep_band = 0.003       # 深回调判定带（0.3%）

# 止损设置
stop_loss_atr_multiplier = 1.5  # 止损距离倍数（1.5×ATR）

# 出场设置
min_holding_bars = 5             # 最小持仓K线数
ema_exit_confirm_bars = 2        # EMA破位确认K线数
ema_exit_buffer_atr = 0.30       # EMA破位缓冲带（0.3×ATR）
```

### 3. 杠杆与约束参数
```python
leverage = 7.0                   # 杠杆倍数
max_leverage_ratio = 0.92        # 最大杠杆使用率（92%）
max_positions = 1                # 最大同时持仓数
max_consecutive_losses = 3       # 最大连续亏损次数
max_daily_loss_pct = 0.07        # 最大日亏损比例（7%）
max_drawdown_pct = 0.15          # 最大回撤比例（15%）
drawdown_position_scale = 0.5    # 回撤仓位缩放系数（50%）
```

### 4. 信号过滤参数
```python
require_both_entry_signals = False  # 入场信号组合模式

# 1小时图RSI过滤
h1_rsi_long_low = 42    # 多头RSI下限
h1_rsi_long_high = 60   # 多头RSI上限
h1_rsi_short_low = 40   # 空头RSI下限
h1_rsi_short_high = 58  # 空头RSI上限

# 15分钟图参数
m15_breakout_lookback = 6     # 突破结构回顾期
m15_rsi_bias_long = 52       # 多头RSI偏置阈值
m15_rsi_bias_short = 48      # 空头RSI偏置阈值
```

## 核心判断逻辑

### 1. 4小时趋势方向判断
```
多头趋势条件：价格 > EMA21 > EMA55
空头趋势条件：价格 < EMA21 < EMA55
震荡趋势：其他情况（不交易）
```

### 2. 1小时回调条件判断

#### 多头回调条件：
- **价格位置**：EMA21 > 价格 > EMA55（健康回调区间）
- **结构完整**：价格 > 前低点（避免反转）
- **EMA趋势**：EMA21持续上升（避免反转）
- **RSI过滤**：RSI ∈ [42, 60]（合理多头区间）

#### 空头回调条件：
- **价格位置**：EMA21 < 价格 < EMA55（健康反弹区间）
- **结构完整**：价格 < 前高点（避免反转）
- **EMA趋势**：EMA21持续下降（避免反转）
- **RSI过滤**：RSI ∈ [40, 58]（合理空头区间）

#### 深回调检测：
```
深回调条件：|价格 - EMA55| / EMA55 ≤ 0.003 (0.3%)
深回调处理：仓位缩放系数 = 0.9（原仓位90%）
```

### 3. 15分钟入场信号判断

#### 入场条件：
- **结构突破**：价格突破/跌破最近6根K线的高低点
- **RSI信号**：RSI穿越50中线或达到偏置阈值

#### 信号组合模式：
- **AND模式**（require_both_entry_signals=True）：需要同时满足结构突破和RSI信号
- **OR模式**（require_both_entry_signals=False）：结构突破或RSI信号任一即可

#### 具体条件：
```
多头入场：
- 结构突破：收盘价 > 近期高点
- RSI信号：RSI > 50 且 前值 ≤ 50
- RSI偏置：RSI ≥ 52

空头入场：
- 结构突破：收盘价 < 近期低点
- RSI信号：RSI < 50 且 前值 ≥ 50
- RSI偏置：RSI ≤ 48
```

## 仓位计算逻辑

### 仓位计算步骤：
1. **风险仓位** = (权益 × 风险比例) ÷ (ATR × 止损倍数)
2. **现金限制** = (权益 × 最大仓位比例) ÷ 价格
3. **杠杆限制** = (权益 × 杠杆 × 最大杠杆使用率) ÷ 价格
4. **基础仓位** = min(风险仓位, 现金限制, 杠杆限制)
5. **最终仓位** = 基础仓位 × 回撤缩放 × 回调缩放

### 缩放系数：
- **回撤缩放**：回撤 ≥ 15% 时，仓位缩放为50%
- **回调缩放**：深回调时，仓位缩放为90%

## 止损止盈设置

### 止损计算：
```
止损距离 = ATR × 1.5
多头止损 = 入场价 - 止损距离
空头止损 = 入场价 + 止损距离
```

### 止盈设置：
- **第一止盈位**：入场价 ± 止损距离（1倍ATR）
- **第二止盈位**：入场价 ± 2倍止损距离（2倍ATR）

### 移动止损：
- 当价格达到第一止盈位时，止损移动到入场价（保本）
- 当价格达到第二止盈位时，平仓一半仓位

## 出场条件判断

### 1. 止损出场
- 价格触及止损位

### 2. 止盈出场
- 部分止盈：价格达到第二止盈位，平仓一半
- 全部止盈：EMA破位或手动平仓

### 3. EMA破位出场
```
EMA破位条件：
- 持仓K线数 ≥ 5根（避免噪音）
- 价格突破EMA21缓冲带
- 连续2根K线确认破位

缓冲带计算：EMA21 ± (ATR × 0.3)
多头破位：价格 < EMA21 - 缓冲带
空头破位：价格 > EMA21 + 缓冲带
```

## 风险管理机制

### 1. 连续亏损限制
- 连续亏损3次后暂停交易

### 2. 日亏损限制
- 当日亏损达到7%后停止当日交易

### 3. 回撤管理
- 回撤达到15%时，仓位规模减半

### 4. 最大持仓限制
- 最多同时持有1个仓位

## 数据要求

### 最小数据长度：
- **4小时图**：至少需要55根K线（EMA55计算）
- **1小时图**：至少需要60根K线（EMA55计算 + 5根缓冲）
- **15分钟图**：至少需要8根K线（突破结构分析 + 2根缓冲）

### 数据完整性检查：
- 在每个判断步骤前检查数据是否足够
- 数据不足时返回False或不进行交易

## Golang实现要点

### 1. 数据结构设计
```go
type StrategyParams struct {
    // 指标周期参数
    H4EmaFast, H4EmaSlow int
    H1EmaFast, H1EmaSlow int
    H1RsiPeriod, M15EmaPeriod, M15AtrPeriod, M15RsiPeriod int
    
    // 风险管理参数
    RiskPerTrade, MaxPositionSize, DeepPullbackScale, PullbackDeepBand float64
    StopLossAtrMultiplier float64
    MinHoldingBars, EmaExitConfirmBars int
    EmaExitBufferAtr float64
    
    // 其他参数...
}

type StrategyState struct {
    CurrentPosition string // "long", "short", ""
    EntryPrice, StopLoss float64
    TakeProfit []float64
    BarsSinceEntry, ConsecutiveLosses int
    // 其他状态变量...
}
```

### 2. 关键算法实现
- 多时间周期EMA、RSI、ATR计算
- 趋势方向判断逻辑
- 回调条件检测
- 入场信号验证
- 仓位大小计算
- 出场条件检查

### 3. 实时数据流处理
- 实现K线数据订阅和更新
- 按时间周期维护指标计算
- 实时信号检测和交易执行

这个文档提供了Strategy3策略的完整技术实现细节，可以作为Golang实时交易系统开发的基础参考。