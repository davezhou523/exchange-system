# Strategy3 技术指标文档
## 框架
语言:golang 1.25
框架:采用微服务架构go-zero
## 数据源
U本位合约模拟交易 API 基础端点：https://demo-fapi.binance.com
U本位合约模拟Websocket baseurl 为 "wss://fstream.binancefuture.com"
合约 API 文档：https://developers.binance.com/docs/derivatives/
从网站:binance.com 获取实时永续合约数据，时区UTC0,模拟交易3个月

获取实时永续合约数据，时区UTC0
接入模拟交易下单、模拟账户同步
go-zero RPC/API 服务编排
## 一、总体架构（go-zero版）

### 创建Kafka主题
./kafka-topics.sh --create --topic kline --bootstrap-server kafka1:9092
./kafka-topics.sh --create --topic signal --bootstrap-server kafka1:9092
./kafka-topics.sh --create --topic order --bootstrap-server kafka1:9092
./kafka-topics.sh --create --topic trade --bootstrap-server kafka1:9092
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic harvest_path_signal --partitions 1 --replication-factor 1
./kafka-topics.sh --list --bootstrap-server kafka1:9092

./kafka-topics.sh --create \
--topic kline \
--bootstrap-server kafka1:9092 \
--partitions 3 \
--replication-factor 1
发送测试消息（验证）
./kafka-console-producer.sh \
--topic kline \
--bootstrap-server kafka1:9092
数据结构:
{"symbol":"ETHUSDT","tf":"1m","close":3000}

消费者（Consumer）
./kafka-console-consumer.sh 
--topic kline \
--from-beginning \
--bootstrap-server kafka2:9092

2026/04/09 22:28:25 Market data sent to topic=kline partition=0 offset=392 data=
{"symbol":"BTCUSDT","interval":"1m","open_time":1775744880000,"open":70523.9,"high":70531.8,"low":70504,"close":70504.7,"volume":44.836,"close_time":1775744939999,"event_time":1775744904600,"first_trade_id":7542726887,"last_trade_id":7542728218,"num_trades":1330,"quote_volume":3161566.8425,"taker_buy_volume":16.386,"taker_buy_quote":1155449.3835}

### 现在创建 K线多周期聚合器，只订阅 1m 数据，在本地聚合成 15m/1h/4h：
时区统一UTC+0
发送kafka必须收盘
// ❗必须过滤未收盘


## 三、API网关设计（api/gateway）
对外提供：
手动下单
查询账户
查询持仓
查询策略状态
### gateway.api（go-zero定义）


### gateway职责
HTTP 请求 → gRPC（zrpc）调用 → execution / strategy / market

### gRPC 正确使用场景（推荐）
1️⃣ 控制层（非常适合）
- 启动策略 / 停止策略 / 查询策略状态（strategy-service）
- 手动下单 / 撤单（execution-service）

2️⃣ 查询接口（同步、可缓存、可限流）
- 获取账户、持仓（execution-service）
- 获取订单状态（execution-service）
- 获取 K 线历史 / 补数据（market-service）

3️⃣ 低频调用（明确超时、幂等、可追踪）
- 风控检查（risk-service）
- 配置更新（strategy-service / risk-service）

不建议用 gRPC 承载高频数据流（行情/信号/成交回报）。这类流式数据建议 Kafka 事件总线（可回放、可扩展、多消费者）。


实际上：
实时数据走 Kafka
RPC用于“补数据 / 查询”
#### 2️⃣ strategy-service（核心）

strategy职责：
订阅 Kafka（kline）
内存维护多周期数据
计算 EMA / RSI / ATR
输出 Signal（再发Kafka）
#### 3️⃣ risk-service

职责：
订阅 Signal
判断是否允许交易
输出approved_signal

#### 4️⃣ execution（最关键）

职责：
接收信号 signal
下单（Binance）
仓位管理
风控
下单路由支持：Binance OKX 模拟撮合（回测 / 仿真）
订单管理
滑点 & 成交模拟（回测用
幂等 & 防重复下单（很重要）

#### 5️⃣ account

### 五、common公共模块设计（非常关键）

### 六、服务通信方式（关键）
- 高频数据流：Kafka（事件驱动，解耦、可扩展、可回放）
- 控制/查询：gRPC（低频、强约束、可设置超时/重试/限流）

数据流（推荐）：
market → Kafka(kline)
strategy → Kafka(signal)
risk → Kafka(approved_signal)
execution → 下单


API 密钥
key: （通过配置文件/环境变量注入，不要写入代码仓库）
secret: （通过配置文件/环境变量注入，不要写入代码仓库）

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

## 收割路径模型（3层结构）

### 设计目标
- 识别价格短时快速扫过关键高低点后立即回撤的插针行情
- 提前判断插针更可能发生在哪一侧、是否具备流动性条件、当前是否接近触发窗口
- 为 execution 提供更稳健的入场过滤、止损保护和被动挂单管理依据

### 模型定位
- 该模型不是单独做方向判断，而是作为趋势策略的微观结构过滤层
- 在 4H/1H/15M 趋势框架基础上，额外判断当前市场是否存在明显的扫损风险
- 当模型判断收割路径风险高时，可用于延迟追价、缩小仓位、放宽确认条件或主动规避

### 模型输入
- K线结构数据：1m、15m K线的高低点、收盘价、成交量、ATR
- 订单簿数据：买一到买N、卖一到卖N 的挂单量、价差、撤单变化
- 成交明细数据：主动买卖成交量、成交方向失衡、短时成交密度
- 时间因子：整点、半点、开盘时段、资金费率结算前后、重要事件窗口

### 第一层：止损密集区识别（核心）

#### 目标
- 找出最可能成为插针目标位的价格区间
- 将离散的结构高低点转化为可计算的候选止损区

#### 核心假设
- 插针往往不是随机波动，而是主动去扫一段集中止损和被动流动性
- 越显眼、越一致、越接近现价的结构位，止损越容易集中

#### 候选区域
- 前高/前低：最近 N 根K线的 swing high / swing low
- 等高顶/等低底：多个高点或低点落在接近价格带内
- 假突破失败位：刚突破又回落的位置，容易聚集追单和反手止损
- 整数关口：如 2300、2350、2400 这类整数价格
- 高频回踩位：短时间内被多次测试但未有效突破的局部结构位

#### 计算思路
- 以价格带而不是单一点表示止损区，避免撮合噪音影响
- 对每个候选区计算 `stop_density_score`
- 分数建议由以下因子加权：
- 触碰次数：同一价格带被测试次数越多，分数越高
- 结构显著性：是否为最近窗口最高点/最低点
- 接近程度：离现价越近，越容易成为下一次扫损目标
- 放量确认：该区域附近历史成交量越大，说明市场关注度越高

#### 输出
- `target_zone_low`
- `target_zone_high`
- `target_side`
- `stop_density_score`

### 第二层：订单簿流动性分析（关键进阶）

#### 目标
- 判断价格是否有能力推进到目标止损区
- 判断扫过目标区后是否存在足够反向流动性支撑快速回撤

#### 核心假设
- 只有当“路径阻力低、扫后承接强”时，插针才更容易形成
- 订单簿结构决定了插针的可执行性，而不是单靠图形结构

#### 重点观察
- 路径成本：现价到目标区之间需要吃掉多少挂单量
- 外侧承接：扫过目标位后，反向盘口是否突然变厚
- 盘口失衡：买卖盘深度是否明显倾斜
- 撤单速度：目标区前方挂单是否被快速撤走

#### 关键指标
- `path_cost`：从现价推进到目标区的累计吃单成本
- `post_sweep_liquidity`：目标区外侧可承接反转的反向深度
- `book_imbalance`：盘口失衡程度
- `cancel_rate`：关键价位附近撤单速度
- `spread_jump`：盘口点差是否突然拉大

#### 判定逻辑
- 向上插针更可能发生：
- 上方止损区分数高
- 现价到上方目标区之间卖盘较薄
- 扫过目标区后卖盘承接变厚
- 向下插针更可能发生：
- 下方止损区分数高
- 现价到下方目标区之间买盘较薄
- 扫过目标区后买盘承接变厚

#### 输出
- `liquidity_sweep_score`
- `expected_sweep_side`
- `expected_reversal_strength`

### 第三层：触发条件（什么时候会扫）

#### 目标
- 判断当前是否接近插针真正发生的时间窗口
- 将静态结构和订单簿状态转化为实时触发信号

#### 常见触发因子
- 波动压缩后突然扩张
- 主动买卖成交量瞬时放大
- 目标位前方盘口快速撤单
- 接近整点、半点、资金费率结算、宏观数据公布窗口
- 连续小单试探同一结构位

#### 关键指标
- `volume_burst_score`：短时成交量突增程度
- `trade_imbalance_score`：主动买卖成交失衡程度
- `cancel_spike_score`：撤单行为突变程度
- `volatility_expansion_score`：波动扩张程度
- `time_window_score`：关键时间窗口加权得分

#### 触发评分
```text
trigger_score =
  a * volume_burst_score +
  b * trade_imbalance_score +
  c * cancel_spike_score +
  d * volatility_expansion_score +
  e * time_window_score
```

#### 触发条件建议
```text
满足以下条件时，判定为高概率插针候选：
1. stop_density_score > 0.75
2. liquidity_sweep_score > 0.70
3. trigger_score > 0.80
```

#### 输出
- `trigger_score`
- `harvest_path_probability`
- `expected_path_depth`
- `expected_reversal_speed`

### 综合决策逻辑
- 第一层回答：扫哪里
- 第二层回答：为什么扫得动
- 第三层回答：什么时候开始扫

#### 统一决策流程
```text
步骤1：扫描最近结构高低点，生成止损候选区
步骤2：基于订单簿计算每个候选区的推进成本和扫后承接
步骤3：结合成交、撤单、波动和时间窗口计算触发分数
步骤4：输出最可能发生插针的一侧和概率等级
步骤5：将结果作为 strategy 的入场过滤和 execution 的保护信号
```

### 风控与执行联动建议

#### 对趋势策略的影响
- 多头趋势中，如果上方收割路径风险高，避免在局部高点直接追多
- 空头趋势中，如果下方收割路径风险高，避免在局部低点直接追空
- 若趋势方向与插针方向相反，可等待插针完成后的回归确认再入场

#### 对仓位管理的影响
- `harvest_path_probability` 高时，降低初始仓位
- `expected_path_depth` 大时，扩大止损缓冲或取消靠近目标区的挂单
- 当插针方向与持仓方向相反时，可提前减仓或收紧风险敞口

#### 对订单执行的影响
- 避免把止损单集中挂在显眼高低点
- 对追价单增加二次确认，减少被短时扫损
- 对被动挂单增加最小安全距离，降低插针误触发

### 与现有 Strategy3 框架的结合方式
- 4H 继续判断大方向，不由插针模型替代
- 1H 继续判断回调健康度
- 15M 继续给出趋势入场信号
- 插针模型作为 1m/15m 级别的微观结构过滤器，为 15M 入场增加一层保护

#### 推荐接入点
- strategy-service：负责计算三层模型分数并输出增强信号
- execution-service：根据收割路径风险调整下单方式、保护单位置和仓位大小
- gateway：用于可视化展示 `target_zone`、`harvest_path_probability` 和 `expected_sweep_side`

### 建议输出字段
```json
{
  "symbol": "ETHUSDT",
  "target_zone_low": 2338.5,
  "target_zone_high": 2340.2,
  "target_side": "UP",
  "stop_density_score": 0.86,
  "liquidity_sweep_score": 0.79,
  "trigger_score": 0.84,
  "harvest_path_probability": 0.82,
  "expected_path_depth": 4.8,
  "expected_reversal_speed": 0.71
}
```

### MVP 实现建议
- 第一阶段：只做结构止损区识别 + 简化版触发条件
- 第二阶段：接入订单簿深度和撤单速率
- 第三阶段：增加插针后回归速度建模和执行联动

### 适用边界
- 该模型更适用于流动性较好、盘口可见的永续合约市场
- 在重大消息面驱动或极端单边趋势下，插针会演化成趋势延续，模型应降低反转预期
- 当订单簿深度数据缺失时，只能输出简化版结构风险分数，不能过度依赖方向结论

## 收割路径模型可编码设计

### Go 结构体设计

#### 1. 原始输入结构
```go
type PriceLevel struct {
	Price    float64
	Quantity float64
}

type OrderBookSnapshot struct {
	Symbol      string
	EventTime   int64
	Bids        []PriceLevel
	Asks        []PriceLevel
	BestBid     float64
	BestAsk     float64
	Spread      float64
	UpdateID    int64
}

type TradeTick struct {
	Symbol      string
	EventTime   int64
	Price       float64
	Quantity    float64
	IsBuyerTaker bool
}

type HarvestPathContext struct {
	Symbol        string
	EventTime     int64
	LastPrice     float64
	Klines1m      []Kline
	Klines5m      []Kline
	Klines15m     []Kline
	OrderBook     *OrderBookSnapshot
	RecentTrades  []TradeTick
}
```

#### 2. 止损候选区结构
```go
type StopZone struct {
	Side              string
	ZoneLow           float64
	ZoneHigh          float64
	ReferencePrice    float64
	Touches           int
	IsSwingExtreme    bool
	IsEqualHighLow    bool
	IsRoundNumber     bool
	HistoricalVolume  float64
	DistanceToMarket  float64
	StopDensityScore  float64
}
```

#### 3. 流动性分析结果结构
```go
type LiquidityAnalysis struct {
	Side                  string
	PathCost              float64
	PostSweepLiquidity    float64
	BookImbalance         float64
	CancelRate            float64
	SpreadJump            float64
	LiquiditySweepScore   float64
	ExpectedReversalScore float64
}
```

#### 4. 触发分析结果结构
```go
type TriggerAnalysis struct {
	VolumeBurstScore        float64
	TradeImbalanceScore     float64
	CancelSpikeScore        float64
	VolatilityExpansionScore float64
	TimeWindowScore         float64
	TriggerScore            float64
}
```

#### 5. 最终输出结构
```go
type HarvestPathSignal struct {
	Symbol                string
	EventTime             int64
	TargetSide            string
	TargetZoneLow         float64
	TargetZoneHigh        float64
	ReferencePrice        float64
	StopDensityScore      float64
	LiquiditySweepScore   float64
	TriggerScore          float64
	HarvestPathProbability       float64
	ExpectedPathDepth     float64
	ExpectedReversalSpeed float64
	MarketPrice           float64
	PathAction            string
	RiskLevel             string
}
```

#### 6. 策略内状态缓存结构
```go
type HarvestPathModelState struct {
	LastSignalBySymbol map[string]HarvestPathSignal
	LastBookBySymbol   map[string]*OrderBookSnapshot
	LastTradesBySymbol map[string][]TradeTick
}
```

### 指标计算伪代码

#### 1. 止损密集区识别
```text
function buildStopZones(klines1m, klines5m, lastPrice):
  candidates = []

  swingHighs = findSwingHighs(klines5m, lookback=20)
  swingLows = findSwingLows(klines5m, lookback=20)
  equalHighZones = clusterNearbyLevels(swingHighs, tolerance=price * 0.0008)
  equalLowZones = clusterNearbyLevels(swingLows, tolerance=price * 0.0008)
  roundNumberZones = buildRoundNumberZones(lastPrice, step=10 or 50)

  for each zone in mergedZones:
    score = 0
    score += normalize(zone.touches) * w1
    score += boolToScore(zone.isSwingExtreme) * w2
    score += boolToScore(zone.isEqualHighLow) * w3
    score += proximityScore(zone.referencePrice, lastPrice) * w4
    score += volumeScore(zone.historicalVolume) * w5
    zone.stopDensityScore = clamp(score, 0, 1)
    candidates.append(zone)

  return sortByScoreDesc(candidates)
```

#### 2. 订单簿流动性分析
```text
function analyzeLiquidity(orderBook, stopZone, lastPrice):
  if orderBook is nil:
    return emptyAnalysis

  pathCost = sumDepthBetween(lastPrice, stopZone.referencePrice, orderBook)
  postSweepLiquidity = sumReverseDepthOutsideZone(stopZone, orderBook)
  bookImbalance = calcBookImbalance(orderBook)
  cancelRate = calcRecentCancelRate(stopZone.side)
  spreadJump = calcSpreadJump(orderBook)

  sweepScore =
    inverseScore(pathCost) * w1 +
    normalize(postSweepLiquidity) * w2 +
    imbalanceScore(bookImbalance, stopZone.side) * w3 +
    normalize(cancelRate) * w4 +
    normalize(spreadJump) * w5

  return LiquidityAnalysis{
    Side: stopZone.side,
    PathCost: pathCost,
    PostSweepLiquidity: postSweepLiquidity,
    BookImbalance: bookImbalance,
    CancelRate: cancelRate,
    SpreadJump: spreadJump,
    LiquiditySweepScore: clamp(sweepScore, 0, 1),
  }
```

#### 3. 触发条件分析
```text
function analyzeTrigger(klines1m, trades, orderBook, now):
  volumeBurst = calcVolumeBurst(klines1m, window=3)
  tradeImbalance = calcTradeImbalance(trades, window=30s)
  cancelSpike = calcCancelSpike(orderBook, window=10s)
  volatilityExpansion = calcVolatilityExpansion(klines1m, atrWindow=14)
  timeWindowScore = calcTimeWindowScore(now)

  triggerScore =
    volumeBurst * a +
    tradeImbalance * b +
    cancelSpike * c +
    volatilityExpansion * d +
    timeWindowScore * e

  return TriggerAnalysis{
    VolumeBurstScore: volumeBurst,
    TradeImbalanceScore: tradeImbalance,
    CancelSpikeScore: cancelSpike,
    VolatilityExpansionScore: volatilityExpansion,
    TimeWindowScore: timeWindowScore,
    TriggerScore: clamp(triggerScore, 0, 1),
  }
```

#### 4. 最终信号决策
```text
function buildHarvestPathSignal(ctx):
  stopZones = buildStopZones(ctx.klines1m, ctx.klines5m, ctx.lastPrice)
  bestZone = stopZones[0]

  liquidity = analyzeLiquidity(ctx.orderBook, bestZone, ctx.lastPrice)
  trigger = analyzeTrigger(ctx.klines1m, ctx.recentTrades, ctx.orderBook, ctx.eventTime)

  harvestPathProbability =
    bestZone.stopDensityScore * 0.4 +
    liquidity.liquiditySweepScore * 0.35 +
    trigger.triggerScore * 0.25

  if harvestPathProbability >= 0.80:
    action = "WAIT_FOR_RECLAIM"
    riskLevel = "PATH_ALERT"
  else if harvestPathProbability >= 0.60:
    action = "REDUCE_PROBE_SIZE"
    riskLevel = "PATH_PRESSURE"
  else:
    action = "FOLLOW_PATH"
    riskLevel = "PATH_CLEAR"

  return HarvestPathSignal(...)
```

### Kafka 信号字段定义

#### 1. 推荐 topic
- `kline`：行情K线
- `signal`：策略交易信号
- `harvest_path_signal`：收割路径增强信号
- `order`：执行成交结果

#### 2. 收割路径信号消息定义
```json
{
  "symbol": "ETHUSDT",
  "event_time": 1776574800000,
  "model": "harvest_path_liquidity_sweep_risk_v1",
  "target_side": "UP",
  "target_zone_low": 2338.50,
  "target_zone_high": 2340.20,
  "reference_price": 2339.80,
  "market_price": 2335.10,
  "stop_density_score": 0.86,
  "liquidity_sweep_score": 0.79,
  "trigger_score": 0.84,
  "harvest_path_probability": 0.82,
  "expected_path_depth": 4.80,
  "expected_reversal_speed": 0.71,
  "path_action": "WAIT_FOR_RECLAIM",
  "risk_level": "PATH_ALERT"
}
```

#### 3. 与现有 `signal` topic 的集成方案
- 方案一：独立发 `harvest_path_signal`，由 execution 同时消费 `signal` 和 `harvest_path_signal`
- 方案二：把插针字段直接并入现有 `signal` 消息，作为增强字段
- 推荐先用方案一，避免直接破坏已有策略信号结构

#### 4. 建议的 Go 消息结构
```go
type HarvestPathSignalMessage struct {
	Symbol                 string  `json:"symbol"`
	EventTime              int64   `json:"event_time"`
	Model                  string  `json:"model"`
	TargetSide             string  `json:"target_side"`
	TargetZoneLow          float64 `json:"target_zone_low"`
	TargetZoneHigh         float64 `json:"target_zone_high"`
	ReferencePrice         float64 `json:"reference_price"`
	MarketPrice            float64 `json:"market_price"`
	StopDensityScore       float64 `json:"stop_density_score"`
	LiquiditySweepScore    float64 `json:"liquidity_sweep_score"`
	TriggerScore           float64 `json:"trigger_score"`
	HarvestPathProbability float64 `json:"harvest_path_probability"`
	ExpectedPathDepth      float64 `json:"expected_path_depth"`
	ExpectedReversalSpeed  float64 `json:"expected_reversal_speed"`
	PathAction             string  `json:"path_action"`
	RiskLevel              string  `json:"risk_level"`
}
```

### strategy-service 中的模块拆分建议

#### 1. 目录建议
```text
app/strategy/rpc/internal/
  strategy/
    harvestpath/
      detector.go
      stop_zone.go
      liquidity.go
      trigger.go
      scorer.go
      types.go
    trend_following.go
  kafka/
    consumer.go
    producer.go
  svc/
    serviceContext.go
```

#### 2. 模块职责划分
- `types.go`
- 定义 `StopZone`、`LiquidityAnalysis`、`TriggerAnalysis`、`HarvestPathSignal`

- `stop_zone.go`
- 负责结构高低点扫描、等高顶等低底聚类、整数关口检测

- `liquidity.go`
- 负责订单簿路径成本、盘口失衡、扫后承接计算

- `trigger.go`
- 负责成交量突增、撤单异常、波动扩张、时间窗口评分

- `scorer.go`
- 负责把三层结果合并成最终 `harvest_path_probability` 和 `path_action`

- `detector.go`
- 对外暴露统一接口，如 `Evaluate(ctx HarvestPathContext) (*HarvestPathSignal, error)`

#### 3. serviceContext 依赖建议
- 持有最近一段时间的 1m/15m K线缓存
- 持有最新订单簿快照缓存
- 持有最近成交缓存
- 持有 `harvest_path_signal` producer
- 持有策略配置和阈值参数

#### 4. 运行流程建议
```text
步骤1：strategy-service 消费 1m Kline
步骤2：更新本地多周期K线缓存
步骤3：拉取或订阅订单簿快照与最近成交
步骤4：调用 harvestpath.Detector.Evaluate()
步骤5：当 harvest_path_probability 超过阈值时发出 harvest_path_signal
步骤6：execution-service 根据 harvest_path_signal 调整下单策略
```

#### 5. 对外接口建议
```go
type Detector interface {
	Evaluate(ctx context.Context, input HarvestPathContext) (*HarvestPathSignal, error)
}

type Publisher interface {
	PublishHarvestPathSignal(ctx context.Context, signal *HarvestPathSignalMessage) error
}
```

#### 6. 配置建议
```yaml
HarvestPathModel:
  Enabled: true
  StopDensityThreshold: 0.75
  LiquiditySweepThreshold: 0.70
  TriggerThreshold: 0.80
  PublishThreshold: 0.60
  BookDepthLevels: 10
  TradeWindowSeconds: 30
  CancelWindowSeconds: 10
```

#### 7. MVP 编码顺序建议
- 第一步：先实现 `stop_zone.go`，只基于 K 线结构输出候选区
- 第二步：补 `trigger.go`，用 1m K线和最近成交生成简化版触发分数
- 第三步：补 `scorer.go`，先输出 `harvest_path_probability`
- 第四步：增加 `harvest_path_signal` Kafka producer
- 第五步：最后再补 `liquidity.go` 接入订单簿深度

这个文档提供了 Strategy3 与收割路径模型的完整设计和可编码实现框架，可作为后续 Golang 开发与 Kafka 信号扩展的基础参考。