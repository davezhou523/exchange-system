package strategy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"exchange-system/app/market/rpc/internal/kafka"
	harvestpathmodel "exchange-system/app/market/rpc/internal/strategy/harvestpath"
	"exchange-system/app/market/rpc/internal/weights"
	marketpb "exchange-system/common/pb/market"
)

// ============================================================================
// 多时间周期趋势跟踪策略 (Strategy5)
//
// 架构：4H趋势判断 + 1H入场形态确认 + 15M入场信号
//   - 4小时：判断主趋势方向（EMA21 vs EMA55 vs 价格）
//   - 1小时：确认标准回调或趋势交叉（含 RSI / ADX / MACD 过滤）
//   - 15分钟：寻找入场信号（结构突破 / RSI穿越 + ATR动态止损）
//
// 指标来源：EMA/RSI/ATR 直接读取 Kline，ADX/MACD 基于本地历史K线即时补算
// 止损：ATR × 1.5 动态止损
// 止盈：+1R（保本）/ +3R（平半）+ EMA 破位出场
// 出场：止损/止盈/EMA破位（缓冲带 = 0.3×ATR，连续2根确认）
// ============================================================================

// ---------------------------------------------------------------------------
// 数据结构
// ---------------------------------------------------------------------------

// klineSnapshot 保存一根K线的关键数据（用于结构突破分析和EMA趋势判断）
type klineSnapshot struct {
	OpenTime    int64
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	QuoteVol    float64
	TakerBuy    float64
	IsDirty     bool
	IsTradable  bool
	IsFinal     bool
	DirtyReason string
	Ema21       float64
	Ema55       float64
	Rsi         float64
	Atr         float64
}

// positionSide 持仓方向
type positionSide int

const (
	sideNone  positionSide = iota // 无持仓
	sideLong                      // 多头
	sideShort                     // 空头
)

// position 持仓状态
type position struct {
	side          positionSide
	entryMode     entryMode
	entryPrice    float64
	quantity      float64 // 当前策略视角下的剩余持仓数量，用于分批止盈和完整平仓信号
	stopLoss      float64
	takeProfit1   float64 // 第一目标位（+1R），命中后只把止损抬到保本
	takeProfit2   float64 // 第二目标位（+3R），命中后只平掉剩余仓位的一半
	entryBarTime  int64   // 入场K线时间
	atr           float64 // 入场时ATR，用于EMA破位缓冲带计算
	hitTP1        bool    // 是否已达到第一目标位（触发移动止损）
	partialClosed bool    // 是否已经执行过 +3R 分批止盈，避免重复发送部分平仓信号
	breakBelowCnt int     // EMA破位确认计数
	breakAboveCnt int     // EMA破位确认计数
	maxProfit     float64 // 持仓期间记录的最大浮盈，供移动止损使用
	exitPolicy    ExitPolicyConfig
	exitState     ExitRuntimeState
}

// ExitEventCode 定义分层出场体系的稳定事件码，供日志、信号与分析表复用。
type ExitEventCode string

const (
	ExitEventFastProtect1m ExitEventCode = "exit_fast_protect_1m"
	ExitEventEmergency1s   ExitEventCode = "exit_emergency_stop_1s"
)

// PositionStage 表示当前持仓处于哪个利润阶段，用于决定快速保护减仓力度。
type PositionStage string

const (
	PositionStageEarly       PositionStage = "early"
	PositionStageProfit      PositionStage = "profit"
	PositionStageTrendProfit PositionStage = "trend_profit"
)

// ExitAction 描述一次保护性出场动作，后续可以统一映射为 REDUCE/CLOSE 信号。
type ExitAction struct {
	Code          ExitEventCode
	Interval      string
	Action        string
	SignalType    string
	ReducePct     float64
	RaiseStopTo   float64
	StopRaiseMode string
	Reason        string
	ReasonCode    string
	ReasonLabel   string
	TriggerPrice  float64
	CurrentR      float64
	PositionStage PositionStage
}

// entryMode 表示文档中的 1H 入场形态，用于区分标准回调与趋势交叉。
type entryMode string

const (
	entryModeNone      entryMode = "NONE"      // 1H 没有可用入场形态，标准回调和趋势交叉都未满足。
	entryModePullback  entryMode = "PULLBACK"  // 1H 走标准回调模式，价格回到 EMA21~EMA55 带内后再等低周期触发。
	entryModeCrossover entryMode = "CROSSOVER" // 1H 走趋势交叉模式，要求 EMA21/55 新交叉且 ADX、MACD 同步确认。
)

// h1EntryDecision 汇总 1H 层判断结果，统一携带方向、形态、仓位缩放与调试字段。
type h1EntryDecision struct {
	Pullback pullbackResult
	Mode     entryMode
	Scale    float64
	Extras   map[string]interface{}
}

// trendExitDecision 汇总趋势策略单次出场决策，便于 15m/1m 共用同一套 +1R/+3R 规则。
type trendExitDecision struct {
	Action          string
	Reason          string
	SignalType      string
	Quantity        float64
	Partial         bool
	ExitReasonKind  string
	ExitReasonLabel string
}

// ExitPolicyConfig 保存 15m/1m/1s 分层出场参数，当前先落 1m 快速保护相关部分。
type ExitPolicyConfig struct {
	FastProtectEnabled            bool
	FastProtectMaxTriggerCount    int
	FastProtectReducePctEarly     float64
	FastProtectReducePctProfit    float64
	FastProtectReducePctTrend     float64
	FastProtectBearBars1m         int
	FastProtectStructureLookback  int
	FastProtectVolumeSpikeMult1m  float64
	FastProtectStructureATRBuf    float64
	FastProtectATR1mDropMult      float64
	FastProtectPullbackATR15m     float64
	EarlyProfitR                  float64
	TrendProfitR                  float64
	Emergency1sEnabled            bool
	Emergency1sWindowSec          int
	Emergency1sDropPct            float64
	Emergency1sVolumeSpikeMult    float64
	Emergency1sFullExitNoProfit   bool
	Emergency1sSyntheticDropMult  float64
	Emergency1sSyntheticReducePct float64
}

// ExitRuntimeState 保存持仓期间不断变化的保护状态，避免同一类快速保护重复触发。
type ExitRuntimeState struct {
	MaxFavorablePrice       float64
	InitialRiskDistance     float64
	CurrentR                float64
	Stage                   PositionStage
	FastProtectTriggered    bool
	FastProtectTriggerCount int
	EmergencyTriggered      bool
	LastReduceReason        ExitEventCode
	LastReduceTime          int64
}

// SecondBar 定义秒级极端保护使用的简化行情结构，避免直接依赖完整 1 秒 K 线链路。
type SecondBar struct {
	OpenTimeMs  int64
	CloseTimeMs int64
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	IsFinal     bool
	Synthetic   bool
}

// TrendFollowingStrategy 多时间周期趋势跟踪策略
type TrendFollowingStrategy struct {
	symbol                   string
	params                   map[string]float64
	producer                 *kafka.Producer
	harvestPathProducer      *kafka.Producer
	harvestPathLSTMPredictor *harvestpathmodel.LSTMPredictor
	weightProvider           func(string) (weights.Recommendation, bool)

	// mu 保护以下字段的并发访问
	mu sync.Mutex

	// 多时间周期K线历史（仅保存最近N根，用于结构突破等需要OHLC的场景）
	// 指标直接从 Kline 消息读取，不需要历史来计算
	klines4h  []klineSnapshot
	klines1h  []klineSnapshot
	klines15m []klineSnapshot
	klines1m  []klineSnapshot // 1m K线历史（1m信号模式下使用）

	// 最新指标快照（每个周期只需保留最新的，由 OnKline 更新）
	latest4h    klineSnapshot
	latest1h    klineSnapshot
	latest15m   klineSnapshot
	latest1m    klineSnapshot // 最新1m K线快照
	latestDepth *harvestpathmodel.OrderBookSnapshot
	secondBars  []SecondBar // 秒级极端保护窗口，仅用于 1s 异常急跌保护

	// 1m trading paused log flag (atomic)
	pause1mLogOnce sync.Once

	// 持仓状态
	pos position

	// 风险管理状态
	consecutiveLosses int
	dailyPnl          float64
	dailyPnlDate      string
	peakEquity        float64

	// 信号日志
	signalLogDir   string
	signalLogMu    sync.Mutex
	signalLogFiles map[string]*os.File

	// 决策日志（无信号也记录），目录：{SignalLogDir}/decision/{SYMBOL}/{YYYY-MM-DD}.jsonl
	decisionLogMu    sync.Mutex
	decisionLogFiles map[string]*os.File

	// 分析写入器（可选），用于把 decision/signal 同步写入 ClickHouse。
	analyticsWriter AnalyticsWriter
}

// AnalyticsWriter 定义策略分析事件的可选写入能力。
type AnalyticsWriter interface {
	// WriteSignal 写入策略信号分析事件。
	WriteSignal(entry StrategySignalAnalyticsEntry)
	// WriteDecision 写入策略决策分析事件。
	WriteDecision(entry StrategyDecisionAnalyticsEntry)
}

// StrategySignalAnalyticsEntry 定义 signal_fact 所需的结构化字段。
type StrategySignalAnalyticsEntry struct {
	EventTime       time.Time
	Symbol          string
	StrategyID      string
	Template        string
	SignalID        string
	Action          string
	Side            string
	SignalType      string
	Quantity        float64
	EntryPrice      float64
	StopLoss        float64
	TakeProfitJSON  string
	Reason          string
	ExitReasonKind  string
	ExitReasonLabel string
	RiskReward      float64
	Atr             float64
	TagsJSON        string
	TraceID         string
}

// StrategyDecisionAnalyticsEntry 定义 decision_fact 所需的结构化字段。
type StrategyDecisionAnalyticsEntry struct {
	EventTime   time.Time
	Symbol      string
	StrategyID  string
	Template    string
	Interval    string
	Stage       string
	Decision    string
	Reason      string
	ReasonCode  string
	HasPosition bool
	IsFinal     bool
	IsTradable  bool
	OpenTime    time.Time
	CloseTime   time.Time
	RouteBucket string
	RouteReason string
	ExtrasJSON  string
	TraceID     string
}

type RuntimeOptions struct {
	HarvestPathLSTMPredictor *harvestpathmodel.LSTMPredictor
	WeightProvider           func(string) (weights.Recommendation, bool)
	AnalyticsWriter          AnalyticsWriter
}

// HistoryWarmupStatus 汇总策略实例当前各周期已缓存的K线数量，便于上层判断冷启动恢复是否完整。
type HistoryWarmupStatus struct {
	HistoryLen4h  int
	HistoryLen1h  int
	HistoryLen15m int
	HistoryLen1m  int
}

// WarmupDiagnostics 汇总策略实例当前 warmup 诊断信息，便于启动后快速确认高周期指标是否已恢复。
type WarmupDiagnostics struct {
	HistoryLen4h  int
	HistoryLen1h  int
	HistoryLen15m int
	HistoryLen1m  int
	Latest4hEma21 float64
	Latest4hEma55 float64
	Latest4hClose float64
	Latest4hFinal bool
}

const positionSyncEpsilon = 1e-8

// ---------------------------------------------------------------------------
// 构造函数
// ---------------------------------------------------------------------------

// NewTrendFollowingStrategy 创建多时间周期趋势跟踪策略实例
func NewTrendFollowingStrategy(symbol string, params map[string]float64, producer, harvestPathProducer *kafka.Producer, signalLogDir string, opts *RuntimeOptions) *TrendFollowingStrategy {
	if params == nil {
		params = map[string]float64{}
	}
	return &TrendFollowingStrategy{
		symbol:                   symbol,
		params:                   params,
		producer:                 producer,
		harvestPathProducer:      harvestPathProducer,
		harvestPathLSTMPredictor: runtimeHarvestPathLSTMPredictor(opts),
		weightProvider:           runtimeWeightProvider(opts),
		analyticsWriter:          runtimeAnalyticsWriter(opts),
		signalLogDir:             signalLogDir,
		signalLogFiles:           make(map[string]*os.File),
		decisionLogFiles:         make(map[string]*os.File),
	}
}

// UpdateRuntimeConfig 原地更新策略实例的运行时参数，避免模板切换时重建实例导致多周期缓存丢失。
func (s *TrendFollowingStrategy) UpdateRuntimeConfig(params map[string]float64, signalLogDir string, opts *RuntimeOptions) {
	if s == nil {
		return
	}
	if params == nil {
		params = map[string]float64{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.params = params
	s.signalLogDir = signalLogDir
	s.harvestPathLSTMPredictor = runtimeHarvestPathLSTMPredictor(opts)
	s.weightProvider = runtimeWeightProvider(opts)
	s.analyticsWriter = runtimeAnalyticsWriter(opts)
}

func runtimeHarvestPathLSTMPredictor(opts *RuntimeOptions) *harvestpathmodel.LSTMPredictor {
	if opts == nil {
		return nil
	}
	return opts.HarvestPathLSTMPredictor
}

func runtimeWeightProvider(opts *RuntimeOptions) func(string) (weights.Recommendation, bool) {
	if opts == nil {
		return nil
	}
	return opts.WeightProvider
}

// runtimeAnalyticsWriter 安全读取运行时分析写入器配置。
func runtimeAnalyticsWriter(opts *RuntimeOptions) AnalyticsWriter {
	if opts == nil {
		return nil
	}
	return opts.AnalyticsWriter
}

// isBreakoutVariant 返回当前策略实例是否运行在 Breakout 变体下。
func (s *TrendFollowingStrategy) isBreakoutVariant() bool {
	return int(s.getParam(paramStrategyVariant, 0)) == 1
}

// isRangeVariant 返回当前策略实例是否运行在 Range 变体下。
func (s *TrendFollowingStrategy) isRangeVariant() bool {
	return int(s.getParam(paramStrategyVariant, 0)) == 2
}

// strategySignalTag 返回信号原因中使用的策略标签，便于区分不同策略变体。
func (s *TrendFollowingStrategy) strategySignalTag() string {
	if s != nil && s.isBreakoutVariant() {
		return "breakout"
	}
	if s != nil && s.isRangeVariant() {
		return "range"
	}
	return "trend_following"
}

// strategySignalID 返回当前策略实例对外暴露的稳定 strategy_id。
func (s *TrendFollowingStrategy) strategySignalID(interval string) string {
	prefix := "trend-following"
	if s != nil && s.isBreakoutVariant() {
		prefix = "breakout"
	}
	if s != nil && s.isRangeVariant() {
		prefix = "range"
	}
	if interval == "1m" {
		return fmt.Sprintf("%s-1m-%s", prefix, s.symbol)
	}
	return fmt.Sprintf("%s-%s", prefix, s.symbol)
}

func (s *TrendFollowingStrategy) HasOpenPosition() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pos.side != sideNone
}

func (s *TrendFollowingStrategy) ReconcilePositionWithExchange(longQty, shortQty float64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pos.side == sideNone {
		return false
	}

	switch s.pos.side {
	case sideLong:
		if math.Abs(longQty) <= positionSyncEpsilon {
			log.Printf("[策略纠偏] %s 检测到交易所多头仓位已清空，重置本地持仓状态 | long=%.8f short=%.8f",
				s.symbol, longQty, shortQty)
			s.pos = position{}
			return true
		}
	case sideShort:
		if math.Abs(shortQty) <= positionSyncEpsilon {
			log.Printf("[策略纠偏] %s 检测到交易所空头仓位已清空，重置本地持仓状态 | long=%.8f short=%.8f",
				s.symbol, longQty, shortQty)
			s.pos = position{}
			return true
		}
	}

	return false
}

// ---------------------------------------------------------------------------
// 参数读取
// ---------------------------------------------------------------------------

func (s *TrendFollowingStrategy) getParam(key string, defaultValue float64) float64 {
	if v, ok := s.params[key]; ok {
		return v
	}
	return defaultValue
}

// 参数常量名（与 Python 回测策略对齐）
const (
	// 风险控制
	paramRiskPerTrade     = "risk_per_trade"     // 单笔风险比例（默认3%）
	paramMaxPositionSize  = "max_position_size"  // 最大仓位规模（默认55%）
	paramLeverage         = "leverage"           // 杠杆倍数（默认7）
	paramMaxLeverageRatio = "max_leverage_ratio" // 最大杠杆使用率（默认0.92）

	// 止损止盈
	paramStopLossATRMult  = "stop_loss_atr_multiplier" // 止损距离 = ATR × N（默认1.5）
	paramEmaExitBufferATR = "ema_exit_buffer_atr"      // EMA破位缓冲带（默认0.3×ATR）

	// 出场条件
	paramMinHoldingBars     = "min_holding_bars"      // 最小持仓K线数（默认5）
	paramEmaExitConfirmBars = "ema_exit_confirm_bars" // EMA破位确认K线数（默认2）

	// 回调
	paramDeepPullbackScale = "deep_pullback_scale" // 深回调仓位缩放系数（默认0.9）
	paramPullbackDeepBand  = "pullback_deep_band"  // 深回调判定带（默认0.003=0.3%）
	paramH4AdxPeriod       = "h4_adx_period"       // 4H ADX 周期（默认14）
	paramH1MacdFast        = "h1_macd_fast"        // 1H MACD 快线周期（默认12）
	paramH1MacdSlow        = "h1_macd_slow"        // 1H MACD 慢线周期（默认26）
	paramH1MacdSignal      = "h1_macd_signal"      // 1H MACD 信号线周期（默认9）
	paramCrossoverATRDist  = "crossover_atr_distance"
	paramCrossoverPosScale = "crossover_position_scale"
	paramCrossoverAdxMin   = "crossover_adx_threshold"

	// 信号过滤
	paramRequireBothSignals  = "require_both_entry_signals" // 入场信号组合模式（0=OR, 1=AND）
	paramH1RsiLongLow        = "h1_rsi_long_low"            // 1H多头RSI下限（默认42）
	paramH1RsiLongHigh       = "h1_rsi_long_high"           // 1H多头RSI上限（默认60）
	paramH1RsiShortLow       = "h1_rsi_short_low"           // 1H空头RSI下限（默认40）
	paramH1RsiShortHigh      = "h1_rsi_short_high"          // 1H空头RSI上限（默认58）
	paramM15BreakoutLookback = "m15_breakout_lookback"      // 突破结构回顾期（默认6）
	paramM15RsiBiasLong      = "m15_rsi_bias_long"          // 15M多头RSI偏置（默认52）
	paramM15RsiBiasShort     = "m15_rsi_bias_short"         // 15M空头RSI偏置（默认48）

	// 调试参数
	paramDebugSkipPullback = "debug_skip_pullback"  // 跳过1H回调条件（默认0=不跳过，1=跳过）
	paramDebugSkip4HTrend  = "debug_skip_4h_trend"  // 跳过4H趋势要求（默认0=不跳过，1=跳过，自动根据价格与EMA关系判断方向）
	paramDebugSkip15MEntry = "debug_skip_15m_entry" // 跳过15M入场信号要求（默认0=不跳过，1=跳过，直接生成入场信号）

	// 风险约束
	paramMaxConsecutiveLosses = "max_consecutive_losses"  // 最大连续亏损（默认3）
	paramMaxDailyLossPct      = "max_daily_loss_pct"      // 最大日亏损比例（默认7%）
	paramMaxDrawdownPct       = "max_drawdown_pct"        // 最大回撤比例（默认15%）
	paramDrawdownPosScale     = "drawdown_position_scale" // 回撤仓位缩放（默认0.5）
	paramMaxPositions         = "max_positions"           // 最大同时持仓数（默认1）

	// 权益（简化：使用固定值，实际应从账户查询）
	paramEquity = "equity"

	// 1m信号模式参数
	paramSignalMode                        = "signal_mode"                             // 信号模式: 0=15m(默认) | 1=1m分钟周期
	param1mTradingPaused                   = "1m_trading_paused"                       // 1m成交暂停: 0=正常(默认) | 1=暂停1m成交，仅用15m+1h+4h判断
	param1mRsiPeriod                       = "1m_rsi_period"                           // 1m RSI周期（默认14）
	param1mAtrMult                         = "1m_atr_multiplier"                       // 1m 止损ATR倍数（默认1.5）
	param1mBreakoutLookback                = "1m_breakout_lookback"                    // 1m 突破回顾期（默认20）
	param1mRsiOverbought                   = "1m_rsi_overbought"                       // 1m RSI超买阈值（默认70）
	param1mRsiOversold                     = "1m_rsi_oversold"                         // 1m RSI超卖阈值（默认30）
	param1mRsiBiasLong                     = "1m_rsi_bias_long"                        // 1m 多头RSI偏置（默认55）
	param1mRsiBiasShort                    = "1m_rsi_bias_short"                       // 1m 空头RSI偏置（默认45）
	param1mMinHoldingBars                  = "1m_min_holding_bars"                     // 1m 最小持仓K线数（默认5）
	param1mEmaExitBufferATR                = "1m_ema_exit_buffer_atr"                  // 1m EMA破位缓冲ATR倍数（默认0.3）
	paramExitFastProtectEnabled            = "exit_fast_protect_enabled"               // 是否开启 1m 快速保护：0=关闭 | 1=开启
	paramExitFastProtectMaxTriggerCount    = "exit_fast_protect_max_trigger_count"     // 单次持仓最多允许触发的 1m 快速保护次数（默认1）
	paramExitFastProtectReducePctEarly     = "exit_fast_protect_reduce_pct_early"      // early 阶段快速保护减仓比例（默认0.50）
	paramExitFastProtectReducePctProfit    = "exit_fast_protect_reduce_pct_profit"     // profit 阶段快速保护减仓比例（默认0.40）
	paramExitFastProtectReducePctTrend     = "exit_fast_protect_reduce_pct_trend"      // trend_profit 阶段快速保护减仓比例（默认0.30）
	paramExitFastProtectBearBars1m         = "exit_fast_protect_bear_bars_1m"          // 连续弱势 1m K 线根数（默认3）
	paramExitFastProtectStructureLookback  = "exit_fast_protect_structure_lookback_1m" // 最近 1m 微结构止损参考窗口（默认3）
	paramExitFastProtectVolumeSpike1m      = "exit_fast_protect_volume_spike_mult"     // 1m 放量阈值，使用近20根均量倍数（默认1.5）
	paramExitFastProtectStructureATRBuf    = "exit_fast_protect_structure_atr_buffer"  // 1m 微结构止损附加 ATR 缓冲倍数（默认0.10）
	paramExitFastProtectATR1mDropMult      = "exit_fast_protect_atr1m_drop_mult"       // 单根 1m 急跌阈值，使用 ATR(1m) 倍数（默认1.1）
	paramExitFastProtectPullbackATR15m     = "exit_fast_protect_pullback_atr15m"       // 持仓后高点到当前价的快速回撤阈值，使用 ATR(15m) 倍数（默认0.8）
	paramExitPositionEarlyProfitR          = "exit_position_early_profit_r"            // 进入 profit 阶段的 R 倍数阈值（默认0.8）
	paramExitPositionTrendProfitR          = "exit_position_trend_profit_r"            // 进入 trend_profit 阶段的 R 倍数阈值（默认2.0）
	paramExitEmergency1sEnabled            = "exit_emergency_1s_enabled"               // 是否开启 1s 极端保护：0=关闭 | 1=开启
	paramExitEmergency1sWindowSec          = "exit_emergency_1s_window_sec"            // 1s 极端保护观察窗口秒数（默认5）
	paramExitEmergency1sDropPct            = "exit_emergency_1s_drop_pct"              // 观察窗口内极端跌幅/涨幅阈值（默认0.005=0.5%）
	paramExitEmergency1sVolumeSpikeMult    = "exit_emergency_1s_volume_spike_mult"     // 秒级放量阈值，使用窗口均量倍数（默认2.0）
	paramExitEmergency1sFullExitNoProfit   = "exit_emergency_1s_full_exit_no_profit"   // 未形成利润时是否直接全平：0=否 | 1=是
	paramExitEmergency1sSyntheticDropMult  = "exit_emergency_1s_synthetic_drop_mult"   // synthetic 秒使用的更高跌幅倍数（默认1.5）
	paramExitEmergency1sSyntheticReducePct = "exit_emergency_1s_synthetic_reduce_pct"  // synthetic 秒触发时的保护性减仓比例（默认0.35）

	// 策略变体参数
	paramStrategyVariant         = "strategy_variant"           // 策略变体：0=趋势跟踪（默认）| 1=突破策略 | 2=震荡策略
	paramBreakoutVolumeRatioMin  = "breakout_volume_ratio_min"  // 突破时当前成交量相对均量的最低倍数
	paramBreakoutEntryBufferATR  = "breakout_entry_buffer_atr"  // 突破判定缓冲带 = ATR × N
	paramBreakoutRsiLongMin      = "breakout_rsi_long_min"      // 向上突破时 RSI 最低阈值
	paramBreakoutRsiShortMax     = "breakout_rsi_short_max"     // 向下突破时 RSI 最高阈值
	paramBreakoutRequireEmaTrend = "breakout_require_ema_trend" // 是否要求 EMA 顺势排列：0=否 | 1=是

	// 震荡策略的 1H 震荡判定 + 15M 入场参数
	paramRangeH4AdxPeriod        = "range_h4_adx_period"          // 4H ADX 周期（默认14）
	paramRangeH4AdxMax           = "range_h4_adx_max"             // 4H 震荡判定的 ADX 上限（默认20）
	paramRangeH4EmaClosenessMax  = "range_h4_ema_closeness_max"   // 4H EMA21 与 EMA55 的最大接近比例（默认0.005）
	paramRangeH4ScoreMin         = "range_h4_score_min"           // 4H 震荡评分最低通过分（默认2）
	paramRangeH1AdxPeriod        = "range_h1_adx_period"          // 1H ADX 周期（默认14）
	paramRangeH1AdxMax           = "range_h1_adx_max"             // 1H 震荡判定的 ADX 上限（默认20）
	paramRangeH1BollPeriod       = "range_h1_boll_period"         // 1H Boll 周期（默认20）
	paramRangeH1BollStdDev       = "range_h1_boll_stddev"         // 1H Boll 标准差倍数（默认2）
	paramRangeH1BollWidthMaxPct  = "range_h1_boll_width_max_pct"  // 1H Boll 带宽占中轨比例上限（默认0.05）
	paramRangeH1CandleFilter     = "range_h1_candle_filter"       // 是否要求 1H 当前K线方向与入场方向一致：0=否 | 1=是
	paramRangeM15BollPeriod      = "range_m15_boll_period"        // 15M Boll 周期（默认20）
	paramRangeM15BollStdDev      = "range_m15_boll_stddev"        // 15M Boll 标准差倍数（默认2）
	paramRangeM15BollTouchBuffer = "range_m15_boll_touch_buffer"  // 15M 触边容差，占带宽比例（默认0.05）
	paramRangeM15RsiLongMax      = "range_m15_rsi_long_max"       // 15M 做多 RSI 上限（默认30）
	paramRangeM15RsiShortMin     = "range_m15_rsi_short_min"      // 15M 做空 RSI 下限（默认70）
	paramRangeM15RsiTurnMin      = "range_m15_rsi_turn_min"       // 15M RSI 拐头最小变化（默认0.5）
	paramRangeM15RsiConfirmLong  = "range_m15_rsi_confirm_long"   // 15M 做多时 RSI 从超卖区回升后的确认阈值（默认32）
	paramRangeM15RsiConfirmShort = "range_m15_rsi_confirm_short"  // 15M 做空时 RSI 从超买区回落后的确认阈值（默认68）
	paramRangeLookback           = "range_lookback"               // 震荡区间回看窗口（默认12）
	paramRangeEntryAtrBuffer     = "range_entry_atr_buffer"       // 接近区间边界的缓冲带 = ATR × N
	paramRangeSignalMinCount     = "range_signal_min_count"       // 做多/做空至少需要满足的 15M 信号数量（默认2）
	paramRangeRequireBBBounce    = "range_require_bb_bounce"      // 是否强制要求出现 15M Boll 反弹信号：0=否 | 1=是
	paramRangeUsePinBar          = "range_use_pin_bar"            // 是否启用 Pin Bar 信号：0=否 | 1=是
	paramRangeUseEngulfing       = "range_use_engulfing"          // 是否启用吞没形态信号：0=否 | 1=是
	paramRangePinBarRatio        = "range_pin_bar_ratio"          // Pin Bar 影线与实体的最小倍数（默认2）
	paramRangeZoneProximity      = "range_zone_proximity"         // 价格接近 1H 上下轨的距离比例（默认0.2）
	paramRangeFakeBreakout       = "range_fake_breakout"          // 是否允许在 1H 轨道外做假突破反向入场：0=否 | 1=是
	paramRangeStopAtrOffset      = "range_stop_atr_offset"        // 止损放在 1H 区间边界外的 ATR 偏移倍数（默认0.3）
	paramRangeUseH4TrendFilter   = "range_use_h4_trend_filter"    // 是否启用 4H EMA 方向过滤：0=否 | 1=是
	paramRangeUseH1RsiFilter     = "range_use_h1_rsi_filter"      // 是否启用 1H RSI 过滤：0=否 | 1=是
	paramRangeH1RsiLongMax       = "range_h1_rsi_long_max"        // 做多时 1H RSI 上限（默认0=关闭）
	paramRangeH1RsiShortMin      = "range_h1_rsi_short_min"       // 做空时 1H RSI 下限（默认0=关闭）
	paramRangeMinH1BollWidthPct  = "range_min_h1_boll_width_pct"  // 1H Boll 最小带宽比例（默认0=关闭）
	paramRangeStopMethod         = "range_stop_method"            // 震荡止损模式：0=h1bb | 1=m15bb
	paramRangeTakeProfitMethod   = "range_take_profit_method"     // 震荡止盈模式：0=band | 1=mid | 2=mid_atr | 3=rsi | 4=split
	paramRangeTakeProfitAtrOff   = "range_take_profit_atr_offset" // mid_atr 模式下围绕 1H 中轨的 ATR 偏移倍数
	paramRangeSplitTPRatio       = "range_split_tp_ratio"         // split 模式下首次分批止盈的比例（默认0.5）
	paramRangeMinRR              = "range_min_rr"                 // 震荡策略最小风险收益比（默认0=关闭）
	paramRangeUseTimeStop        = "range_use_time_stop"          // 是否启用时间止损：0=否 | 1=是
	paramRangeMaxBars            = "range_max_bars"               // 时间止损允许持有的最大K线数（默认0=关闭）
	paramRangeUseBreakevenStop   = "range_use_breakeven_stop"     // 是否启用保本止损：0=否 | 1=是
	paramRangeBreakevenActivate  = "range_breakeven_activate"     // 盈利达到目标距离的该比例后将止损抬到成本（默认0.6）
	paramRangeUseSteppedTrail    = "range_use_stepped_trail"      // 是否启用阶梯追踪：0=否 | 1=是
	paramRangeTrailStep1Pct      = "range_trail_step1_pct"        // 阶梯追踪第一阶段触发比例（默认0.9）
	paramRangeTrailStep1SL       = "range_trail_step1_sl"         // 第一阶段把止损推到目标距离的该比例（默认0.65）
	paramRangeUseTrailingStop    = "range_use_trailing_stop"      // 是否启用 ATR 移动止损：0=否 | 1=是
	paramRangeTrailAtrMult       = "range_trail_atr_mult"         // 移动止损使用的 ATR 倍数
	paramRangeTrailActivate      = "range_trail_activate"         // 最大浮盈达到 ATR×该倍数后激活移动止损
	paramRangeTargetAtrMult      = "range_target_atr_multiplier"  // 震荡策略首个止盈使用的 ATR 倍数
	paramRangeRsiLongMax         = "range_rsi_long_max"           // 震荡做多时 RSI 上限，避免追涨
	paramRangeRsiShortMin        = "range_rsi_short_min"          // 震荡做空时 RSI 下限，避免追空
	paramRangeRequireFlatEma     = "range_require_flat_ema"       // 是否要求 EMA 结构相对收敛：0=否 | 1=是
	paramRangeMidNoTradeWidthPct = "range_mid_no_trade_width_pct" // 区间中轴禁做带宽，占整个区间宽度的比例
	paramRangeTrendEmaDriftMax   = "range_trend_ema_drift_max"    // EMA21 在窗口内允许漂移的最大比例，占区间宽度的比例

	// 收割路径风险过滤（默认关闭，按需开启）
	paramHarvestPathModelEnabled                = "harvest_path_model_enabled"                  // 0=关闭 | 1=开启
	paramHarvestPathPublishThreshold            = "harvest_path_publish_threshold"              // harvest_path_signal 发布阈值（默认0.60）
	paramHarvestPathLookback                    = "harvest_path_lookback"                       // 结构识别回看窗口（默认20根1m）
	paramHarvestPathBlockThreshold              = "harvest_path_block_threshold"                // 高风险阻断阈值（默认0.80）
	paramHarvestPathLogThreshold                = "harvest_path_log_threshold"                  // 中风险日志阈值（默认0.60）
	paramHarvestPathLSTMEnabled                 = "harvest_path_lstm_enabled"                   // 0=关闭 | 1=开启
	paramHarvestPathLSTMWeight                  = "harvest_path_lstm_weight"                    // LSTM 概率融合权重（默认0.35）
	paramHarvestPathBookEnabled                 = "harvest_path_book_enabled"                   // 0=关闭 | 1=开启
	paramHarvestPathBookWeight                  = "harvest_path_book_weight"                    // 订单簿概率融合权重（默认0.35）
	paramHarvestPathBookTopN                    = "harvest_path_book_top_n"                     // 订单簿 topN 档位（默认10）
	paramHarvestPathBookSpreadBpsCap            = "harvest_path_book_spread_bps_cap"            // spread 风险归一化上限（默认12bps）
	paramHarvestPathRegimeAwareThresholdEnabled = "harvest_path_regime_aware_threshold_enabled" // 0=关闭 | 1=开启
	paramHarvestPathRegimeLookback              = "harvest_path_regime_lookback"                // 在线波动 regime 识别回看窗口（默认120根1m）

	// 观测日志（无信号也记录）
	paramDecisionLogEnabled = "decision_log_enabled" // 0=关闭(默认) | 1=开启：每根15m final K线写决策日志
)

// ---------------------------------------------------------------------------
// OnKline — 入口
// ---------------------------------------------------------------------------

// OnKline 接收K线数据，按周期分发处理
//
// 处理流程：
//  1. 更新对应周期的K线历史和指标快照
//  2. 根据信号模式（signal_mode）决定触发周期：
//     - 15m模式（默认）：仅在15m K线到达时执行策略判断
//     - 1m模式：在1m K线到达时执行策略判断，用1m级别的价格和指标驱动入场/出场
//  3. 先检查出场条件（持仓时），再检查入场条件（空仓时）
func (s *TrendFollowingStrategy) OnKline(ctx context.Context, k *marketpb.Kline) error {
	if k == nil || k.Symbol != s.symbol || !k.IsClosed {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.applyKlineSnapshotLocked(k) {
		return nil // 忽略其他周期
	}

	// 判断信号模式：0=15m(默认) | 1=1m
	signalMode := int(s.getParam(paramSignalMode, 0))
	isBreakoutVariant := s.isBreakoutVariant()
	isRangeVariant := s.isRangeVariant()

	if signalMode == 1 {
		// 1m信号模式：检查1m成交暂停标志
		pause1m := int(s.getParam(param1mTradingPaused, 0))
		if pause1m == 1 && k.Interval == "1m" {
			// 1m成交暂停：不生成交易信号，但继续更新1m指标快照供多周期判断使用
			s.pause1mLogOnce.Do(func() {
				log.Printf("[策略] %s 1m成交已暂停，仅保留指标计算用于多周期判断", s.symbol)
			})
			return nil
		}

		// 如果1m成交暂停，但收到15m K线，则降级到15m信号模式进行交易判断
		if pause1m == 1 && k.Interval == "15m" {
			if !k.IsFinal {
				return nil
			}
			if s.pos.side != sideNone {
				if isRangeVariant {
					return s.checkRangeExitConditions(ctx, k)
				}
				return s.checkExitConditions(ctx, k)
			}
			if isRangeVariant {
				return s.checkRangeEntryConditions(ctx, k)
			}
			if isBreakoutVariant {
				return s.checkBreakoutEntryConditions(ctx, k)
			}
			return s.checkEntryConditions(ctx, k)
		}

		// 1m信号模式：仅在1m K线到达时执行策略判断
		if k.Interval != "1m" {
			return nil
		}
		// isFinal 守卫：仅在 watermark 确认后的K线上交易
		if !k.IsFinal {
			return nil
		}
		// 持仓时 → 检查出场条件；空仓时 → 检查入场条件
		if s.pos.side != sideNone {
			if isRangeVariant {
				return s.checkRangeExitConditions1m(ctx, k)
			}
			return s.checkExitConditions1m(ctx, k)
		}
		if isRangeVariant {
			return s.checkRangeEntryConditions1m(ctx, k)
		}
		if isBreakoutVariant {
			return s.checkBreakoutEntryConditions1m(ctx, k)
		}
		return s.checkEntryConditions1m(ctx, k)
	}

	// 15m信号模式（默认）：仅在15m K线到达时执行策略判断
	// 4h/1h/1m K线仅用于更新指标快照，供15m入场时读取
	if k.Interval != "15m" {
		return nil
	}

	// isFinal 守卫：仅在 watermark 确认后的K线上交易
	if !k.IsFinal {
		return nil
	}

	// 持仓时 → 检查出场条件
	if s.pos.side != sideNone {
		if isRangeVariant {
			return s.checkRangeExitConditions(ctx, k)
		}
		return s.checkExitConditions(ctx, k)
	}
	if isRangeVariant {
		return s.checkRangeEntryConditions(ctx, k)
	}
	if isBreakoutVariant {
		return s.checkBreakoutEntryConditions(ctx, k)
	}

	// 空仓时 → 检查入场条件
	return s.checkEntryConditions(ctx, k)
}

// WarmupKline 仅回灌历史K线到本地缓存，不触发任何交易判断，供运行时冷启动恢复使用。
func (s *TrendFollowingStrategy) WarmupKline(k *marketpb.Kline) {
	if s == nil || k == nil || k.Symbol != s.symbol || !k.IsClosed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyKlineSnapshotLocked(k)
}

// HistoryLen 返回指定周期当前已缓存的K线数量，便于验证冷启动回灌是否生效。
func (s *TrendFollowingStrategy) HistoryLen(interval string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch interval {
	case "4h":
		return len(s.klines4h)
	case "1h":
		return len(s.klines1h)
	case "15m":
		return len(s.klines15m)
	case "1m":
		return len(s.klines1m)
	default:
		return 0
	}
}

// HistoryWarmupStatus 返回策略实例当前多周期缓存长度快照，供状态查询直接展示 warmup 进度。
func (s *TrendFollowingStrategy) HistoryWarmupStatus() HistoryWarmupStatus {
	if s == nil {
		return HistoryWarmupStatus{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return HistoryWarmupStatus{
		HistoryLen4h:  len(s.klines4h),
		HistoryLen1h:  len(s.klines1h),
		HistoryLen15m: len(s.klines15m),
		HistoryLen1m:  len(s.klines1m),
	}
}

// WarmupDiagnostics 返回策略实例当前 warmup 长度和 4h 关键指标快照，供启动诊断日志使用。
func (s *TrendFollowingStrategy) WarmupDiagnostics() WarmupDiagnostics {
	if s == nil {
		return WarmupDiagnostics{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return WarmupDiagnostics{
		HistoryLen4h:  len(s.klines4h),
		HistoryLen1h:  len(s.klines1h),
		HistoryLen15m: len(s.klines15m),
		HistoryLen1m:  len(s.klines1m),
		Latest4hEma21: s.latest4h.Ema21,
		Latest4hEma55: s.latest4h.Ema55,
		Latest4hClose: s.latest4h.Close,
		Latest4hFinal: s.latest4h.IsFinal,
	}
}

// RecentSecondBars 返回最近收到的秒级行情快照副本，供上层测试或状态检查验证 1s 保护输入。
func (s *TrendFollowingStrategy) RecentSecondBars(limit int) []SecondBar {
	if s == nil || limit == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.secondBars) == 0 {
		return nil
	}
	if limit < 0 || limit > len(s.secondBars) {
		limit = len(s.secondBars)
	}
	start := len(s.secondBars) - limit
	result := make([]SecondBar, limit)
	copy(result, s.secondBars[start:])
	return result
}

// applyKlineSnapshotLocked 把一根已收盘K线写入对应周期缓存，调用方需持有策略锁。
func (s *TrendFollowingStrategy) applyKlineSnapshotLocked(k *marketpb.Kline) bool {
	if s == nil || k == nil || !k.IsClosed {
		return false
	}

	snap := klineSnapshot{
		OpenTime:    k.OpenTime,
		Open:        k.Open,
		High:        k.High,
		Low:         k.Low,
		Close:       k.Close,
		Volume:      k.Volume,
		QuoteVol:    k.QuoteVolume,
		TakerBuy:    k.TakerBuyVolume,
		IsDirty:     k.IsDirty,
		IsTradable:  k.IsTradable,
		IsFinal:     k.IsFinal,
		DirtyReason: k.GetDirtyReason(),
		Ema21:       k.Ema21,
		Ema55:       k.Ema55,
		Rsi:         k.Rsi,
		Atr:         k.Atr,
	}

	switch k.Interval {
	case "4h":
		s.latest4h = snap
		s.klines4h = append(s.klines4h, snap)
		if len(s.klines4h) > 60 {
			s.klines4h = s.klines4h[len(s.klines4h)-60:]
		}
	case "1h":
		s.latest1h = snap
		s.klines1h = append(s.klines1h, snap)
		if len(s.klines1h) > 70 {
			s.klines1h = s.klines1h[len(s.klines1h)-70:]
		}
	case "15m":
		s.latest15m = snap
		s.klines15m = append(s.klines15m, snap)
		if len(s.klines15m) > 80 {
			s.klines15m = s.klines15m[len(s.klines15m)-80:]
		}
	case "1m":
		s.latest1m = snap
		s.klines1m = append(s.klines1m, snap)
		if len(s.klines1m) > 120 {
			s.klines1m = s.klines1m[len(s.klines1m)-120:]
		}
	default:
		return false
	}
	return true
}

func (s *TrendFollowingStrategy) OnDepth(depth *marketpb.Depth) {
	if s == nil || depth == nil || depth.Symbol != s.symbol {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := &harvestpathmodel.OrderBookSnapshot{
		Timestamp: depth.GetTimestamp(),
		Bids:      make([]harvestpathmodel.BookLevel, 0, len(depth.GetBids())),
		Asks:      make([]harvestpathmodel.BookLevel, 0, len(depth.GetAsks())),
	}
	for _, item := range depth.GetBids() {
		if item == nil || item.GetPrice() <= 0 || item.GetQuantity() <= 0 {
			continue
		}
		snapshot.Bids = append(snapshot.Bids, harvestpathmodel.BookLevel{
			Price:    item.GetPrice(),
			Quantity: item.GetQuantity(),
		})
	}
	for _, item := range depth.GetAsks() {
		if item == nil || item.GetPrice() <= 0 || item.GetQuantity() <= 0 {
			continue
		}
		snapshot.Asks = append(snapshot.Asks, harvestpathmodel.BookLevel{
			Price:    item.GetPrice(),
			Quantity: item.GetQuantity(),
		})
	}
	if len(snapshot.Bids) == 0 || len(snapshot.Asks) == 0 {
		return
	}
	s.latestDepth = snapshot
}

// appendSecondBarLocked 把最新秒级行情加入窗口缓存，只保留极端保护所需的最近片段。
func (s *TrendFollowingStrategy) appendSecondBarLocked(bar *SecondBar) {
	if s == nil || bar == nil {
		return
	}
	s.secondBars = append(s.secondBars, *bar)
	if len(s.secondBars) > 30 {
		s.secondBars = s.secondBars[len(s.secondBars)-30:]
	}
}

// OnSecondBar 接收秒级简化行情，仅用于 1s 极端保护，不参与正常入场或常规出场判断。
func (s *TrendFollowingStrategy) OnSecondBar(ctx context.Context, bar *SecondBar) error {
	if s == nil || bar == nil || !bar.IsFinal {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.appendSecondBarLocked(bar)
	if s.pos.side == sideNone {
		return nil
	}

	action := s.evaluateEmergencyExitOn1s()
	if action == nil {
		return nil
	}

	k := secondBarToKline(s.symbol, bar)
	s.writeDecisionLogIfEnabled("exit", "signal", action.ReasonCode, k, map[string]interface{}{
		"event_code":      string(action.Code),
		"event_desc":      action.ReasonLabel,
		"interval":        action.Interval,
		"synthetic_bar":   bar.Synthetic,
		"second_source":   secondBarSourceLabel(bar),
		"signal_type":     action.SignalType,
		"reduce_pct":      action.ReducePct,
		"raise_stop_to":   action.RaiseStopTo,
		"stop_raise_mode": action.StopRaiseMode,
		"trigger_price":   action.TriggerPrice,
		"current_r":       action.CurrentR,
		"position_stage":  action.PositionStage,
	})
	return s.applyEmergencyExitOn1s(ctx, k, action)
}

// secondBarSourceLabel 将秒级条目的来源统一映射为调试字段，便于现场区分真实成交和盘口兜底。
func secondBarSourceLabel(bar *SecondBar) string {
	if bar == nil {
		return "unknown"
	}
	if bar.Synthetic {
		return "depth_fallback"
	}
	return "trade"
}

// secondBarToKline 把秒级极端保护条目转换成最小 Kline 结构，便于复用现有日志与信号链路。
func secondBarToKline(symbol string, bar *SecondBar) *marketpb.Kline {
	if bar == nil {
		return nil
	}
	return &marketpb.Kline{
		Symbol:     symbol,
		Interval:   "1s",
		OpenTime:   bar.OpenTimeMs,
		CloseTime:  bar.CloseTimeMs,
		Open:       bar.Open,
		High:       bar.High,
		Low:        bar.Low,
		Close:      bar.Close,
		Volume:     bar.Volume,
		IsClosed:   true,
		IsFinal:    bar.IsFinal,
		IsTradable: true,
	}
}

// ---------------------------------------------------------------------------
// 第1层：4H趋势方向判断
// ---------------------------------------------------------------------------

// trendDirection 趋势方向
type trendDirection int

const (
	trendNone  trendDirection = iota // 震荡/不确定
	trendLong                        // 多头趋势
	trendShort                       // 空头趋势
)

// judge4HTrend 判断4小时趋势方向
//
// 多头趋势：价格 > EMA21 > EMA55
// 空头趋势：价格 < EMA21 < EMA55
// 震荡：其他情况（不交易）
func (s *TrendFollowingStrategy) judge4HTrend() trendDirection {
	h4 := s.latest4h
	// 指标未就绪
	if h4.Ema21 == 0 || h4.Ema55 == 0 {
		return trendNone
	}

	if h4.Close > h4.Ema21 && h4.Ema21 > h4.Ema55 {
		return trendLong
	}
	if h4.Close < h4.Ema21 && h4.Ema21 < h4.Ema55 {
		return trendShort
	}
	return trendNone
}

// ---------------------------------------------------------------------------
// 第2层：1H回调条件判断
// ---------------------------------------------------------------------------

// pullbackResult 回调判断结果
type pullbackResult int

const (
	pullbackNone  pullbackResult = iota // 不满足回调条件
	pullbackLong                        // 多头回调（买入机会）
	pullbackShort                       // 空头回调（卖出机会）
)

// 兼容旧调用方，只返回 1H 层最终方向，不暴露具体入场形态。
func (s *TrendFollowingStrategy) judge1HPullback(trend trendDirection) pullbackResult {
	return s.evaluate1HEntryDecision(trend).Pullback
}

// 兼容旧签名，供 1m 模式等只关心方向的逻辑复用。
func (s *TrendFollowingStrategy) evaluate1HPullback(trend trendDirection) (pullbackResult, map[string]interface{}) {
	decision := s.evaluate1HEntryDecision(trend)
	return decision.Pullback, decision.Extras
}

// 为模拟测试构造“跳过 1H 入场形态判断”的结果，只保留 4H 方向和 15m 入场。
func (s *TrendFollowingStrategy) buildBypassedH1EntryDecision(trend trendDirection) h1EntryDecision {
	extras := map[string]interface{}{
		"trend":               stringTrend(trend),
		"entry_mode":          stringEntryMode(entryModePullback),
		"debug_skip_pullback": true,
		"h1_bypassed":         true,
	}
	appendSnapshotExtras(extras, "h1", s.latest1h)

	decision := h1EntryDecision{
		Mode:   entryModePullback,
		Scale:  1.0,
		Extras: extras,
	}
	if trend == trendShort {
		decision.Pullback = pullbackShort
	} else {
		decision.Pullback = pullbackLong
	}
	return decision
}

// 按 Strategy5 文档评估 1H 标准回调/趋势交叉，并返回仓位缩放。
func (s *TrendFollowingStrategy) evaluate1HEntryDecision(trend trendDirection) h1EntryDecision {
	h1 := s.latest1h
	extras := map[string]interface{}{
		"trend": stringTrend(trend),
	}
	appendSnapshotExtras(extras, "h1", h1)
	decision := h1EntryDecision{
		Pullback: pullbackNone,
		Mode:     entryModeNone,
		Scale:    0,
		Extras:   extras,
	}
	if h1.Ema21 == 0 || h1.Ema55 == 0 || h1.Rsi == 0 {
		extras["price_in_range"] = false
		extras["rsi_ok"] = false
		extras["structure_ok"] = false
		extras["ema_trend_ok"] = false
		extras["entry_mode"] = stringEntryMode(entryModeNone)
		return decision
	}

	rsiLongLow := s.getParam(paramH1RsiLongLow, 42)
	rsiLongHigh := s.getParam(paramH1RsiLongHigh, 60)
	rsiShortLow := s.getParam(paramH1RsiShortLow, 40)
	rsiShortHigh := s.getParam(paramH1RsiShortHigh, 58)
	deepPullbackScale := s.getParam(paramDeepPullbackScale, 0.9)
	deepPullbackBand := s.getParam(paramPullbackDeepBand, 0.003)
	crossoverATRDist := s.getParam(paramCrossoverATRDist, 0.5)
	crossoverPosScale := s.getParam(paramCrossoverPosScale, 0.9)
	crossoverAdxMin := s.getParam(paramCrossoverAdxMin, 25)
	h4AdxPeriod := int(s.getParam(paramH4AdxPeriod, 14))
	macdFast := int(s.getParam(paramH1MacdFast, 12))
	macdSlow := int(s.getParam(paramH1MacdSlow, 26))
	macdSignal := int(s.getParam(paramH1MacdSignal, 9))

	// 标准回调和趋势交叉都依赖上一根 1H 已完成K线，因此这里提前读出前值。
	emaTrendOk := false
	prevEma21 := 0.0
	prevEma55 := 0.0
	prevLow := 0.0
	prevHigh := 0.0
	if len(s.klines1h) >= 2 {
		prev := s.klines1h[len(s.klines1h)-2]
		prevEma21 = prev.Ema21
		prevEma55 = prev.Ema55
		prevLow = prev.Low
		prevHigh = prev.High
		if trend == trendLong {
			emaTrendOk = h1.Ema21 > prevEma21 // EMA21 持续上升
		} else if trend == trendShort {
			emaTrendOk = h1.Ema21 < prevEma21 // EMA21 持续下降
		}
	}
	extras["ema_trend_ok"] = emaTrendOk
	extras["prev_h1_ema21"] = prevEma21
	extras["prev_h1_ema55"] = prevEma55
	extras["prev_h1_low"] = prevLow
	extras["prev_h1_high"] = prevHigh

	h4Adx, h4AdxReady := calculateADXValue(s.klines4h, h4AdxPeriod)
	macdHist, prevMacdHist, macdReady := calculateLatestMACDHistogram(s.klines1h, macdFast, macdSlow, macdSignal)
	extras["h4_adx"] = h4Adx
	extras["h4_adx_ready"] = h4AdxReady
	extras["h1_macd_hist"] = macdHist
	extras["h1_prev_macd_hist"] = prevMacdHist
	extras["h1_macd_ready"] = macdReady

	if trend == trendLong {
		zoneLow := math.Min(h1.Ema21, h1.Ema55)
		zoneHigh := math.Max(h1.Ema21, h1.Ema55)
		priceInRange := h1.Close >= zoneLow && h1.Close <= zoneHigh
		rsiOk := h1.Rsi >= rsiLongLow && h1.Rsi <= rsiLongHigh

		if h1.Close < zoneLow {
			extras["price_in_range"] = false
			extras["rsi_ok"] = rsiOk
			extras["structure_ok"] = false
			extras["entry_mode"] = stringEntryMode(entryModeNone)
			return decision
		}

		// 结构完整：价格 > 前低点
		structureOk := true
		if prevLow > 0 {
			structureOk = h1.Close > prevLow
		}
		extras["price_in_range"] = priceInRange
		extras["rsi_ok"] = rsiOk
		extras["structure_ok"] = structureOk
		extras["h1_rsi_low"] = rsiLongLow
		extras["h1_rsi_high"] = rsiLongHigh
		extras["deep_pullback"] = h1.Ema55 > 0 && math.Abs(h1.Close-h1.Ema55)/h1.Ema55 <= deepPullbackBand

		// 模式A：标准回调，优先级高于交叉模式。
		if priceInRange && rsiOk && structureOk && emaTrendOk {
			scale := 1.0
			if extras["deep_pullback"].(bool) {
				scale = deepPullbackScale
			}
			extras["entry_mode"] = stringEntryMode(entryModePullback)
			extras["position_scale"] = scale
			decision.Pullback = pullbackLong
			decision.Mode = entryModePullback
			decision.Scale = scale
			return decision
		}

		crossUp := prevEma21 > 0 && prevEma55 > 0 && h1.Ema21 > h1.Ema55 && prevEma21 <= prevEma55
		crossoverLower := h1.Ema21 - 0.3*h1.Atr
		crossoverUpper := h1.Ema21 + crossoverATRDist*h1.Atr
		crossoverPriceOk := h1.Atr > 0 && h1.Close >= crossoverLower && h1.Close <= crossoverUpper
		crossoverAdxOk := h4AdxReady && h4Adx >= crossoverAdxMin
		crossoverMacdOk := macdReady && macdHist > 0 && macdHist > prevMacdHist
		extras["crossover_cross_ok"] = crossUp
		extras["crossover_price_ok"] = crossoverPriceOk
		extras["crossover_adx_ok"] = crossoverAdxOk
		extras["crossover_macd_ok"] = crossoverMacdOk
		extras["crossover_lower"] = crossoverLower
		extras["crossover_upper"] = crossoverUpper
		if crossUp && crossoverPriceOk && crossoverAdxOk && crossoverMacdOk && rsiOk && structureOk {
			extras["entry_mode"] = stringEntryMode(entryModeCrossover)
			extras["position_scale"] = crossoverPosScale
			decision.Pullback = pullbackLong
			decision.Mode = entryModeCrossover
			decision.Scale = crossoverPosScale
			return decision
		}
	} else if trend == trendShort {
		zoneLow := math.Min(h1.Ema21, h1.Ema55)
		zoneHigh := math.Max(h1.Ema21, h1.Ema55)
		priceInRange := h1.Close >= zoneLow && h1.Close <= zoneHigh
		rsiOk := h1.Rsi >= rsiShortLow && h1.Rsi <= rsiShortHigh

		if h1.Close > zoneHigh {
			extras["price_in_range"] = false
			extras["rsi_ok"] = rsiOk
			extras["structure_ok"] = false
			extras["entry_mode"] = stringEntryMode(entryModeNone)
			return decision
		}

		// 结构完整：价格 < 前高点
		structureOk := true
		if prevHigh > 0 {
			structureOk = h1.Close < prevHigh
		}
		extras["price_in_range"] = priceInRange
		extras["rsi_ok"] = rsiOk
		extras["structure_ok"] = structureOk
		extras["h1_rsi_low"] = rsiShortLow
		extras["h1_rsi_high"] = rsiShortHigh
		extras["deep_pullback"] = h1.Ema55 > 0 && math.Abs(h1.Close-h1.Ema55)/h1.Ema55 <= deepPullbackBand

		if priceInRange && rsiOk && structureOk && emaTrendOk {
			scale := 1.0
			if extras["deep_pullback"].(bool) {
				scale = deepPullbackScale
			}
			extras["entry_mode"] = stringEntryMode(entryModePullback)
			extras["position_scale"] = scale
			decision.Pullback = pullbackShort
			decision.Mode = entryModePullback
			decision.Scale = scale
			return decision
		}

		crossDown := prevEma21 > 0 && prevEma55 > 0 && h1.Ema21 < h1.Ema55 && prevEma21 >= prevEma55
		crossoverLower := h1.Ema21 - crossoverATRDist*h1.Atr
		crossoverUpper := h1.Ema21 + 0.3*h1.Atr
		crossoverPriceOk := h1.Atr > 0 && h1.Close >= crossoverLower && h1.Close <= crossoverUpper
		crossoverAdxOk := h4AdxReady && h4Adx >= crossoverAdxMin
		crossoverMacdOk := macdReady && macdHist < 0 && macdHist < prevMacdHist
		extras["crossover_cross_ok"] = crossDown
		extras["crossover_price_ok"] = crossoverPriceOk
		extras["crossover_adx_ok"] = crossoverAdxOk
		extras["crossover_macd_ok"] = crossoverMacdOk
		extras["crossover_lower"] = crossoverLower
		extras["crossover_upper"] = crossoverUpper
		if crossDown && crossoverPriceOk && crossoverAdxOk && crossoverMacdOk && rsiOk && structureOk {
			extras["entry_mode"] = stringEntryMode(entryModeCrossover)
			extras["position_scale"] = crossoverPosScale
			decision.Pullback = pullbackShort
			decision.Mode = entryModeCrossover
			decision.Scale = crossoverPosScale
			return decision
		}
	}

	extras["entry_mode"] = stringEntryMode(entryModeNone)
	return decision
}

func appendSnapshotExtras(extras map[string]interface{}, prefix string, snap klineSnapshot) {
	if extras == nil || prefix == "" {
		return
	}
	extras[prefix+"_open_time"] = formatDecisionLogTime(time.UnixMilli(snap.OpenTime).UTC())
	extras[prefix+"_close"] = snap.Close
	extras[prefix+"_ema21"] = snap.Ema21
	extras[prefix+"_ema55"] = snap.Ema55
	extras[prefix+"_rsi"] = snap.Rsi
	extras[prefix+"_atr"] = snap.Atr
	extras[prefix+"_is_dirty"] = snap.IsDirty
	extras[prefix+"_is_tradable"] = snap.IsTradable
	extras[prefix+"_is_final"] = snap.IsFinal
	extras[prefix+"_dirty_reason"] = snap.DirtyReason
}

// isDeepPullback 判断是否为深回调
// 深回调条件：|价格 - EMA55| / EMA55 ≤ 0.3%
func (s *TrendFollowingStrategy) isDeepPullback() bool {
	h1 := s.latest1h
	if h1.Ema55 == 0 {
		return false
	}
	band := s.getParam(paramPullbackDeepBand, 0.003)
	return math.Abs(h1.Close-h1.Ema55)/h1.Ema55 <= band
}

// calculateADXValue 基于已缓存历史K线计算最新 ADX，用于 4H 趋势强度过滤。
func calculateADXValue(klines []klineSnapshot, period int) (float64, bool) {
	if period <= 0 || len(klines) < 2*period {
		return 0, false
	}

	tr := make([]float64, len(klines))
	plusDM := make([]float64, len(klines))
	minusDM := make([]float64, len(klines))
	for i := 1; i < len(klines); i++ {
		highDiff := klines[i].High - klines[i-1].High
		lowDiff := klines[i-1].Low - klines[i].Low
		if highDiff > lowDiff && highDiff > 0 {
			plusDM[i] = highDiff
		}
		if lowDiff > highDiff && lowDiff > 0 {
			minusDM[i] = lowDiff
		}
		tr[i] = math.Max(
			klines[i].High-klines[i].Low,
			math.Max(math.Abs(klines[i].High-klines[i-1].Close), math.Abs(klines[i].Low-klines[i-1].Close)),
		)
	}

	smoothedTR := 0.0
	smoothedPlusDM := 0.0
	smoothedMinusDM := 0.0
	for i := 1; i <= period; i++ {
		smoothedTR += tr[i]
		smoothedPlusDM += plusDM[i]
		smoothedMinusDM += minusDM[i]
	}

	dxValues := make([]float64, 0, len(klines)-period)
	appendDX := func() {
		if smoothedTR <= 0 {
			return
		}
		plusDI := smoothedPlusDM / smoothedTR * 100
		minusDI := smoothedMinusDM / smoothedTR * 100
		sumDI := plusDI + minusDI
		if sumDI <= 0 {
			return
		}
		dxValues = append(dxValues, math.Abs(plusDI-minusDI)/sumDI*100)
	}
	appendDX()
	for i := period + 1; i < len(klines); i++ {
		smoothedTR = smoothedTR - smoothedTR/float64(period) + tr[i]
		smoothedPlusDM = smoothedPlusDM - smoothedPlusDM/float64(period) + plusDM[i]
		smoothedMinusDM = smoothedMinusDM - smoothedMinusDM/float64(period) + minusDM[i]
		appendDX()
	}
	if len(dxValues) < period {
		return 0, false
	}

	adx := 0.0
	for i := 0; i < period; i++ {
		adx += dxValues[i]
	}
	adx /= float64(period)
	for i := period; i < len(dxValues); i++ {
		adx = ((adx * float64(period-1)) + dxValues[i]) / float64(period)
	}
	return adx, true
}

// calculateLatestMACDHistogram 计算 1H MACD 柱的当前值和前值，供趋势交叉模式确认动量方向。
func calculateLatestMACDHistogram(klines []klineSnapshot, fast, slow, signal int) (float64, float64, bool) {
	if fast <= 0 || slow <= 0 || signal <= 0 || fast >= slow || len(klines) < slow+signal {
		return 0, 0, false
	}

	closes := make([]float64, 0, len(klines))
	for _, k := range klines {
		closes = append(closes, k.Close)
	}
	fastEMA := calculateEMASeries(closes, fast)
	slowEMA := calculateEMASeries(closes, slow)
	macdLine := make([]float64, len(closes))
	for i := range macdLine {
		macdLine[i] = math.NaN()
		if !math.IsNaN(fastEMA[i]) && !math.IsNaN(slowEMA[i]) {
			macdLine[i] = fastEMA[i] - slowEMA[i]
		}
	}

	validMacd := make([]float64, 0, len(macdLine))
	for _, value := range macdLine {
		if math.IsNaN(value) {
			continue
		}
		validMacd = append(validMacd, value)
	}
	signalEMA := calculateEMASeries(validMacd, signal)
	histValues := make([]float64, 0, len(validMacd))
	for i, value := range signalEMA {
		if math.IsNaN(value) {
			continue
		}
		histValues = append(histValues, validMacd[i]-value)
	}
	if len(histValues) < 2 {
		return 0, 0, false
	}
	return histValues[len(histValues)-1], histValues[len(histValues)-2], true
}

// calculateEMASeries 计算完整 EMA 序列，未达到起算长度的位置返回 NaN，便于和 MACD 对齐。
func calculateEMASeries(values []float64, period int) []float64 {
	result := make([]float64, len(values))
	for i := range result {
		result[i] = math.NaN()
	}
	if period <= 0 || len(values) < period {
		return result
	}

	sum := 0.0
	for i := 0; i < period; i++ {
		sum += values[i]
	}
	result[period-1] = sum / float64(period)
	multiplier := 2.0 / float64(period+1)
	for i := period; i < len(values); i++ {
		result[i] = values[i]*multiplier + result[i-1]*(1-multiplier)
	}
	return result
}

// latestStrategyH4Adx 读取当前策略缓存里的最新 4H ADX，供开仓日志和信号指标补充。
func (s *TrendFollowingStrategy) latestStrategyH4Adx() float64 {
	adx, _ := calculateADXValue(s.klines4h, int(s.getParam(paramH4AdxPeriod, 14)))
	return adx
}

// latestStrategyH1MacdHist 读取当前策略缓存里的最新 1H MACD 柱，供开仓日志和信号指标补充。
func (s *TrendFollowingStrategy) latestStrategyH1MacdHist() float64 {
	hist, _, _ := calculateLatestMACDHistogram(
		s.klines1h,
		int(s.getParam(paramH1MacdFast, 12)),
		int(s.getParam(paramH1MacdSlow, 26)),
		int(s.getParam(paramH1MacdSignal, 9)),
	)
	return hist
}

// ---------------------------------------------------------------------------
// 第3层：15M入场信号判断
// ---------------------------------------------------------------------------

// entrySignal 入场信号类型
type entrySignal int

const (
	entryNone  entrySignal = iota // 无入场信号
	entryLong                     // 多头入场
	entryShort                    // 空头入场
)

// judge15MEntry 判断15分钟入场信号
//
// 入场条件（OR模式默认）：
//   - 结构突破：收盘价突破最近N根K线的高低点
//   - RSI信号：RSI穿越50中线 或 达到偏置阈值
//
// 多头入场：
//   - 结构突破：收盘价 > 近期高点
//   - RSI信号：RSI > 50 且 前值 ≤ 50
//   - RSI偏置：RSI ≥ 52
//
// 空头入场：
//   - 结构突破：收盘价 < 近期低点
//   - RSI信号：RSI < 50 且 前值 ≥ 50
//   - RSI偏置：RSI ≤ 48
func (s *TrendFollowingStrategy) judge15MEntry(pullback pullbackResult) entrySignal {
	m15 := s.latest15m
	if m15.Rsi == 0 || m15.Atr == 0 {
		return entryNone
	}

	lookback := int(s.getParam(paramM15BreakoutLookback, 6))
	rsiBiasLong := s.getParam(paramM15RsiBiasLong, 52)
	rsiBiasShort := s.getParam(paramM15RsiBiasShort, 48)
	requireBoth := s.getParam(paramRequireBothSignals, 0) > 0

	// 计算近期高低点（排除当前K线）
	var recentHigh, recentLow float64
	if len(s.klines15m) > 1 {
		start := len(s.klines15m) - 1 - lookback
		if start < 0 {
			start = 0
		}
		recentHigh = s.klines15m[start].High
		recentLow = s.klines15m[start].Low
		for i := start + 1; i < len(s.klines15m)-1; i++ {
			if s.klines15m[i].High > recentHigh {
				recentHigh = s.klines15m[i].High
			}
			if s.klines15m[i].Low < recentLow {
				recentLow = s.klines15m[i].Low
			}
		}
	} else {
		return entryNone // 数据不足
	}

	// 前一根K线的RSI
	var prevRsi float64
	if len(s.klines15m) >= 2 {
		prevRsi = s.klines15m[len(s.klines15m)-2].Rsi
	}

	if pullback == pullbackLong {
		// 多头入场信号
		breakoutUp := m15.Close > recentHigh      // 结构突破：收盘价 > 近期高点
		rsiCross := m15.Rsi > 50 && prevRsi <= 50 // RSI穿越50向上
		rsiBias := m15.Rsi >= rsiBiasLong         // RSI偏置：≥ 52

		structureSignal := breakoutUp
		rsiSignal := rsiCross || rsiBias

		if requireBoth {
			if structureSignal && rsiSignal {
				return entryLong
			}
		} else {
			if structureSignal || rsiSignal {
				return entryLong
			}
		}
	} else if pullback == pullbackShort {
		// 空头入场信号
		breakoutDown := m15.Close < recentLow     // 结构突破：收盘价 < 近期低点
		rsiCross := m15.Rsi < 50 && prevRsi >= 50 // RSI穿越50向下
		rsiBias := m15.Rsi <= rsiBiasShort        // RSI偏置：≤ 48

		structureSignal := breakoutDown
		rsiSignal := rsiCross || rsiBias

		if requireBoth {
			if structureSignal && rsiSignal {
				return entryShort
			}
		} else {
			if structureSignal || rsiSignal {
				return entryShort
			}
		}
	}

	return entryNone
}

// describeTrendM15EntryReject 解释趋势策略 15M 入场未通过的具体原因，避免日志只剩一个 m15_no_entry。
func (s *TrendFollowingStrategy) describeTrendM15EntryReject(pullback pullbackResult) *decisionRejectDetail {
	detail := newDecisionRejectDetail()
	m15 := s.latest15m
	if m15.Rsi == 0 || m15.Atr == 0 {
		detail.append("m15_not_ready_atr_0")
		return detail
	}

	lookback := int(s.getParam(paramM15BreakoutLookback, 6))
	if len(s.klines15m) <= 1 {
		detail.append("m15_history_insufficient")
		return detail
	}
	start := len(s.klines15m) - 1 - lookback
	if start < 0 {
		start = 0
	}
	recentHigh := s.klines15m[start].High
	recentLow := s.klines15m[start].Low
	for i := start + 1; i < len(s.klines15m)-1; i++ {
		if s.klines15m[i].High > recentHigh {
			recentHigh = s.klines15m[i].High
		}
		if s.klines15m[i].Low < recentLow {
			recentLow = s.klines15m[i].Low
		}
	}
	prevRsi := 0.0
	if len(s.klines15m) >= 2 {
		prevRsi = s.klines15m[len(s.klines15m)-2].Rsi
	}
	rsiBiasLong := s.getParam(paramM15RsiBiasLong, 52)
	rsiBiasShort := s.getParam(paramM15RsiBiasShort, 48)

	switch pullback {
	case pullbackLong:
		if !(m15.Close > recentHigh) {
			detail.append("m15_long_structure_missing")
		}
		if !(m15.Rsi > 50 && prevRsi <= 50) && !(m15.Rsi >= rsiBiasLong) {
			detail.append("m15_long_rsi_signal_missing")
		}
	case pullbackShort:
		if !(m15.Close < recentLow) {
			detail.append("m15_short_structure_missing")
		}
		if !(m15.Rsi < 50 && prevRsi >= 50) && !(m15.Rsi <= rsiBiasShort) {
			detail.append("m15_short_rsi_signal_missing")
		}
	default:
		detail.append("m15_no_entry")
	}
	if detail.PrimaryCode == "" {
		detail.append("m15_no_entry")
	}
	return detail
}

// ---------------------------------------------------------------------------
// 入场条件检查
// ---------------------------------------------------------------------------

// checkEntryConditions 检查入场条件（三层过滤 + 风控 + 仓位计算）
func (s *TrendFollowingStrategy) checkEntryConditions(ctx context.Context, k *marketpb.Kline) error {
	// 风控检查
	if !s.checkRiskLimits() {
		s.writeDecisionLogIfEnabled("entry", "skip", "risk_limits_block", k, nil)
		return nil
	}

	// 调试模式：跳过4H趋势和1H回调，直接用15M指标判断方向并入场
	skip4H := s.getParam(paramDebugSkip4HTrend, 0) > 0
	skipPullback := s.getParam(paramDebugSkipPullback, 0) > 0
	skip15M := s.getParam(paramDebugSkip15MEntry, 0) > 0

	if skip4H && skipPullback {
		// 快速调试路径：跳过4H和1H，直接用15M RSI判断方向
		m15 := s.latest15m
		if m15.Atr == 0 {
			log.Printf("[策略] %s 15M指标未就绪（ATR=0），跳过入场", s.symbol)
			s.writeDecisionLogIfEnabled("entry", "skip", "m15_not_ready_atr_0", k, nil)
			return nil
		}

		// 根据15M RSI判断方向：RSI<50空头，RSI>=50多头
		var trend trendDirection
		var pullback pullbackResult
		var entry entrySignal
		if m15.Rsi < 50 {
			trend = trendShort
			pullback = pullbackShort
			entry = entryShort
		} else {
			trend = trendLong
			pullback = pullbackLong
			entry = entryLong
		}
		log.Printf("[策略] %s 快速调试模式 | 15M RSI=%.2f → trend=%v pullback=%v entry=%v ATR=%.2f",
			s.symbol, m15.Rsi, trend, pullback, entry, m15.Atr)

		// 如果还需要跳过15M入场信号判断，上面已经赋值了entry，直接进入开仓
		// 否则走正常的15M入场判断
		if !skip15M {
			entry = s.judge15MEntry(pullback)
			if entry == entryNone {
				log.Printf("[策略] %s 15M无入场 | pullback=%v RSI=%.2f ATR=%.2f",
					s.symbol, pullback, m15.Rsi, m15.Atr)
				reject := s.describeTrendM15EntryReject(pullback)
				extras := map[string]interface{}{
					"trend":    stringTrend(trend),
					"pullback": stringPullback(pullback),
					"m15_rsi":  m15.Rsi,
					"m15_atr":  m15.Atr,
				}
				reject.applyToExtras(extras)
				s.writeDecisionLogIfEnabled("entry", "skip", reject.PrimaryCode, k, extras)
				return nil
			}
		}
		if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
			extras := map[string]interface{}{
				"trend":    stringTrend(trend),
				"pullback": stringPullback(pullback),
				"entry":    stringEntry(entry),
			}
			appendSnapshotExtras(extras, "m15", m15)
			s.writeDecisionLogIfEnabled("entry", "skip", "harvest_path_block", k, extras)
			return nil
		}

		return s.openPosition(ctx, k, trend, pullback, entryModePullback, 1.0, entry, m15)
	}

	// ========== 以下为正常三层过滤逻辑 ==========

	// 第1层：4H趋势判断
	// 检查 4H/1H 指标是否就绪（需要先收到对应周期的闭K线）
	if s.latest4h.Ema21 == 0 || s.latest4h.Ema55 == 0 {
		log.Printf("[策略] %s 4H指标未就绪（等待4H闭K线），跳过入场", s.symbol)
		s.writeDecisionLogIfEnabled("entry", "skip", "h4_not_ready", k, map[string]interface{}{
			"h4_ema21":        s.latest4h.Ema21,
			"h4_ema55":        s.latest4h.Ema55,
			"h4_is_dirty":     s.latest4h.IsDirty,
			"h4_is_tradable":  s.latest4h.IsTradable,
			"h4_is_final":     s.latest4h.IsFinal,
			"h4_dirty_reason": s.latest4h.DirtyReason,
		})
		return nil
	}
	if !skipPullback && (s.latest1h.Ema21 == 0 || s.latest1h.Ema55 == 0) {
		log.Printf("[策略] %s 1H指标未就绪（等待1H闭K线），跳过入场", s.symbol)
		s.writeDecisionLogIfEnabled("entry", "skip", "h1_not_ready", k, map[string]interface{}{
			"h1_ema21":        s.latest1h.Ema21,
			"h1_ema55":        s.latest1h.Ema55,
			"h1_is_dirty":     s.latest1h.IsDirty,
			"h1_is_tradable":  s.latest1h.IsTradable,
			"h1_is_final":     s.latest1h.IsFinal,
			"h1_dirty_reason": s.latest1h.DirtyReason,
		})
		return nil
	}

	trend := s.judge4HTrend()
	if trend == trendNone {
		log.Printf("[策略] %s 4H震荡/无趋势 | EMA21=%.2f EMA55=%.2f 价格=%.2f",
			s.symbol, s.latest4h.Ema21, s.latest4h.Ema55, s.latest4h.Close)
		extras := map[string]interface{}{}
		appendSnapshotExtras(extras, "h4", s.latest4h)
		s.writeDecisionLogIfEnabled("entry", "skip", "h4_no_trend", k, extras)
		return nil
	}

	// 第2层：1H 入场形态确认
	var entryDecision h1EntryDecision
	if skipPullback {
		entryDecision = s.buildBypassedH1EntryDecision(trend)
		log.Printf("[策略] %s 模拟测试模式：跳过1H入场形态判定，仅保留4H趋势 + 15M入场", s.symbol)
	} else {
		entryDecision = s.evaluate1HEntryDecision(trend)
	}

	if entryDecision.Pullback == pullbackNone {
		log.Printf("[策略] %s 1H无入场形态 | trend=%v EMA21=%.2f EMA55=%.2f 价格=%.2f RSI=%.2f",
			s.symbol, trend, s.latest1h.Ema21, s.latest1h.Ema55, s.latest1h.Close, s.latest1h.Rsi)
		s.writeDecisionLogIfEnabled("entry", "skip", "h1_no_pullback", k, entryDecision.Extras)
		return nil
	}

	// 第3层：15M入场信号
	entry := s.judge15MEntry(entryDecision.Pullback)
	if entry == entryNone {
		log.Printf("[策略] %s 15M无入场 | pullback=%v RSI=%.2f ATR=%.2f",
			s.symbol, entryDecision.Pullback, s.latest15m.Rsi, s.latest15m.Atr)
		reject := s.describeTrendM15EntryReject(entryDecision.Pullback)
		extras := map[string]interface{}{
			"trend":      stringTrend(trend),
			"pullback":   stringPullback(entryDecision.Pullback),
			"entry_mode": stringEntryMode(entryDecision.Mode),
		}
		reject.applyToExtras(extras)
		appendSnapshotExtras(extras, "m15", s.latest15m)
		s.writeDecisionLogIfEnabled("entry", "skip", reject.PrimaryCode, k, extras)
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		extras := map[string]interface{}{
			"trend":      stringTrend(trend),
			"pullback":   stringPullback(entryDecision.Pullback),
			"entry":      stringEntry(entry),
			"entry_mode": stringEntryMode(entryDecision.Mode),
		}
		appendSnapshotExtras(extras, "h4", s.latest4h)
		appendSnapshotExtras(extras, "h1", s.latest1h)
		appendSnapshotExtras(extras, "m15", s.latest15m)
		s.writeDecisionLogIfEnabled("entry", "skip", "harvest_path_block", k, extras)
		return nil
	}

	return s.openPosition(ctx, k, trend, entryDecision.Pullback, entryDecision.Mode, entryDecision.Scale, entry, s.latest15m)
}

// openPosition 计算止损止盈并发送入场信号
func (s *TrendFollowingStrategy) openPosition(ctx context.Context, k *marketpb.Kline, trend trendDirection, pullback pullbackResult, mode entryMode, scale float64, entry entrySignal, m15 klineSnapshot) error {
	atrMult := s.getParam(paramStopLossATRMult, 1.5)
	stopDist := atrMult * m15.Atr

	var action, side string
	var stopLoss, tp1, tp2 float64

	if entry == entryLong {
		action = "BUY"
		side = "LONG"
		stopLoss = m15.Close - stopDist
		tp1 = m15.Close + stopDist   // +1R
		tp2 = m15.Close + 3*stopDist // +3R
	} else {
		action = "SELL"
		side = "SHORT"
		stopLoss = m15.Close + stopDist
		tp1 = m15.Close - stopDist   // +1R
		tp2 = m15.Close - 3*stopDist // +3R
	}

	// 仓位计算
	quantity := s.calculatePositionSize(m15.Close, stopLoss)

	// 统一使用 1H 决策返回的仓位缩放，避免深回调缩放和交叉缩放叠加。
	if scale > 0 {
		quantity *= scale
	}
	adjustedQuantity, blocked, blockReason := s.applyWeightRecommendation(quantity)
	if blocked {
		extras := map[string]interface{}{
			"base_quantity": quantity,
			"reason":        blockReason,
		}
		appendSnapshotExtras(extras, "h4", s.latest4h)
		appendSnapshotExtras(extras, "h1", s.latest1h)
		appendSnapshotExtras(extras, "m15", m15)
		s.writeDecisionLogIfEnabled("entry", "skip", blockReason, k, extras)
		log.Printf("[策略入场跳过] %s %s | base_qty=%.4f | %s", s.symbol, action, quantity, blockReason)
		return nil
	}
	quantity = adjustedQuantity

	// 构建信号原因
	reason := s.buildEntryReason(trend, pullback, mode, entry, m15, stopLoss, tp1, tp2)
	signalReason := s.newSignalReason(
		reason,
		"OPEN_ENTRY",
		fmt.Sprintf("4H趋势=%s", describeTrendContext(trend)),
		fmt.Sprintf("1H形态=%s | 15M入场确认", describeEntrySetupContext(mode, pullback)),
		fmt.Sprintf("RSI=%.2f ATR=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f", m15.Rsi, m15.Atr, stopLoss, tp1, tp2),
		"15m", "trend_following", "open", side,
	)

	// 记录持仓
	exitPolicy, exitState := s.initializeExitPolicyState(m15.Close, stopLoss)
	s.pos = position{
		side:          sideLong,
		entryMode:     mode,
		entryPrice:    m15.Close,
		quantity:      quantity,
		stopLoss:      stopLoss,
		takeProfit1:   tp1,
		takeProfit2:   tp2,
		entryBarTime:  m15.OpenTime,
		atr:           m15.Atr,
		hitTP1:        false,
		partialClosed: false,
		exitPolicy:    exitPolicy,
		exitState:     exitState,
	}
	if entry == entryShort {
		s.pos.side = sideShort
	}

	// 计算风险收益比：TP1距离 / 止损距离
	riskReward := 0.0
	tpStopDist := math.Abs(m15.Close - stopLoss)
	if tpStopDist > 0 {
		riskReward = math.Abs(tp1-m15.Close) / tpStopDist
	}

	// 发送开仓信号
	signal := map[string]interface{}{
		"strategy_id":   s.strategySignalID("15m"),
		"symbol":        s.symbol,
		"interval":      "15m",
		"action":        action,
		"side":          side,
		"signal_type":   "OPEN",
		"quantity":      quantity,
		"entry_price":   m15.Close,
		"stop_loss":     stopLoss,
		"take_profits":  []float64{tp1, tp2},
		"reason":        signalReason.Summary,
		"signal_reason": signalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           m15.Atr,
		"risk_reward":   riskReward,
		"indicators": map[string]interface{}{
			"h4_ema21":       s.latest4h.Ema21,
			"h4_ema55":       s.latest4h.Ema55,
			"h4_adx":         s.latestStrategyH4Adx(),
			"h1_ema21":       s.latest1h.Ema21,
			"h1_ema55":       s.latest1h.Ema55,
			"h1_rsi":         s.latest1h.Rsi,
			"h1_macd_hist":   s.latestStrategyH1MacdHist(),
			"m15_ema21":      m15.Ema21,
			"m15_rsi":        m15.Rsi,
			"m15_atr":        m15.Atr,
			"entry_mode":     stringEntryMode(mode),
			"position_scale": scale,
		},
	}

	log.Printf("[策略入场] %s %s | 价格=%.2f | 止损=%.2f | TP1=%.2f | TP2=%.2f | %s",
		s.symbol, action, m15.Close, stopLoss, tp1, tp2, reason)

	extras := map[string]interface{}{
		"action":         action,
		"side":           side,
		"quantity":       quantity,
		"entry_price":    m15.Close,
		"stop_loss":      stopLoss,
		"take_profits":   []float64{tp1, tp2},
		"entry_mode":     stringEntryMode(mode),
		"position_scale": scale,
	}
	appendSnapshotExtras(extras, "h4", s.latest4h)
	appendSnapshotExtras(extras, "h1", s.latest1h)
	appendSnapshotExtras(extras, "m15", m15)
	s.writeDecisionLogIfEnabled("entry", "signal", "open_signal_sent", k, extras)
	return s.sendSignal(ctx, signal, k)
}

// buildEntryReason 构建入场信号原因描述
func (s *TrendFollowingStrategy) buildEntryReason(trend trendDirection, pullback pullbackResult, mode entryMode, entry entrySignal, m15 klineSnapshot, stopLoss, tp1, tp2 float64) string {
	return fmt.Sprintf(
		"4H趋势=%s | 1H形态=%s | 15M入场=%s | RSI=%.2f ATR=%.2f | 止损=%.2f(1.5×ATR) TP1=%.2f(+1R) TP2=%.2f(+3R)",
		describeTrendContext(trend), describeEntrySetupContext(mode, pullback), stringEntry(entry), m15.Rsi, m15.Atr, stopLoss, tp1, tp2,
	)
}

// ---------------------------------------------------------------------------
// 出场条件检查
// ---------------------------------------------------------------------------

// evaluateTrendExitDecision 按文档统一执行止损、+1R 保本、+3R 平半和 EMA 破位判断。
func (s *TrendFollowingStrategy) evaluateTrendExitDecision(snap klineSnapshot, heldBars, minHoldingBars, exitConfirmBars int, emaBufferATR float64, interval string) trendExitDecision {
	price := snap.Close

	// 1. 止损优先级最高。
	if s.pos.side == sideLong && price <= s.pos.stopLoss {
		exitReasonKind, exitReasonLabel := s.currentStopExitReason()
		return trendExitDecision{
			Action:          "SELL",
			Reason:          fmt.Sprintf("[%s] 多头止损：价格%.2f ≤ 止损%.2f", interval, price, s.pos.stopLoss),
			SignalType:      "CLOSE",
			Quantity:        s.pos.quantity,
			ExitReasonKind:  exitReasonKind,
			ExitReasonLabel: exitReasonLabel,
		}
	}
	if s.pos.side == sideShort && price >= s.pos.stopLoss {
		exitReasonKind, exitReasonLabel := s.currentStopExitReason()
		return trendExitDecision{
			Action:          "BUY",
			Reason:          fmt.Sprintf("[%s] 空头止损：价格%.2f ≥ 止损%.2f", interval, price, s.pos.stopLoss),
			SignalType:      "CLOSE",
			Quantity:        s.pos.quantity,
			ExitReasonKind:  exitReasonKind,
			ExitReasonLabel: exitReasonLabel,
		}
	}

	// 2. +1R 只移动止损到保本，不直接出场。
	if s.pos.side == sideLong && price >= s.pos.takeProfit1 && !s.pos.hitTP1 {
		s.pos.hitTP1 = true
		s.pos.stopLoss = s.pos.entryPrice
		log.Printf("[策略%s] %s 多头达到 +1R，止损抬到保本价%.2f", interval, s.symbol, s.pos.entryPrice)
	}
	if s.pos.side == sideShort && price <= s.pos.takeProfit1 && !s.pos.hitTP1 {
		s.pos.hitTP1 = true
		s.pos.stopLoss = s.pos.entryPrice
		log.Printf("[策略%s] %s 空头达到 +1R，止损抬到保本价%.2f", interval, s.symbol, s.pos.entryPrice)
	}

	// 3. +3R 命中后只平半仓一次。
	if !s.pos.partialClosed && s.pos.quantity > 0 {
		if s.pos.side == sideLong && price >= s.pos.takeProfit2 {
			return trendExitDecision{
				Action:          "SELL",
				Reason:          fmt.Sprintf("[%s] 多头 +3R 分批止盈：价格%.2f ≥ TP2%.2f，减仓50%%", interval, price, s.pos.takeProfit2),
				SignalType:      "PARTIAL_CLOSE",
				Quantity:        s.pos.quantity * 0.5,
				Partial:         true,
				ExitReasonKind:  "partial_take_profit",
				ExitReasonLabel: "分批止盈",
			}
		}
		if s.pos.side == sideShort && price <= s.pos.takeProfit2 {
			return trendExitDecision{
				Action:          "BUY",
				Reason:          fmt.Sprintf("[%s] 空头 +3R 分批止盈：价格%.2f ≤ TP2%.2f，减仓50%%", interval, price, s.pos.takeProfit2),
				SignalType:      "PARTIAL_CLOSE",
				Quantity:        s.pos.quantity * 0.5,
				Partial:         true,
				ExitReasonKind:  "partial_take_profit",
				ExitReasonLabel: "分批止盈",
			}
		}
	}

	// 4. EMA 破位要求达到最小持仓根数，并且连续确认。
	if heldBars >= minHoldingBars && snap.Ema21 != 0 {
		buffer := emaBufferATR * s.pos.atr
		if s.pos.side == sideLong {
			if price < snap.Ema21-buffer {
				s.pos.breakBelowCnt++
				s.pos.breakAboveCnt = 0
				if s.pos.breakBelowCnt >= exitConfirmBars {
					return trendExitDecision{
						Action:          "SELL",
						Reason:          fmt.Sprintf("[%s] 多头EMA破位：价格 %.2f < EMA21 %.2f - 缓冲 %.2f（连续 %d 根确认）", interval, price, snap.Ema21, buffer, s.pos.breakBelowCnt),
						SignalType:      "CLOSE",
						Quantity:        s.pos.quantity,
						ExitReasonKind:  "final_close",
						ExitReasonLabel: "最终平仓",
					}
				}
			} else {
				s.pos.breakBelowCnt = 0
			}
		} else if s.pos.side == sideShort {
			if price > snap.Ema21+buffer {
				s.pos.breakAboveCnt++
				s.pos.breakBelowCnt = 0
				if s.pos.breakAboveCnt >= exitConfirmBars {
					return trendExitDecision{
						Action:          "BUY",
						Reason:          fmt.Sprintf("[%s] 空头EMA破位：价格 %.2f > EMA21 %.2f + 缓冲 %.2f（连续 %d 根确认）", interval, price, snap.Ema21, buffer, s.pos.breakAboveCnt),
						SignalType:      "CLOSE",
						Quantity:        s.pos.quantity,
						ExitReasonKind:  "final_close",
						ExitReasonLabel: "最终平仓",
					}
				}
			} else {
				s.pos.breakAboveCnt = 0
			}
		}
	}

	return trendExitDecision{}
}

// applyTrendExitDecision 负责发送趋势策略出场信号，并按是否分批止盈更新本地持仓状态。
func (s *TrendFollowingStrategy) applyTrendExitDecision(ctx context.Context, k *marketpb.Kline, snap klineSnapshot, interval string, decision trendExitDecision) error {
	if decision.SignalType == "" || decision.Action == "" || s.pos.side == sideNone {
		return nil
	}

	side := "LONG"
	if s.pos.side == sideShort {
		side = "SHORT"
	}
	quantity := decision.Quantity
	if decision.Partial {
		if quantity <= 0 || quantity >= s.pos.quantity {
			quantity = s.pos.quantity * 0.5
		}
		if quantity <= 0 {
			return nil
		}
	} else {
		quantity = s.pos.quantity
	}

	pnl := s.calculatePnL(snap.Close)
	executionContext := fmt.Sprintf("平仓价=%.2f | 已实现盈亏=%.2f | 止损=%.2f", snap.Close, pnl, s.pos.stopLoss)
	if decision.Partial {
		executionContext = fmt.Sprintf("减仓价=%.2f | 减仓数量=%.4f | 剩余仓位=%.4f | 止损=%.2f", snap.Close, quantity, math.Max(0, s.pos.quantity-quantity), s.pos.stopLoss)
	}
	exitSignalReason := s.newExitSignalReason(
		decision.Reason,
		fmt.Sprintf("持仓方向=%s | 周期=%s | 形态=%s", side, interval, stringEntryMode(s.pos.entryMode)),
		decision.Reason,
		executionContext,
		decision.ExitReasonKind,
		decision.ExitReasonLabel,
		interval, s.strategySignalTag(), "close", side,
	)
	signal := map[string]interface{}{
		"strategy_id":   s.strategySignalID(interval),
		"symbol":        s.symbol,
		"interval":      interval,
		"action":        decision.Action,
		"side":          side,
		"signal_type":   decision.SignalType,
		"quantity":      quantity,
		"entry_price":   snap.Close,
		"stop_loss":     s.pos.stopLoss,
		"take_profits":  []float64{s.pos.takeProfit1, s.pos.takeProfit2},
		"reason":        exitSignalReason.Summary,
		"signal_reason": exitSignalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           snap.Atr,
		"risk_reward":   0,
		"indicators": map[string]interface{}{
			"entry_mode": stringEntryMode(s.pos.entryMode),
			"pnl":        pnl,
		},
	}
	if interval == "15m" {
		signal["indicators"].(map[string]interface{})["m15_ema21"] = snap.Ema21
		signal["indicators"].(map[string]interface{})["m15_rsi"] = snap.Rsi
		signal["indicators"].(map[string]interface{})["m15_atr"] = snap.Atr
	} else {
		signal["indicators"].(map[string]interface{})["m1_ema21"] = snap.Ema21
		signal["indicators"].(map[string]interface{})["m1_rsi"] = snap.Rsi
		signal["indicators"].(map[string]interface{})["m1_atr"] = snap.Atr
	}
	if err := s.sendSignal(ctx, signal, k); err != nil {
		return err
	}

	if decision.Partial {
		s.pos.quantity = math.Max(0, s.pos.quantity-quantity)
		s.pos.partialClosed = true
		log.Printf("[策略%s出场] %s %s | 价格=%.2f | 数量=%.4f | %s", interval, s.symbol, decision.Action, snap.Close, quantity, decision.Reason)
		return nil
	}

	s.updateRiskState(pnl)
	s.pos = position{}
	log.Printf("[策略%s出场] %s %s | 价格=%.2f | 盈亏=%.2f | %s", interval, s.symbol, decision.Action, snap.Close, pnl, decision.Reason)
	return nil
}

// checkExitConditions 检查出场条件（止损 / +1R保本 / +3R平半 / EMA破位）
func (s *TrendFollowingStrategy) checkExitConditions(ctx context.Context, k *marketpb.Kline) error {
	m15 := s.latest15m

	// 计算持仓K线数
	heldBars := 0
	for _, snap := range s.klines15m {
		if snap.OpenTime >= s.pos.entryBarTime {
			heldBars++
		}
	}

	minHoldingBars := int(s.getParam(paramMinHoldingBars, 5))
	exitConfirmBars := int(s.getParam(paramEmaExitConfirmBars, 2))
	emaBufferATR := s.getParam(paramEmaExitBufferATR, 0.30)

	decision := s.evaluateTrendExitDecision(m15, heldBars, minHoldingBars, exitConfirmBars, emaBufferATR, "15m")
	if decision.SignalType == "" {
		log.Printf("[策略] %s 持仓中 | 方向=%v 价格=%.2f 止损=%.2f TP1=%.2f TP2=%.2f 持仓K线=%d",
			s.symbol, s.pos.side, m15.Close, s.pos.stopLoss, s.pos.takeProfit1, s.pos.takeProfit2, heldBars)
		return nil
	}

	return s.applyTrendExitDecision(ctx, k, m15, "15m", decision)
}

// calculatePnL 计算持仓盈亏
func (s *TrendFollowingStrategy) calculatePnL(currentPrice float64) float64 {
	if s.pos.side == sideLong {
		return currentPrice - s.pos.entryPrice
	} else if s.pos.side == sideShort {
		return s.pos.entryPrice - currentPrice
	}
	return 0
}

// ---------------------------------------------------------------------------
// 仓位计算
// ---------------------------------------------------------------------------

// calculatePositionSize 计算仓位大小
//
// 仓位计算步骤：
//  1. 风险仓位 = (权益 × 风险比例) ÷ (ATR × 止损倍数)
//  2. 现金限制 = (权益 × 最大仓位比例) ÷ 价格
//  3. 杠杆限制 = (权益 × 杠杆 × 最大杠杆使用率) ÷ 价格
//  4. 基础仓位 = min(风险仓位, 现金限制, 杠杆限制)
//  5. 最终仓位 = 基础仓位 × 回撤缩放
func (s *TrendFollowingStrategy) calculatePositionSize(price float64, stopLoss float64) float64 {
	equity := s.getParam(paramEquity, 10000)
	riskPct := s.getParam(paramRiskPerTrade, 0.03)
	maxPosSize := s.getParam(paramMaxPositionSize, 0.55)
	leverage := s.getParam(paramLeverage, 7)
	maxLevRatio := s.getParam(paramMaxLeverageRatio, 0.92)

	stopDist := math.Abs(price - stopLoss)
	if stopDist == 0 {
		return 0.01
	}

	// 1. 风险仓位
	riskPosition := (equity * riskPct) / stopDist

	// 2. 现金限制
	cashLimit := (equity * maxPosSize) / price

	// 3. 杠杆限制
	leverageLimit := (equity * leverage * maxLevRatio) / price

	// 4. 基础仓位
	basePosition := math.Min(math.Min(riskPosition, cashLimit), leverageLimit)

	// 5. 回撤缩放
	drawdownScale := 1.0
	drawdownPct := s.getParam(paramMaxDrawdownPct, 0.15)
	drawdownPosScale := s.getParam(paramDrawdownPosScale, 0.5)
	if s.peakEquity > 0 {
		drawdown := (s.peakEquity - equity) / s.peakEquity
		if drawdown >= drawdownPct {
			drawdownScale = drawdownPosScale
		}
	}

	finalPosition := basePosition * drawdownScale

	// 安全下限
	if finalPosition < 0.001 {
		finalPosition = 0.001
	}

	return finalPosition
}

// buildExitPolicyConfig 从当前策略参数中读取 1m 快速保护相关配置，供出场判断复用。
func (s *TrendFollowingStrategy) buildExitPolicyConfig() ExitPolicyConfig {
	return ExitPolicyConfig{
		FastProtectEnabled:            s.getParam(paramExitFastProtectEnabled, 1) > 0,
		FastProtectMaxTriggerCount:    int(s.getParam(paramExitFastProtectMaxTriggerCount, 1)),
		FastProtectReducePctEarly:     s.getParam(paramExitFastProtectReducePctEarly, 0.50),
		FastProtectReducePctProfit:    s.getParam(paramExitFastProtectReducePctProfit, 0.40),
		FastProtectReducePctTrend:     s.getParam(paramExitFastProtectReducePctTrend, 0.30),
		FastProtectBearBars1m:         int(s.getParam(paramExitFastProtectBearBars1m, 3)),
		FastProtectStructureLookback:  int(s.getParam(paramExitFastProtectStructureLookback, 3)),
		FastProtectVolumeSpikeMult1m:  s.getParam(paramExitFastProtectVolumeSpike1m, 1.50),
		FastProtectStructureATRBuf:    s.getParam(paramExitFastProtectStructureATRBuf, 0.10),
		FastProtectATR1mDropMult:      s.getParam(paramExitFastProtectATR1mDropMult, 1.10),
		FastProtectPullbackATR15m:     s.getParam(paramExitFastProtectPullbackATR15m, 0.80),
		EarlyProfitR:                  s.getParam(paramExitPositionEarlyProfitR, 0.80),
		TrendProfitR:                  s.getParam(paramExitPositionTrendProfitR, 2.00),
		Emergency1sEnabled:            s.getParam(paramExitEmergency1sEnabled, 1) > 0,
		Emergency1sWindowSec:          int(s.getParam(paramExitEmergency1sWindowSec, 5)),
		Emergency1sDropPct:            s.getParam(paramExitEmergency1sDropPct, 0.005),
		Emergency1sVolumeSpikeMult:    s.getParam(paramExitEmergency1sVolumeSpikeMult, 2.00),
		Emergency1sFullExitNoProfit:   s.getParam(paramExitEmergency1sFullExitNoProfit, 1) > 0,
		Emergency1sSyntheticDropMult:  s.getParam(paramExitEmergency1sSyntheticDropMult, 1.50),
		Emergency1sSyntheticReducePct: s.getParam(paramExitEmergency1sSyntheticReducePct, 0.35),
	}
}

// currentPositionStage 根据当前 R 倍数把持仓划分为 early/profit/trend_profit 三段。
func (s *TrendFollowingStrategy) currentPositionStage(currentR float64, cfg ExitPolicyConfig) PositionStage {
	if currentR >= cfg.TrendProfitR {
		return PositionStageTrendProfit
	}
	if currentR >= cfg.EarlyProfitR {
		return PositionStageProfit
	}
	return PositionStageEarly
}

// currentRMultiple 计算当前价格相对初始风险距离的 R 倍数，便于统一决定保护力度。
func (s *TrendFollowingStrategy) currentRMultiple(currentPrice float64) float64 {
	if s == nil || s.pos.side == sideNone {
		return 0
	}
	riskDistance := s.pos.exitState.InitialRiskDistance
	if riskDistance <= 0 {
		riskDistance = math.Abs(s.pos.entryPrice - s.pos.stopLoss)
	}
	if riskDistance <= 0 {
		return 0
	}
	return s.calculatePnL(currentPrice) / riskDistance
}

// hasConsecutiveWeak1mBars 判断最近 N 根 1m K 线是否连续收在 EMA21 下方并持续走弱。
func (s *TrendFollowingStrategy) hasConsecutiveWeak1mBars(requiredBars int) bool {
	if requiredBars <= 0 || len(s.klines1m) < requiredBars {
		return false
	}
	start := len(s.klines1m) - requiredBars
	for i := start; i < len(s.klines1m); i++ {
		snap := s.klines1m[i]
		if snap.Close == 0 || snap.Ema21 == 0 || snap.Close >= snap.Ema21 {
			return false
		}
		if i > start && snap.Low >= s.klines1m[i-1].Low {
			return false
		}
	}
	return true
}

// averageVolumeOfRecent1m 计算最近 N 根 1m K 线的平均成交量，供放量急跌判断使用。
func (s *TrendFollowingStrategy) averageVolumeOfRecent1m(lookback int) float64 {
	if lookback <= 0 || len(s.klines1m) == 0 {
		return 0
	}
	if lookback > len(s.klines1m) {
		lookback = len(s.klines1m)
	}
	start := len(s.klines1m) - lookback
	total := 0.0
	for i := start; i < len(s.klines1m); i++ {
		total += s.klines1m[i].Volume
	}
	return total / float64(lookback)
}

// averageVolumeOfRecentSecondBars 计算最近若干秒级条目的平均成交量，可选择忽略最新一条避免被尖峰自我稀释。
func (s *TrendFollowingStrategy) averageVolumeOfRecentSecondBars(lookback int, excludeLatest bool) float64 {
	if lookback <= 0 || len(s.secondBars) == 0 {
		return 0
	}
	end := len(s.secondBars)
	if excludeLatest {
		end--
	}
	if end <= 0 {
		return 0
	}
	if lookback > end {
		lookback = end
	}
	start := end - lookback
	total := 0.0
	for i := start; i < end; i++ {
		total += s.secondBars[i].Volume
	}
	return total / float64(lookback)
}

// emergencyWindowMovePct 计算秒级窗口内相对极值到当前收盘的急跌/急涨比例。
func (s *TrendFollowingStrategy) emergencyWindowMovePct(windowSec int) (float64, bool) {
	if s == nil || s.pos.side == sideNone || len(s.secondBars) == 0 {
		return 0, false
	}
	if windowSec <= 1 {
		windowSec = 5
	}
	if windowSec > len(s.secondBars) {
		windowSec = len(s.secondBars)
	}
	start := len(s.secondBars) - windowSec
	latest := s.secondBars[len(s.secondBars)-1]
	if s.pos.side == sideLong {
		refHigh := s.secondBars[start].High
		for i := start + 1; i < len(s.secondBars); i++ {
			if s.secondBars[i].High > refHigh {
				refHigh = s.secondBars[i].High
			}
		}
		if refHigh <= 0 {
			return 0, false
		}
		return (refHigh - latest.Close) / refHigh, true
	}
	refLow := s.secondBars[start].Low
	for i := start + 1; i < len(s.secondBars); i++ {
		if s.secondBars[i].Low < refLow {
			refLow = s.secondBars[i].Low
		}
	}
	if refLow <= 0 {
		return 0, false
	}
	return (latest.Close - refLow) / refLow, true
}

// fastProtectReducePct 根据持仓阶段返回建议的快速保护减仓比例。
func (s *TrendFollowingStrategy) fastProtectReducePct(stage PositionStage, cfg ExitPolicyConfig) float64 {
	switch stage {
	case PositionStageTrendProfit:
		return cfg.FastProtectReducePctTrend
	case PositionStageProfit:
		return cfg.FastProtectReducePctProfit
	default:
		return cfg.FastProtectReducePctEarly
	}
}

// recent1mStructureStop 返回最近 1m 微结构止损价，供快速保护后进一步锁盈使用。
func (s *TrendFollowingStrategy) recent1mStructureStop(cfg ExitPolicyConfig) (float64, bool) {
	if s == nil || len(s.klines1m) == 0 {
		return 0, false
	}
	lookback := cfg.FastProtectStructureLookback
	if lookback <= 0 {
		lookback = 3
	}
	if lookback > len(s.klines1m) {
		lookback = len(s.klines1m)
	}
	start := len(s.klines1m) - lookback
	atrBuf := cfg.FastProtectStructureATRBuf * s.latest1m.Atr
	if s.pos.side == sideLong {
		structureLow := s.klines1m[start].Low
		for i := start + 1; i < len(s.klines1m); i++ {
			if s.klines1m[i].Low < structureLow {
				structureLow = s.klines1m[i].Low
			}
		}
		return structureLow - atrBuf, true
	}
	structureHigh := s.klines1m[start].High
	for i := start + 1; i < len(s.klines1m); i++ {
		if s.klines1m[i].High > structureHigh {
			structureHigh = s.klines1m[i].High
		}
	}
	return structureHigh + atrBuf, true
}

// fastProtectRaisedStop 决定快速保护减仓后把止损抬到保本，还是抬到最近 1m 微结构位置。
func (s *TrendFollowingStrategy) fastProtectRaisedStop(stage PositionStage, cfg ExitPolicyConfig) (float64, string, bool) {
	if s == nil || s.pos.side == sideNone {
		return 0, "", false
	}
	breakeven := s.pos.entryPrice
	if stage != PositionStageTrendProfit {
		return breakeven, "break_even", true
	}
	structureStop, ok := s.recent1mStructureStop(cfg)
	if !ok {
		return breakeven, "break_even", true
	}
	if s.pos.side == sideLong {
		if structureStop > breakeven {
			return structureStop, "micro_structure_1m", true
		}
		return breakeven, "break_even", true
	}
	if structureStop < breakeven {
		return structureStop, "micro_structure_1m", true
	}
	return breakeven, "break_even", true
}

// initializeExitPolicyState 在开仓时初始化分层出场参数和运行态，避免首次保护判断缺少基线。
func (s *TrendFollowingStrategy) initializeExitPolicyState(entryPrice, stopLoss float64) (ExitPolicyConfig, ExitRuntimeState) {
	cfg := s.buildExitPolicyConfig()
	state := ExitRuntimeState{
		MaxFavorablePrice:   entryPrice,
		InitialRiskDistance: math.Abs(entryPrice - stopLoss),
		Stage:               PositionStageEarly,
	}
	return cfg, state
}

// evaluateFastProtectOn1m 评估 1m 快速保护是否应先减仓。
// 当前返回的是可直接执行的 PARTIAL_CLOSE 动作，同时携带止损抬升目标。
func (s *TrendFollowingStrategy) evaluateFastProtectOn1m() *ExitAction {
	if s == nil || s.pos.side == sideNone {
		return nil
	}

	cfg := s.buildExitPolicyConfig()
	if !cfg.FastProtectEnabled {
		return nil
	}
	if cfg.FastProtectMaxTriggerCount > 0 && s.pos.exitState.FastProtectTriggerCount >= cfg.FastProtectMaxTriggerCount {
		return nil
	}

	m1 := s.latest1m
	if m1.Close == 0 || m1.Atr == 0 {
		return nil
	}

	currentR := s.currentRMultiple(m1.Close)
	stage := s.currentPositionStage(currentR, cfg)
	consecutiveWeak := s.hasConsecutiveWeak1mBars(cfg.FastProtectBearBars1m)
	avgVolume20 := s.averageVolumeOfRecent1m(20)
	largeBearBar := false
	if s.pos.side == sideLong {
		largeBearBar = (m1.Open-m1.Close) >= cfg.FastProtectATR1mDropMult*m1.Atr &&
			avgVolume20 > 0 && m1.Volume >= avgVolume20*cfg.FastProtectVolumeSpikeMult1m
	} else if s.pos.side == sideShort {
		largeBearBar = (m1.Close-m1.Open) >= cfg.FastProtectATR1mDropMult*m1.Atr &&
			avgVolume20 > 0 && m1.Volume >= avgVolume20*cfg.FastProtectVolumeSpikeMult1m
	}

	if !consecutiveWeak && !largeBearBar {
		return nil
	}

	reducePct := s.fastProtectReducePct(stage, cfg)
	if reducePct <= 0 {
		return nil
	}

	action := "SELL"
	if s.pos.side == sideShort {
		action = "BUY"
	}
	raiseStopTo, stopRaiseMode, _ := s.fastProtectRaisedStop(stage, cfg)
	return &ExitAction{
		Code:          ExitEventFastProtect1m,
		Interval:      "1m",
		Action:        action,
		SignalType:    "PARTIAL_CLOSE",
		ReducePct:     reducePct,
		RaiseStopTo:   raiseStopTo,
		StopRaiseMode: stopRaiseMode,
		Reason:        "1分钟快速保护触发，建议先减仓防止亏损扩大",
		ReasonCode:    "exit_fast_protect_1m",
		ReasonLabel:   "1分钟快速保护减仓",
		TriggerPrice:  m1.Close,
		CurrentR:      currentR,
		PositionStage: stage,
	}
}

// evaluateEmergencyExitOn1s 评估 1s 极端保护，只处理异常瀑布或急速反向波动。
func (s *TrendFollowingStrategy) evaluateEmergencyExitOn1s() *ExitAction {
	if s == nil || s.pos.side == sideNone {
		return nil
	}

	cfg := s.buildExitPolicyConfig()
	if !cfg.Emergency1sEnabled || s.pos.exitState.EmergencyTriggered || len(s.secondBars) < 2 {
		return nil
	}

	latest := s.secondBars[len(s.secondBars)-1]
	syntheticBar := latest.Synthetic
	currentR := s.currentRMultiple(latest.Close)
	stage := s.currentPositionStage(currentR, cfg)
	movePct, ok := s.emergencyWindowMovePct(cfg.Emergency1sWindowSec)
	if !ok {
		return nil
	}
	avgVolume := s.averageVolumeOfRecentSecondBars(cfg.Emergency1sWindowSec, true)
	volumeSpike := avgVolume > 0 && latest.Volume >= avgVolume*cfg.Emergency1sVolumeSpikeMult
	triggerDropPct := cfg.Emergency1sDropPct
	if syntheticBar && cfg.Emergency1sSyntheticDropMult > 1 {
		triggerDropPct *= cfg.Emergency1sSyntheticDropMult
	}

	action := "SELL"
	breakStop := latest.Close <= s.pos.stopLoss
	if s.pos.side == sideShort {
		action = "BUY"
		breakStop = latest.Close >= s.pos.stopLoss
	}
	moveTriggered := movePct >= triggerDropPct
	if syntheticBar {
		if !(breakStop && moveTriggered) && !moveTriggered {
			return nil
		}
	} else if !breakStop && !(moveTriggered && volumeSpike) {
		return nil
	}

	fullExit := breakStop || (cfg.Emergency1sFullExitNoProfit && currentR <= 0)
	if syntheticBar {
		fullExit = false
	}
	signalType := "PARTIAL_CLOSE"
	reducePct := 0.5
	raiseStopTo, stopRaiseMode, _ := s.fastProtectRaisedStop(stage, cfg)
	reason := "1秒极端保护触发，检测到异常急跌或急速反向波动"
	reasonLabel := "1秒极端保护止损"
	if syntheticBar {
		reducePct = cfg.Emergency1sSyntheticReducePct
		if reducePct <= 0 {
			reducePct = 0.35
		}
		reason = "1秒极端保护触发，检测到 synthetic 秒级急跌，先做保护性减仓确认"
		reasonLabel = "1秒极端保护减仓（盘口兜底）"
	}
	if fullExit {
		signalType = "CLOSE"
		reducePct = 1.0
		raiseStopTo = 0
		stopRaiseMode = ""
	}

	return &ExitAction{
		Code:          ExitEventEmergency1s,
		Interval:      "1s",
		Action:        action,
		SignalType:    signalType,
		ReducePct:     reducePct,
		RaiseStopTo:   raiseStopTo,
		StopRaiseMode: stopRaiseMode,
		Reason:        reason,
		ReasonCode:    "exit_emergency_stop_1s",
		ReasonLabel:   reasonLabel,
		TriggerPrice:  latest.Close,
		CurrentR:      currentR,
		PositionStage: stage,
	}
}

// applyFastProtectOn1m 发送真实的 1m 快速保护减仓信号，并同步更新本地持仓状态。
func (s *TrendFollowingStrategy) applyFastProtectOn1m(ctx context.Context, k *marketpb.Kline, action *ExitAction) error {
	if s == nil || action == nil || s.pos.side == sideNone {
		return nil
	}
	if action.SignalType == "" {
		action.SignalType = "PARTIAL_CLOSE"
	}
	if action.ReducePct <= 0 || s.pos.quantity <= 0 {
		return nil
	}

	quantity := s.pos.quantity * action.ReducePct
	if quantity <= 0 {
		return nil
	}
	if quantity > s.pos.quantity {
		quantity = s.pos.quantity
	}

	side := "LONG"
	if s.pos.side == sideShort {
		side = "SHORT"
	}
	interval := action.Interval
	if interval == "" {
		interval = "1m"
	}
	signalReason := s.newExitSignalReason(
		action.Reason,
		fmt.Sprintf("持仓方向=%s | 周期=%s | 动作=%s", side, interval, action.SignalType),
		action.Reason,
		fmt.Sprintf("触发价=%.2f | 当前R=%.2f | 减仓比例=%.2f | 减仓数量=%.4f", action.TriggerPrice, action.CurrentR, action.ReducePct, quantity),
		"risk_reduce",
		action.ReasonLabel,
		interval, s.strategySignalTag(), "close", side,
	)
	signal := map[string]interface{}{
		"strategy_id":   s.strategySignalID(interval),
		"symbol":        s.symbol,
		"interval":      interval,
		"action":        action.Action,
		"side":          side,
		"signal_type":   action.SignalType,
		"quantity":      quantity,
		"entry_price":   action.TriggerPrice,
		"stop_loss":     action.RaiseStopTo,
		"take_profits":  []float64{s.pos.takeProfit1, s.pos.takeProfit2},
		"reason":        signalReason.Summary,
		"signal_reason": signalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           s.latest1m.Atr,
		"risk_reward":   0,
		"indicators": map[string]interface{}{
			"m1_ema21":        s.latest1m.Ema21,
			"m1_rsi":          s.latest1m.Rsi,
			"m1_atr":          s.latest1m.Atr,
			"m15_atr":         s.latest15m.Atr,
			"current_r":       action.CurrentR,
			"raise_stop_to":   action.RaiseStopTo,
			"stop_raise_mode": action.StopRaiseMode,
		},
	}

	if err := s.sendSignal(ctx, signal, k); err != nil {
		return err
	}

	s.pos.quantity = math.Max(0, s.pos.quantity-quantity)
	s.pos.partialClosed = true
	s.pos.exitState.CurrentR = action.CurrentR
	s.pos.exitState.Stage = action.PositionStage
	s.pos.exitState.FastProtectTriggered = true
	s.pos.exitState.FastProtectTriggerCount++
	s.pos.exitState.LastReduceReason = action.Code
	s.pos.exitState.LastReduceTime = time.Now().UnixMilli()
	if action.RaiseStopTo > 0 {
		if s.pos.side == sideLong {
			s.pos.stopLoss = math.Max(s.pos.stopLoss, action.RaiseStopTo)
		} else {
			s.pos.stopLoss = math.Min(s.pos.stopLoss, action.RaiseStopTo)
		}
	}
	return nil
}

// applyEmergencyExitOn1s 执行 1s 极端保护动作，未盈利时优先直接全平，盈利阶段可先做保护性减仓。
func (s *TrendFollowingStrategy) applyEmergencyExitOn1s(ctx context.Context, k *marketpb.Kline, action *ExitAction) error {
	if s == nil || action == nil || s.pos.side == sideNone {
		return nil
	}

	side := "LONG"
	if s.pos.side == sideShort {
		side = "SHORT"
	}
	quantity := s.pos.quantity
	if action.SignalType == "PARTIAL_CLOSE" {
		quantity = s.pos.quantity * action.ReducePct
		if quantity <= 0 {
			return nil
		}
		if quantity > s.pos.quantity {
			quantity = s.pos.quantity
		}
	}

	signalReason := s.newExitSignalReason(
		action.Reason,
		fmt.Sprintf("持仓方向=%s | 周期=%s | 动作=%s", side, action.Interval, action.SignalType),
		action.Reason,
		fmt.Sprintf("触发价=%.2f | 当前R=%.2f | 数量=%.4f", action.TriggerPrice, action.CurrentR, quantity),
		"emergency_stop",
		action.ReasonLabel,
		action.Interval, s.strategySignalTag(), "close", side,
	)
	signal := map[string]interface{}{
		"strategy_id":   s.strategySignalID(action.Interval),
		"symbol":        s.symbol,
		"interval":      action.Interval,
		"action":        action.Action,
		"side":          side,
		"signal_type":   action.SignalType,
		"quantity":      quantity,
		"entry_price":   action.TriggerPrice,
		"stop_loss":     action.RaiseStopTo,
		"take_profits":  []float64{s.pos.takeProfit1, s.pos.takeProfit2},
		"reason":        signalReason.Summary,
		"signal_reason": signalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           s.latest1m.Atr,
		"risk_reward":   0,
		"indicators": map[string]interface{}{
			"current_r":       action.CurrentR,
			"raise_stop_to":   action.RaiseStopTo,
			"stop_raise_mode": action.StopRaiseMode,
		},
	}
	if err := s.sendSignal(ctx, signal, k); err != nil {
		return err
	}

	s.pos.exitState.CurrentR = action.CurrentR
	s.pos.exitState.Stage = action.PositionStage
	s.pos.exitState.EmergencyTriggered = true
	s.pos.exitState.LastReduceReason = action.Code
	s.pos.exitState.LastReduceTime = time.Now().UnixMilli()
	if action.SignalType == "CLOSE" {
		s.updateRiskState(s.calculatePnL(action.TriggerPrice))
		s.pos = position{}
		return nil
	}
	s.pos.quantity = math.Max(0, s.pos.quantity-quantity)
	s.pos.partialClosed = true
	if action.RaiseStopTo > 0 {
		if s.pos.side == sideLong {
			s.pos.stopLoss = math.Max(s.pos.stopLoss, action.RaiseStopTo)
		} else {
			s.pos.stopLoss = math.Min(s.pos.stopLoss, action.RaiseStopTo)
		}
	}
	return nil
}

// applyWeightRecommendation 把 Phase 5 的权重建议真正应用到开仓数量上。
func (s *TrendFollowingStrategy) applyWeightRecommendation(quantity float64) (float64, bool, string) {
	if quantity <= 0 {
		return 0, true, "base_quantity_invalid"
	}
	if s == nil || s.weightProvider == nil {
		return quantity, false, ""
	}
	rec, ok := s.weightProvider(s.symbol)
	if !ok {
		return quantity, false, ""
	}
	return applyWeightScale(quantity, rec)
}

// latestWeightRecommendation 读取当前 symbol 最近一轮权重快照，统一供日志和下单链路复用。
func (s *TrendFollowingStrategy) latestWeightRecommendation() (weights.Recommendation, bool) {
	if s == nil || s.weightProvider == nil {
		return weights.Recommendation{}, false
	}
	return s.weightProvider(s.symbol)
}

// enrichDecisionExtrasWithRoute 把统一路由视角和 allocator 预算快照写入 decision extras，避免排查时再跨日志回溯。
func (s *TrendFollowingStrategy) enrichDecisionExtrasWithRoute(extras map[string]interface{}) map[string]interface{} {
	if s == nil {
		return extras
	}
	var out map[string]interface{}
	if len(extras) > 0 {
		out = make(map[string]interface{}, len(extras)+2)
		for k, v := range extras {
			out[k] = v
		}
	}
	rec, ok := s.latestWeightRecommendation()
	if !ok {
		return out
	}
	if out == nil {
		out = make(map[string]interface{}, 2)
	}
	if rec.Bucket != "" {
		out["route_bucket"] = rec.Bucket
	}
	if rec.RouteReason != "" {
		out["route_reason"] = rec.RouteReason
	}
	if rec.Template != "" {
		out["route_template"] = rec.Template
	}
	out["risk_scale"] = rec.RiskScale
	out["position_budget"] = rec.PositionBudget
	return out
}

// applyWeightScale 根据最新一轮权重建议缩放开仓数量。
func applyWeightScale(quantity float64, rec weights.Recommendation) (float64, bool, string) {
	if quantity <= 0 {
		return 0, true, "base_quantity_invalid"
	}
	if rec.TradingPaused {
		if rec.PauseReason != "" {
			return 0, true, rec.PauseReason
		}
		return 0, true, "weight_trading_paused"
	}
	scale := rec.PositionBudget
	if scale <= 0 {
		return 0, true, "weight_budget_zero"
	}
	if scale > 1 {
		scale = 1
	}
	adjusted := quantity * scale
	if adjusted < 0.001 {
		return 0, true, "weight_budget_too_small"
	}
	return adjusted, false, ""
}

// ---------------------------------------------------------------------------
// 风险管理
// ---------------------------------------------------------------------------

// checkRiskLimits 检查风控限制（是否允许开仓）
func (s *TrendFollowingStrategy) checkRiskLimits() bool {
	// 已有持仓，不允许再开
	if s.pos.side != sideNone {
		return false
	}

	// 最大持仓限制
	maxPos := int(s.getParam(paramMaxPositions, 1))
	if maxPos <= 0 {
		maxPos = 1
	}
	// 当前只有一个持仓（s.pos），所以始终满足

	// 连续亏损限制
	maxLosses := int(s.getParam(paramMaxConsecutiveLosses, 3))
	if s.consecutiveLosses >= maxLosses {
		log.Printf("[策略] %s 连续亏损%d次 ≥ 上限%d，暂停交易", s.symbol, s.consecutiveLosses, maxLosses)
		return false
	}

	// 日亏损限制
	today := time.Now().UTC().Format("2006-01-02")
	if s.dailyPnlDate != today {
		// 新的一天，重置日亏损
		s.dailyPnl = 0
		s.dailyPnlDate = today
	}
	equity := s.getParam(paramEquity, 10000)
	maxDailyLoss := s.getParam(paramMaxDailyLossPct, 0.07)
	if equity > 0 && s.dailyPnl < 0 {
		dailyLossPct := -s.dailyPnl / equity
		if dailyLossPct >= maxDailyLoss {
			log.Printf("[策略] %s 日亏损%.2f%% ≥ 上限%.0f%%，暂停交易",
				s.symbol, dailyLossPct*100, maxDailyLoss*100)
			return false
		}
	}

	return true
}

// updateRiskState 更新风控状态（出场后调用）
func (s *TrendFollowingStrategy) updateRiskState(pnl float64) {
	if pnl > 0 {
		s.consecutiveLosses = 0
	} else if pnl < 0 {
		s.consecutiveLosses++
	}

	// 累加日亏损
	today := time.Now().UTC().Format("2006-01-02")
	if s.dailyPnlDate != today {
		s.dailyPnl = 0
		s.dailyPnlDate = today
	}
	s.dailyPnl += pnl

	// 更新峰值权益（用于回撤计算）
	equity := s.getParam(paramEquity, 10000)
	currentEquity := equity + s.dailyPnl
	if currentEquity > s.peakEquity {
		s.peakEquity = currentEquity
	}
}

// ---------------------------------------------------------------------------
// 信号发送与日志
// ---------------------------------------------------------------------------

// fmtFloatOrNA 将 interface{} 格式化为浮点数字符串，nil 或零值时显示 "N/A"
func fmtFloatOrNA(v interface{}) string {
	if v == nil {
		return "N/A"
	}
	f, ok := v.(float64)
	if !ok {
		return "N/A"
	}
	return fmt.Sprintf("%.2f", f)
}

// sendSignal 发送交易信号到 Kafka signal topic，并写入本地日志
func (s *TrendFollowingStrategy) sendSignal(ctx context.Context, signal map[string]interface{}, k *marketpb.Kline) error {
	action, _ := signal["action"].(string)
	side, _ := signal["side"].(string)
	entryPrice, _ := signal["entry_price"].(float64)
	stopLoss, _ := signal["stop_loss"].(float64)
	quantity, _ := signal["quantity"].(float64)
	reason, _ := signal["reason"].(string)
	interval, _ := signal["interval"].(string)
	indicators, _ := signal["indicators"].(map[string]interface{})
	if interval == "" {
		interval = "15m"
	}

	tpStr := "[]"
	if tp, ok := signal["take_profits"].([]float64); ok && len(tp) >= 2 {
		tpStr = fmt.Sprintf("[%.2f, %.2f]", tp[0], tp[1])
	}

	indicatorsStr := ""
	if indicators != nil {
		indicatorsStr = fmt.Sprintf("4H:EMA21=%s EMA55=%s | 1H:EMA21=%s RSI=%s | 15M:RSI=%s ATR=%s",
			fmtFloatOrNA(indicators["h4_ema21"]), fmtFloatOrNA(indicators["h4_ema55"]),
			fmtFloatOrNA(indicators["h1_ema21"]), fmtFloatOrNA(indicators["h1_rsi"]),
			fmtFloatOrNA(indicators["m15_rsi"]), fmtFloatOrNA(indicators["m15_atr"]))
	}
	weightsStr := ""
	if rec, ok := s.latestWeightRecommendation(); ok {
		weightsStr = fmt.Sprintf(
			" | weights(template=%s bucket=%s route_reason=%s budget=%.4f bucket_budget=%.4f risk=%.4f strategy=%.4f symbol=%.4f score=%.4f source=%s paused=%v",
			rec.Template,
			rec.Bucket,
			rec.RouteReason,
			rec.PositionBudget,
			rec.BucketBudget,
			rec.RiskScale,
			rec.StrategyWeight,
			rec.SymbolWeight,
			rec.Score,
			rec.ScoreSource,
			rec.TradingPaused,
		)
		if rec.PauseReason != "" {
			weightsStr += fmt.Sprintf(" reason=%s", rec.PauseReason)
		}
		weightsStr += ")"
	}

	log.Printf("[策略信号] %s %s %s | 方向=%s | 价格=%.2f | 数量=%.4f | 止损=%.2f | 止盈=%s | %s | %s%s",
		s.symbol, interval, action, side, entryPrice, quantity, stopLoss, tpStr, indicatorsStr, reason, weightsStr)

	signal = s.enrichSignalWithRouteContext(signal)
	s.writeSignalLog(signal, k)

	return s.producer.SendMarketData(ctx, signal)
}

// signalLogEntry 定义结构化的信号日志格式
type signalLogEntry struct {
	Timestamp    string                   `json:"timestamp"`
	Symbol       string                   `json:"symbol"`
	Interval     string                   `json:"interval"`
	OpenTime     string                   `json:"open_time"`
	CloseTime    string                   `json:"close_time"`
	OpenTimeMs   int64                    `json:"open_time_ms"`
	CloseTimeMs  int64                    `json:"close_time_ms"`
	StrategyID   string                   `json:"strategyId"`
	Action       string                   `json:"action"`
	Side         string                   `json:"side"`
	EntryPrice   float64                  `json:"entryPrice"`
	Quantity     float64                  `json:"quantity"`
	StopLoss     float64                  `json:"stopLoss"`
	TakeProfits  []float64                `json:"takeProfits"`
	Reason       string                   `json:"reason"`
	SignalReason interface{}              `json:"signalReason,omitempty"`
	IsTradable   bool                     `json:"isTradable"`
	IsFinal      bool                     `json:"isFinal"`
	Indicators   *orderedSignalIndicators `json:"indicators,omitempty"`
	Weights      *signalWeightSnapshot    `json:"weights,omitempty"`
}

type signalWeightSnapshot struct {
	Template       string  `json:"template,omitempty"`
	RouteBucket    string  `json:"route_bucket,omitempty"`
	RouteReason    string  `json:"route_reason,omitempty"`
	Score          float64 `json:"score,omitempty"`
	ScoreSource    string  `json:"score_source,omitempty"`
	BucketBudget   float64 `json:"bucket_budget,omitempty"`
	StrategyWeight float64 `json:"strategy_weight"`
	SymbolWeight   float64 `json:"symbol_weight"`
	RiskScale      float64 `json:"risk_scale"`
	PositionBudget float64 `json:"position_budget"`
	TradingPaused  bool    `json:"trading_paused"`
	PauseReason    string  `json:"pause_reason,omitempty"`
}

type signalReasonAllocatorPayload struct {
	Template       string  `json:"template,omitempty"`
	RouteBucket    string  `json:"route_bucket,omitempty"`
	RouteReason    string  `json:"route_reason,omitempty"`
	Score          float64 `json:"score,omitempty"`
	ScoreSource    string  `json:"score_source,omitempty"`
	BucketBudget   float64 `json:"bucket_budget,omitempty"`
	StrategyWeight float64 `json:"strategy_weight"`
	SymbolWeight   float64 `json:"symbol_weight"`
	RiskScale      float64 `json:"risk_scale"`
	PositionBudget float64 `json:"position_budget"`
	TradingPaused  bool    `json:"trading_paused"`
	PauseReason    string  `json:"pause_reason,omitempty"`
}

type orderedSignalIndicators struct {
	values map[string]interface{}
}

func newOrderedSignalIndicators(indicators map[string]interface{}) *orderedSignalIndicators {
	if len(indicators) == 0 {
		return nil
	}
	return &orderedSignalIndicators{values: indicators}
}

func (o *orderedSignalIndicators) MarshalJSON() ([]byte, error) {
	if o == nil || len(o.values) == 0 {
		return []byte("null"), nil
	}
	priority := []string{
		"h4_ema21", "h4_ema55", "h4_adx",
		"h1_ema21", "h1_ema55", "h1_rsi", "h1_macd_hist",
		"entry_mode", "position_scale",
		"m15_ema21", "m15_rsi", "m15_atr",
		"m1_ema21", "m1_rsi", "m1_atr",
	}
	seen := make(map[string]struct{}, len(o.values))
	keys := make([]string, 0, len(o.values))
	for _, key := range priority {
		if _, ok := o.values[key]; ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}
	rest := make([]string, 0, len(o.values)-len(keys))
	for key := range o.values {
		if _, ok := seen[key]; !ok {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	keys = append(keys, rest...)

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, err := marshalJSONNoHTMLEscape(key)
		if err != nil {
			return nil, err
		}
		valJSON, err := marshalJSONNoHTMLEscape(o.values[key])
		if err != nil {
			return nil, err
		}
		buf.Write(keyJSON)
		buf.WriteByte(':')
		buf.Write(valJSON)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

type signalReasonPayload struct {
	Summary          string                        `json:"summary"`
	Phase            string                        `json:"phase"`
	TrendContext     string                        `json:"trend_context,omitempty"`
	SetupContext     string                        `json:"setup_context,omitempty"`
	PathContext      string                        `json:"path_context,omitempty"`
	ExecutionContext string                        `json:"execution_context,omitempty"`
	ExitReasonKind   string                        `json:"exit_reason_kind,omitempty"`
	ExitReasonLabel  string                        `json:"exit_reason_label,omitempty"`
	Tags             []string                      `json:"tags,omitempty"`
	RouteBucket      string                        `json:"route_bucket,omitempty"`
	RouteReason      string                        `json:"route_reason,omitempty"`
	RouteTemplate    string                        `json:"route_template,omitempty"`
	Allocator        *signalReasonAllocatorPayload `json:"allocator,omitempty"`
	Range            *signalReasonRangePayload     `json:"range,omitempty"`
}

type signalReasonRangePayload struct {
	H1RangeOK      bool `json:"h1_range_ok"`
	H1AdxOK        bool `json:"h1_adx_ok"`
	H1BollWidthOK  bool `json:"h1_boll_width_ok"`
	M15TouchLower  bool `json:"m15_touch_lower"`
	M15RsiTurnUp   bool `json:"m15_rsi_turn_up"`
	M15TouchUpper  bool `json:"m15_touch_upper"`
	M15RsiTurnDown bool `json:"m15_rsi_turn_down"`
}

// signalReasonAllocatorPayloadFromRecommendation 生成随 signal_reason 一起下发的 allocator 快照。
func signalReasonAllocatorPayloadFromRecommendation(rec weights.Recommendation) *signalReasonAllocatorPayload {
	return &signalReasonAllocatorPayload{
		Template:       rec.Template,
		RouteBucket:    rec.Bucket,
		RouteReason:    rec.RouteReason,
		Score:          rec.Score,
		ScoreSource:    rec.ScoreSource,
		BucketBudget:   rec.BucketBudget,
		StrategyWeight: rec.StrategyWeight,
		SymbolWeight:   rec.SymbolWeight,
		RiskScale:      rec.RiskScale,
		PositionBudget: rec.PositionBudget,
		TradingPaused:  rec.TradingPaused,
		PauseReason:    rec.PauseReason,
	}
}

func (s *TrendFollowingStrategy) newSignalReason(summary, phase, trendContext, setupContext, executionContext string, tags ...string) signalReasonPayload {
	payload := signalReasonPayload{
		Summary:          summary,
		Phase:            phase,
		TrendContext:     trendContext,
		SetupContext:     setupContext,
		PathContext:      s.harvestPathReasonContext(),
		ExecutionContext: executionContext,
		Tags:             tags,
	}
	if rec, ok := s.latestWeightRecommendation(); ok {
		payload.RouteBucket = rec.Bucket
		payload.RouteReason = rec.RouteReason
		payload.RouteTemplate = rec.Template
		payload.Allocator = signalReasonAllocatorPayloadFromRecommendation(rec)
	}
	return payload
}

// withExitReason 在 signal_reason 中补充稳定的出场原因分类，供下游直接展示。
func (p signalReasonPayload) withExitReason(kind, label string) signalReasonPayload {
	p.ExitReasonKind = strings.TrimSpace(kind)
	p.ExitReasonLabel = strings.TrimSpace(label)
	return p
}

// newExitSignalReason 统一构造带结构化出场原因的 signal_reason，避免各策略分支重复拼装。
func (s *TrendFollowingStrategy) newExitSignalReason(summary, trendContext, setupContext, executionContext, exitReasonKind, exitReasonLabel string, tags ...string) signalReasonPayload {
	return s.newSignalReason(summary, "CLOSE_EXIT", trendContext, setupContext, executionContext, tags...).withExitReason(exitReasonKind, exitReasonLabel)
}

// currentStopExitReason 基于当前止损位与入场价的关系，区分普通止损和保本止损。
func (s *TrendFollowingStrategy) currentStopExitReason() (string, string) {
	if isBreakevenPrice(s.pos.stopLoss, s.pos.entryPrice) {
		return "break_even_stop", "保本止损"
	}
	return "stop_loss", "止损"
}

// isBreakevenPrice 判断止损位是否已经抬到接近入场价，用于稳定识别保本止损。
func isBreakevenPrice(stopLoss, entryPrice float64) bool {
	tolerance := math.Max(1e-8, math.Abs(entryPrice)*1e-6)
	return math.Abs(stopLoss-entryPrice) <= tolerance
}

// enrichSignalWithRouteContext 在实际发送前把统一路由视角补进 signal 顶层，方便 Kafka 下游直接消费。
func (s *TrendFollowingStrategy) enrichSignalWithRouteContext(signal map[string]interface{}) map[string]interface{} {
	if len(signal) == 0 {
		return signal
	}
	rec, ok := s.latestWeightRecommendation()
	if !ok {
		return signal
	}
	if rec.Bucket != "" {
		signal["route_bucket"] = rec.Bucket
	}
	if rec.RouteReason != "" {
		signal["route_reason"] = rec.RouteReason
	}
	if rec.Template != "" {
		signal["route_template"] = rec.Template
	}
	return signal
}

func (s *TrendFollowingStrategy) harvestPathReasonContext() string {
	if s.getParam(paramHarvestPathModelEnabled, 0) > 0 {
		return "harvest_path_guard=enabled"
	}
	return "harvest_path_guard=disabled"
}

func describeTrendContext(trend trendDirection) string {
	switch trend {
	case trendLong:
		return "多头（价格>EMA21>EMA55）"
	case trendShort:
		return "空头（价格<EMA21<EMA55）"
	default:
		return "无"
	}
}

// describeEntrySetupContext 把 1H 入场形态翻译成稳定中文描述，供日志与 signal_reason 复用。
func describeEntrySetupContext(mode entryMode, pullback pullbackResult) string {
	switch mode {
	case entryModePullback:
		switch pullback {
		case pullbackLong:
			return "模式A标准回调做多（价格在EMA带内）"
		case pullbackShort:
			return "模式A标准回调做空（价格在EMA带内）"
		}
	case entryModeCrossover:
		switch pullback {
		case pullbackLong:
			return "模式C趋势交叉做多（EMA上穿+ADX+MACD确认）"
		case pullbackShort:
			return "模式C趋势交叉做空（EMA下穿+ADX+MACD确认）"
		}
	}
	switch pullback {
	case pullbackLong:
		return "多头入场形态"
	case pullbackShort:
		return "空头入场形态"
	default:
		return "无"
	}
}

// stringEntryMode 输出稳定英文码，供 decision/signal 结构化字段复用。
func stringEntryMode(mode entryMode) string {
	return strings.ToUpper(strings.TrimSpace(string(mode)))
}

// writeSignalLog 追加信号到每日 JSONL 日志文件
// 格式：data/signal/ETHUSDT/2026-04-11.jsonl
func (s *TrendFollowingStrategy) writeSignalLog(signal map[string]interface{}, k *marketpb.Kline) {
	if s.signalLogDir == "" {
		return
	}

	now := time.Now().UTC()
	dateStr := now.Format("2006-01-02")
	dir := filepath.Join(s.signalLogDir, s.symbol)

	s.signalLogMu.Lock()
	defer s.signalLogMu.Unlock()

	key := s.symbol + "/" + dateStr
	f, ok := s.signalLogFiles[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[signal-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[signal-log] failed to open %s: %v", path, err)
			return
		}
		s.signalLogFiles[key] = f
	}

	action, _ := signal["action"].(string)
	side, _ := signal["side"].(string)
	entryPrice, _ := signal["entry_price"].(float64)
	quantity, _ := signal["quantity"].(float64)
	stopLoss, _ := signal["stop_loss"].(float64)
	reason, _ := signal["reason"].(string)
	signalReason := signal["signal_reason"]
	interval, _ := signal["interval"].(string)
	signalType, _ := signal["signal_type"].(string)
	atr, _ := signal["atr"].(float64)
	riskReward, _ := signal["risk_reward"].(float64)
	indicators, _ := signal["indicators"].(map[string]interface{})
	strategyID, _ := signal["strategy_id"].(string)

	var takeProfits []float64
	if tp, ok := signal["take_profits"].([]float64); ok {
		takeProfits = tp
	}

	// 数值保留2位小数
	quantity = round2(quantity)
	entryPrice = round2(entryPrice)
	stopLoss = round2(stopLoss)
	for i, tp := range takeProfits {
		takeProfits[i] = round2(tp)
	}
	var roundedIndicators map[string]interface{}
	if indicators != nil {
		roundedIndicators = make(map[string]interface{}, len(indicators))
		for k, v := range indicators {
			if f, ok := v.(float64); ok {
				roundedIndicators[k] = round2(f)
			} else {
				roundedIndicators[k] = v
			}
		}
	}
	var weightSnapshot *signalWeightSnapshot
	if rec, ok := s.latestWeightRecommendation(); ok {
		weightSnapshot = &signalWeightSnapshot{
			Template:       rec.Template,
			RouteBucket:    rec.Bucket,
			RouteReason:    rec.RouteReason,
			Score:          round2(rec.Score),
			ScoreSource:    rec.ScoreSource,
			BucketBudget:   round2(rec.BucketBudget),
			StrategyWeight: round2(rec.StrategyWeight),
			SymbolWeight:   round2(rec.SymbolWeight),
			RiskScale:      round2(rec.RiskScale),
			PositionBudget: round2(rec.PositionBudget),
			TradingPaused:  rec.TradingPaused,
			PauseReason:    rec.PauseReason,
		}
	}

	entry := signalLogEntry{
		Timestamp:    formatDecisionLogTime(now),
		Symbol:       s.symbol,
		Interval:     interval,
		OpenTime:     formatDecisionLogTime(time.UnixMilli(k.OpenTime).UTC()),
		CloseTime:    formatDecisionLogTime(time.UnixMilli(k.CloseTime).UTC()),
		OpenTimeMs:   k.OpenTime,
		CloseTimeMs:  k.CloseTime,
		StrategyID:   strategyID,
		Action:       action,
		Side:         side,
		EntryPrice:   entryPrice,
		Quantity:     quantity,
		StopLoss:     stopLoss,
		TakeProfits:  takeProfits,
		Reason:       reason,
		SignalReason: signalReason,
		IsTradable:   k.IsTradable,
		IsFinal:      k.IsFinal,
		Indicators:   newOrderedSignalIndicators(roundedIndicators),
		Weights:      weightSnapshot,
	}

	// 禁用 HTML 转义，避免 < > & 被编码为 \u003c 等
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(entry); err != nil {
		log.Printf("[signal-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		log.Printf("[signal-log] write failed: %v", err)
	}
	if s.analyticsWriter != nil {
		exitReasonKind, exitReasonLabel, tagsJSON, template := extractSignalReasonAnalytics(signalReason)
		s.analyticsWriter.WriteSignal(StrategySignalAnalyticsEntry{
			EventTime:       now,
			Symbol:          s.symbol,
			StrategyID:      strategyID,
			Template:        firstNonEmptyString(template, weightTemplate(weightSnapshot)),
			SignalID:        buildSignalAnalyticsID(strategyID, signalType, k.CloseTime),
			Action:          action,
			Side:            side,
			SignalType:      signalType,
			Quantity:        quantity,
			EntryPrice:      entryPrice,
			StopLoss:        stopLoss,
			TakeProfitJSON:  marshalAnalyticsJSON(takeProfits),
			Reason:          reason,
			ExitReasonKind:  exitReasonKind,
			ExitReasonLabel: exitReasonLabel,
			RiskReward:      round2(riskReward),
			Atr:             round2(atr),
			TagsJSON:        tagsJSON,
			TraceID:         buildSignalAnalyticsTraceID(strategyID, s.symbol, signalType, k.CloseTime),
		})
	}
}

// round2 四舍五入保留2位小数
func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

type decisionLogEntry struct {
	Timestamp   string                 `json:"timestamp"`
	TimestampBj string                 `json:"timestamp_bj,omitempty"`
	Symbol      string                 `json:"symbol"`
	Interval    string                 `json:"interval"`
	Stage       string                 `json:"stage"`    // entry / exit
	Decision    string                 `json:"decision"` // skip / pass / signal
	Reason      string                 `json:"reason"`
	ReasonCode  string                 `json:"reason_code"`
	HasPosition bool                   `json:"has_position"`
	IsFinal     bool                   `json:"is_final"`
	IsTradable  bool                   `json:"is_tradable"`
	OpenTime    string                 `json:"open_time"`
	CloseTime   string                 `json:"close_time"`
	OpenTimeMs  int64                  `json:"open_time_ms"`
	CloseTimeMs int64                  `json:"close_time_ms"`
	Extras      *orderedDecisionExtras `json:"extras,omitempty"`
}

type orderedDecisionExtras struct {
	values map[string]interface{}
}

// decisionRejectDetail 汇总一次入场被拦截时的主原因和全部原因，供不同策略变体复用统一 reject 摘要。
type decisionRejectDetail struct {
	PrimaryCode string
	Codes       []string
	Descs       []string
}

func newOrderedDecisionExtras(extras map[string]interface{}) *orderedDecisionExtras {
	if len(extras) == 0 {
		return nil
	}
	cloned := make(map[string]interface{}, len(extras)+1)
	for k, v := range extras {
		cloned[k] = v
	}
	if summary := buildDecisionExtrasSummary(cloned); summary != "" {
		cloned["summary"] = summary
	}
	return &orderedDecisionExtras{values: cloned}
}

func (o *orderedDecisionExtras) MarshalJSON() ([]byte, error) {
	if o == nil || len(o.values) == 0 {
		return []byte("null"), nil
	}

	orderedKeys := orderedDecisionExtraKeys(o.values)
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, key := range orderedKeys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, err := marshalJSONNoHTMLEscape(key)
		if err != nil {
			return nil, err
		}
		valJSON, err := marshalJSONNoHTMLEscape(o.values[key])
		if err != nil {
			return nil, err
		}
		buf.Write(keyJSON)
		buf.WriteByte(':')
		buf.Write(valJSON)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (o *orderedDecisionExtras) Summary() string {
	if o == nil || len(o.values) == 0 {
		return ""
	}
	return stringFromExtras(o.values["summary"])
}

func orderedDecisionExtraKeys(values map[string]interface{}) []string {
	priority := []string{
		"summary",
		"action", "side", "quantity", "entry_price", "stop_loss", "take_profits",
		"route_bucket", "route_reason", "route_template", "risk_scale", "position_budget",
		"trend", "pullback", "entry", "entry_mode",
		"reject_codes", "reject_descs",
		"price_in_range", "rsi_ok", "structure_ok", "ema_trend_ok",
		"crossover_cross_ok", "crossover_price_ok", "crossover_adx_ok", "crossover_macd_ok",
		"h1_rsi_low", "h1_rsi_high", "prev_h1_ema21", "prev_h1_low", "prev_h1_high",
	}
	for _, prefix := range []string{"h4", "h1", "m15"} {
		priority = append(priority,
			prefix+"_open_time",
			prefix+"_close",
			prefix+"_ema21",
			prefix+"_ema55",
			prefix+"_rsi",
			prefix+"_atr",
			prefix+"_is_dirty",
			prefix+"_dirty_reason",
			prefix+"_is_tradable",
			prefix+"_is_final",
		)
	}

	seen := make(map[string]struct{}, len(values))
	keys := make([]string, 0, len(values))
	for _, key := range priority {
		if _, ok := values[key]; ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}

	rest := make([]string, 0, len(values)-len(keys))
	for key := range values {
		if _, ok := seen[key]; !ok {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	return append(keys, rest...)
}

func buildDecisionExtrasSummary(values map[string]interface{}) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, 3)
	for _, prefix := range []string{"h4", "h1", "m15"} {
		if part := buildTimeframeSummary(prefix, values); part != "" {
			parts = append(parts, part)
		}
	}
	if part := buildDecisionRejectSummary(values); part != "" {
		parts = append(parts, part)
	}
	return strings.Join(parts, " -> ")
}

// buildDecisionRejectSummary 从结构化 extras 中提炼“为什么被跳过”的一句话摘要，减少排查时展开 JSON 的次数。
func buildDecisionRejectSummary(values map[string]interface{}) string {
	rejectDescs := stringListFromExtras(values["reject_descs"])
	if len(rejectDescs) == 0 {
		rejectDescs = stringListFromExtras(values["breakout_reject_descs"])
	}
	if len(rejectDescs) == 0 {
		return ""
	}
	return "reject=" + strings.Join(rejectDescs, "+")
}

// buildH1EntryRejectDetail 从 1H 判定 extras 中提炼失败条件，便于直接写入 decision 日志。
func buildH1EntryRejectDetail(values map[string]interface{}) *decisionRejectDetail {
	if len(values) == 0 {
		return nil
	}
	reject := newDecisionRejectDetail()
	trend := strings.ToUpper(strings.TrimSpace(stringFromExtras(values["trend"])))

	if !boolFromExtras(values["price_in_range"]) {
		reject.append("h1_pullback_price_not_in_band")
	}
	if !boolFromExtras(values["rsi_ok"]) {
		reject.append("h1_pullback_rsi_blocked")
	}
	if !boolFromExtras(values["structure_ok"]) {
		if trend == "SHORT" {
			reject.append("h1_short_structure_blocked")
		} else {
			reject.append("h1_long_structure_blocked")
		}
	}
	if _, ok := values["ema_trend_ok"]; ok && !boolFromExtras(values["ema_trend_ok"]) {
		reject.append("h1_pullback_ema_trend_blocked")
	}
	if _, ok := values["crossover_cross_ok"]; ok && !boolFromExtras(values["crossover_cross_ok"]) {
		if trend == "SHORT" {
			reject.append("h1_crossover_cross_down_missing")
		} else {
			reject.append("h1_crossover_cross_up_missing")
		}
	}
	if _, ok := values["crossover_price_ok"]; ok && !boolFromExtras(values["crossover_price_ok"]) {
		reject.append("h1_crossover_price_not_near_ema21")
	}
	if _, ok := values["crossover_adx_ok"]; ok && !boolFromExtras(values["crossover_adx_ok"]) {
		reject.append("h1_crossover_adx_blocked")
	}
	if _, ok := values["crossover_macd_ok"]; ok && !boolFromExtras(values["crossover_macd_ok"]) {
		reject.append("h1_crossover_macd_blocked")
	}
	if len(reject.Codes) == 0 {
		return nil
	}
	return reject
}

func buildTimeframeSummary(prefix string, values map[string]interface{}) string {
	openTimeKey := prefix + "_open_time"
	isDirtyKey := prefix + "_is_dirty"
	isTradableKey := prefix + "_is_tradable"
	isFinalKey := prefix + "_is_final"
	dirtyReasonKey := prefix + "_dirty_reason"

	_, hasOpenTime := values[openTimeKey]
	_, hasDirty := values[isDirtyKey]
	_, hasTradable := values[isTradableKey]
	_, hasFinal := values[isFinalKey]
	_, hasReason := values[dirtyReasonKey]
	if !hasOpenTime && !hasDirty && !hasTradable && !hasFinal && !hasReason {
		return ""
	}

	isDirty := boolFromExtras(values[isDirtyKey])
	isTradable := boolFromExtras(values[isTradableKey])
	isFinal := boolFromExtras(values[isFinalKey])
	dirtyReason := strings.TrimSpace(stringFromExtras(values[dirtyReasonKey]))

	status := "unknown"
	switch {
	case isDirty:
		status = "dirty"
	case isFinal && isTradable:
		status = "final"
	case isFinal && !isTradable:
		status = "final_not_tradable"
	case !isFinal && isTradable:
		status = "pending_finalization"
	default:
		status = "blocked"
	}

	if dirtyReason != "" && dirtyReason != "clean" {
		return fmt.Sprintf("%s=%s(%s)", prefix, status, dirtyReason)
	}
	return fmt.Sprintf("%s=%s", prefix, status)
}

// decorateDecisionReason 把 reject_descs 直接拼进顶层 reason，减少排查时必须展开 extras 的成本。
func decorateDecisionReason(reason string, extras map[string]interface{}) string {
	base := translateDecisionReason(reason)
	rejectDescs := stringListFromExtras(extras["reject_descs"])
	if len(rejectDescs) == 0 {
		rejectDescs = stringListFromExtras(extras["breakout_reject_descs"])
	}
	if len(rejectDescs) == 0 {
		return base
	}
	return base + "：" + strings.Join(rejectDescs, "；")
}

func boolFromExtras(v interface{}) bool {
	b, ok := v.(bool)
	return ok && b
}

func stringFromExtras(v interface{}) string {
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// stringListFromExtras 把 extras 中的字符串数组统一转换成 []string，兼容运行时 map 和测试反序列化后的 []interface{}。
func stringListFromExtras(v interface{}) []string {
	switch items := v.(type) {
	case []string:
		out := make([]string, 0, len(items))
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []interface{}:
		out := make([]string, 0, len(items))
		for _, item := range items {
			text, ok := item.(string)
			if !ok {
				continue
			}
			text = strings.TrimSpace(text)
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

// newDecisionRejectDetail 创建一个可去重收集原因码/中文说明的 reject detail 容器。
func newDecisionRejectDetail() *decisionRejectDetail {
	return &decisionRejectDetail{}
}

// append 把新的拒绝原因加入 detail，并自动补齐中文说明和主原因。
func (d *decisionRejectDetail) append(code string) {
	if d == nil {
		return
	}
	code = strings.TrimSpace(code)
	if code == "" {
		return
	}
	for _, existing := range d.Codes {
		if existing == code {
			return
		}
	}
	d.Codes = append(d.Codes, code)
	d.Descs = append(d.Descs, translateDecisionReason(code))
	if d.PrimaryCode == "" {
		d.PrimaryCode = code
	}
}

// applyToExtras 把 reject 详情写回决策 extras，供 summary 和下游排查复用。
func (d *decisionRejectDetail) applyToExtras(extras map[string]interface{}) {
	if d == nil || len(d.Codes) == 0 || extras == nil {
		return
	}
	extras["reject_codes"] = append([]string(nil), d.Codes...)
	extras["reject_descs"] = append([]string(nil), d.Descs...)
}

func decisionLogLevel(decision, reason string) string {
	switch reason {
	case "risk_limits_block", "harvest_path_block":
		return "BLOCK"
	}
	switch decision {
	case "signal":
		return "SIGNAL"
	case "pass":
		return "PASS"
	case "skip":
		return "SKIP"
	default:
		return "INFO"
	}
}

func formatDecisionLogTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()/int(time.Millisecond) == 0 {
		return t.Format("2006-01-02 15:04:05 UTC")
	}
	return t.Format("2006-01-02 15:04:05.000 UTC")
}

// formatDecisionLogTimeBJ 把 UTC 时间转换成北京时间，方便线上直接按东八区核对日志。
func formatDecisionLogTimeBJ(t time.Time) string {
	t = t.UTC().Add(8 * time.Hour)
	if t.Nanosecond()/int(time.Millisecond) == 0 {
		return t.Format("2006-01-02 15:04:05 UTC+8")
	}
	return t.Format("2006-01-02 15:04:05.000 UTC+8")
}

func (s *TrendFollowingStrategy) writeDecisionLogIfEnabled(stage, decision, reason string, k *marketpb.Kline, extras map[string]interface{}) {
	if s == nil || k == nil {
		return
	}
	if s.signalLogDir == "" {
		return
	}
	if s.getParam(paramDecisionLogEnabled, 0) <= 0 {
		return
	}

	now := time.Now().UTC()
	dateStr := now.Format("2006-01-02")
	dir := filepath.Join(s.signalLogDir, "decision", s.symbol)

	s.decisionLogMu.Lock()
	defer s.decisionLogMu.Unlock()

	key := s.symbol + "/" + dateStr
	f, ok := s.decisionLogFiles[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[decision-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[decision-log] failed to open %s: %v", path, err)
			return
		}
		s.decisionLogFiles[key] = f
	}

	enrichedExtras := s.enrichDecisionExtrasWithRoute(extras)
	if reason == "h1_no_pullback" {
		if reject := buildH1EntryRejectDetail(enrichedExtras); reject != nil {
			reject.applyToExtras(enrichedExtras)
		}
	}
	orderedExtras := newOrderedDecisionExtras(enrichedExtras)
	reasonText := decorateDecisionReason(reason, enrichedExtras)
	entry := decisionLogEntry{
		Timestamp:   formatDecisionLogTime(now),
		TimestampBj: formatDecisionLogTimeBJ(now),
		Symbol:      s.symbol,
		Interval:    k.Interval,
		Stage:       stage,
		Decision:    decision,
		Reason:      reasonText,
		ReasonCode:  reason,
		HasPosition: s.pos.side != sideNone,
		IsFinal:     k.IsFinal,
		IsTradable:  k.IsTradable,
		OpenTime:    formatDecisionLogTime(time.UnixMilli(k.OpenTime).UTC()),
		CloseTime:   formatDecisionLogTime(time.UnixMilli(k.CloseTime).UTC()),
		OpenTimeMs:  k.OpenTime,
		CloseTimeMs: k.CloseTime,
		Extras:      orderedExtras,
	}
	summary := orderedExtras.Summary()
	openTimeStr := time.UnixMilli(k.OpenTime).UTC().Format("15:04:05")
	closeTimeStr := time.UnixMilli(k.CloseTime).UTC().Format("15:04:05")
	level := decisionLogLevel(decision, reason)
	if summary != "" {
		log.Printf("[decision][%s] symbol=%s interval=%s openTime=%s closeTime=%s stage=%s decision=%s reason=%s summary=%s",
			level, s.symbol, k.Interval, openTimeStr, closeTimeStr, stage, decision, reason, summary)
	} else {
		log.Printf("[decision][%s] symbol=%s interval=%s openTime=%s closeTime=%s stage=%s decision=%s reason=%s",
			level, s.symbol, k.Interval, openTimeStr, closeTimeStr, stage, decision, reason)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(entry); err != nil {
		log.Printf("[decision-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		log.Printf("[decision-log] write failed: %v", err)
	}
	if s.analyticsWriter != nil {
		routeBucket, routeReason, template := extractDecisionRouteAnalytics(orderedExtras)
		s.analyticsWriter.WriteDecision(StrategyDecisionAnalyticsEntry{
			EventTime:   now,
			Symbol:      s.symbol,
			StrategyID:  s.strategySignalID(k.Interval),
			Template:    template,
			Interval:    k.Interval,
			Stage:       stage,
			Decision:    decision,
			Reason:      reasonText,
			ReasonCode:  reason,
			HasPosition: s.pos.side != sideNone,
			IsFinal:     k.IsFinal,
			IsTradable:  k.IsTradable,
			OpenTime:    time.UnixMilli(k.OpenTime).UTC(),
			CloseTime:   time.UnixMilli(k.CloseTime).UTC(),
			RouteBucket: routeBucket,
			RouteReason: routeReason,
			ExtrasJSON:  marshalAnalyticsJSON(orderedExtras),
			TraceID:     buildSignalAnalyticsTraceID(s.strategySignalID(k.Interval), s.symbol, decision, k.CloseTime),
		})
	}
}

// extractSignalReasonAnalytics 从 signal_reason 结构中提取分析库需要的关键字段。
func extractSignalReasonAnalytics(signalReason interface{}) (string, string, string, string) {
	switch payload := signalReason.(type) {
	case signalReasonPayload:
		tagsJSON := marshalAnalyticsJSON(payload.Tags)
		if tagsJSON == "null" || tagsJSON == "" {
			tagsJSON = "[]"
		}
		return payload.ExitReasonKind, payload.ExitReasonLabel, tagsJSON, payload.RouteTemplate
	case map[string]interface{}:
		exitReasonKind, _ := payload["exit_reason_kind"].(string)
		exitReasonLabel, _ := payload["exit_reason_label"].(string)
		routeTemplate, _ := payload["route_template"].(string)
		tagsJSON := "[]"
		if tags, ok := payload["tags"]; ok {
			tagsJSON = marshalAnalyticsJSON(tags)
		}
		if tagsJSON == "null" || tagsJSON == "" {
			tagsJSON = "[]"
		}
		return exitReasonKind, exitReasonLabel, tagsJSON, routeTemplate
	default:
		return "", "", "[]", ""
	}
}

// extractDecisionRouteAnalytics 从 decision extras 中提取路由与模板信息。
func extractDecisionRouteAnalytics(extras *orderedDecisionExtras) (string, string, string) {
	if extras == nil || len(extras.values) == 0 {
		return "", "", ""
	}
	routeBucket, _ := extras.values["route_bucket"].(string)
	routeReason, _ := extras.values["route_reason"].(string)
	template, _ := extras.values["route_template"].(string)
	if template == "" {
		template, _ = extras.values["template"].(string)
	}
	return routeBucket, routeReason, template
}

// marshalAnalyticsJSON 把任意结构编码为紧凑 JSON 字符串，失败时返回空 JSON。
func marshalAnalyticsJSON(v interface{}) string {
	if v == nil {
		return "{}"
	}
	data, err := marshalJSONNoHTMLEscape(v)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// marshalJSONNoHTMLEscape 使用标准 JSON 编码，但保留 >、<、& 原字符，便于日志直读。
func marshalJSONNoHTMLEscape(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// buildSignalAnalyticsID 生成 signal_fact 使用的稳定信号ID。
func buildSignalAnalyticsID(strategyID, signalType string, closeTimeMs int64) string {
	return fmt.Sprintf("%s-%s-%d", firstNonEmptyString(strategyID, "strategy"), firstNonEmptyString(signalType, "signal"), closeTimeMs)
}

// buildSignalAnalyticsTraceID 生成分析事件使用的 trace_id。
func buildSignalAnalyticsTraceID(strategyID, symbol, suffix string, ts int64) string {
	return fmt.Sprintf("%s-%s-%s-%d", firstNonEmptyString(strategyID, "strategy"), firstNonEmptyString(symbol, "symbol"), firstNonEmptyString(suffix, "event"), ts)
}

// weightTemplate 安全读取权重快照中的模板名。
func weightTemplate(snapshot *signalWeightSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return snapshot.Template
}

// firstNonEmptyString 返回第一个非空字符串，用于模板与trace字段兜底。
func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func translateDecisionReason(reason string) string {
	switch reason {
	case "risk_limits_block":
		return "风控限制阻止开仓"
	case "range_m15_not_ready":
		return "15分钟震荡指标未就绪"
	case "range_1m_not_ready":
		return "1分钟震荡指标未就绪"
	case "range_no_entry":
		return "震荡策略未满足入场条件"
	case "range_h4_oscillation_missing":
		return "4小时震荡评分不足，暂不做区间反转"
	case "range_h1_adx_too_high":
		return "1小时ADX偏高，当前更像趋势而非震荡"
	case "range_h1_boll_too_wide":
		return "1小时布林带过宽，区间约束不足"
	case "range_h1_boll_too_narrow":
		return "1小时布林带过窄，波动空间不足"
	case "range_h1_middle_zone":
		return "价格位于1小时区间中部，未到边缘反转位"
	case "range_long_h1_rsi_blocked":
		return "1小时RSI过滤阻止做多"
	case "range_long_h4_trend_blocked":
		return "4小时方向过滤阻止做多"
	case "range_long_h1_candle_blocked":
		return "1小时K线方向未支持做多"
	case "range_long_rsi_signal_missing":
		return "做多侧RSI超卖/反弹信号不足"
	case "range_long_rsi_turn_missing":
		return "做多侧RSI尚未拐头向上"
	case "range_long_bb_bounce_missing":
		return "做多侧未出现下轨回收确认"
	case "range_long_signal_count_low":
		return "做多侧信号数量不足"
	case "range_long_not_near_low":
		return "价格未贴近区间下沿"
	case "range_long_fake_breakout_not_confirmed":
		return "下破区间后未满足假突破反向做多条件"
	case "range_short_h1_rsi_blocked":
		return "1小时RSI过滤阻止做空"
	case "range_short_h4_trend_blocked":
		return "4小时方向过滤阻止做空"
	case "range_short_h1_candle_blocked":
		return "1小时K线方向未支持做空"
	case "range_short_rsi_signal_missing":
		return "做空侧RSI超买/回落信号不足"
	case "range_short_rsi_turn_missing":
		return "做空侧RSI尚未拐头向下"
	case "range_short_bb_bounce_missing":
		return "做空侧未出现上轨回收确认"
	case "range_short_signal_count_low":
		return "做空侧信号数量不足"
	case "range_short_not_near_high":
		return "价格未贴近区间上沿"
	case "range_short_fake_breakout_not_confirmed":
		return "上破区间后未满足假突破反向做空条件"
	case "m15_not_ready_atr_0":
		return "15分钟指标未就绪，ATR为0"
	case "harvest_path_block":
		return "Harvest Path 风险过滤阻止开仓"
	case "h4_not_ready":
		return "4小时指标未就绪"
	case "h1_not_ready":
		return "1小时指标未就绪"
	case "h4_no_trend":
		return "4小时没有明确趋势"
	case "h1_no_pullback":
		return "1小时没有满足回调或交叉入场条件"
	case "h1_pullback_price_not_in_band":
		return "1小时价格未落在 EMA21~EMA55 回调带内"
	case "h1_pullback_rsi_blocked":
		return "1小时 RSI 未落在允许区间"
	case "h1_long_structure_blocked":
		return "1小时做多结构被破坏，当前价格未站稳前低之上"
	case "h1_short_structure_blocked":
		return "1小时做空结构被破坏，当前价格未压回前高之下"
	case "h1_pullback_ema_trend_blocked":
		return "1小时 EMA21 斜率未继续顺趋势"
	case "h1_crossover_cross_up_missing":
		return "1小时做多未形成 EMA21 上穿 EMA55"
	case "h1_crossover_cross_down_missing":
		return "1小时做空未形成 EMA21 下穿 EMA55"
	case "h1_crossover_price_not_near_ema21":
		return "1小时交叉后价格未回到 EMA21 附近允许区间"
	case "h1_crossover_adx_blocked":
		return "4小时 ADX 未达到趋势交叉入场阈值"
	case "h1_crossover_macd_blocked":
		return "1小时 MACD 柱未给出同向增强确认"
	case "m15_no_entry":
		return "15分钟没有满足入场条件"
	case "m15_history_insufficient":
		return "15分钟历史K线不足，无法判断突破"
	case "m15_long_structure_missing":
		return "多头入场时价格未突破近期高点"
	case "m15_long_rsi_signal_missing":
		return "多头入场时RSI未上穿50且未达到偏强阈值"
	case "m15_short_structure_missing":
		return "空头入场时价格未跌破近期低点"
	case "m15_short_rsi_signal_missing":
		return "空头入场时RSI未下穿50且未达到偏弱阈值"
	case "breakout_no_entry":
		return "突破策略未满足入场条件"
	case "breakout_no_price_break":
		return "价格未突破前高/前低"
	case "breakout_volume_low":
		return "量能不足，未达到放量确认条件"
	case "breakout_long_rsi_low":
		return "多头突破时 RSI 不够强"
	case "breakout_short_rsi_high":
		return "空头突破时 RSI 不够弱"
	case "breakout_rsi_not_ready":
		return "RSI 条件未满足"
	case "breakout_long_ema_misaligned":
		return "多头突破时均线结构未对齐"
	case "breakout_short_ema_misaligned":
		return "空头突破时均线结构未对齐"
	case "breakout_ema_misaligned":
		return "均线结构未对齐"
	case "open_signal_sent":
		return "已发送开仓信号"
	default:
		return reason
	}
}

func stringTrend(trend trendDirection) string {
	switch trend {
	case trendLong:
		return "LONG"
	case trendShort:
		return "SHORT"
	default:
		return "NONE"
	}
}

func stringPullback(pullback pullbackResult) string {
	switch pullback {
	case pullbackLong:
		return "LONG"
	case pullbackShort:
		return "SHORT"
	default:
		return "NONE"
	}
}

func stringEntry(entry entrySignal) string {
	switch entry {
	case entryLong:
		return "LONG"
	case entryShort:
		return "SHORT"
	default:
		return "NONE"
	}
}

// ---------------------------------------------------------------------------
// 1m分钟周期信号模式
//
// 使用1m K线作为入场/出场周期，利用1m级别的价格波动和指标生成交易信号。
// 与15m模式相比，1m模式信号更频繁、响应更快，适合模拟测试和1m撮合引擎配合使用。
//
// 入场逻辑：
//   1. 风控检查（连亏/日亏损限制）
//   2. 方向判断：基于1m RSI + 15m/1h趋势确认
//   3. 入场信号：1m结构突破 或 RSI穿越 + 偏置
//   4. 止损止盈：ATR × N 动态计算
//
// 出场逻辑：
//   1. 止损检查
//   2. 止盈检查（TP1移动止损 / TP2全平）
//   3. EMA破位出场（持仓≥N根1m K线后检查）
// ---------------------------------------------------------------------------

// checkEntryConditions1m 1m模式入场条件检查
//
// 入场流程：
//  1. 风控限制检查
//  2. 方向判断：
//     - 调试模式：直接用1m RSI判断
//     - 正常模式：4H趋势 → 1H回调 → 1M入场信号
//  3. 计算仓位、止损、止盈
//  4. 发送开仓信号
func (s *TrendFollowingStrategy) checkEntryConditions1m(ctx context.Context, k *marketpb.Kline) error {
	// 风控检查
	if !s.checkRiskLimits() {
		return nil
	}

	// 读取1m K线快照
	m1 := s.latest1m
	if m1.Atr == 0 {
		log.Printf("[策略1m] %s 1m指标未就绪（ATR=0），跳过入场", s.symbol)
		return nil
	}

	// 调试模式参数
	skip4H := s.getParam(paramDebugSkip4HTrend, 0) > 0
	skipPullback := s.getParam(paramDebugSkipPullback, 0) > 0
	skip1mEntry := s.getParam(paramDebugSkip15MEntry, 0) > 0

	if skip4H && skipPullback {
		// 调试模式：跳过4H和1H，直接用1m RSI判断方向
		var trend trendDirection
		var pullback pullbackResult
		var entry entrySignal

		if m1.Rsi < 50 {
			trend = trendShort
			pullback = pullbackShort
			entry = entryShort
		} else {
			trend = trendLong
			pullback = pullbackLong
			entry = entryLong
		}

		log.Printf("[策略1m] %s 快速调试模式 | RSI=%.2f → trend=%v pullback=%v entry=%v ATR=%.2f",
			s.symbol, m1.Rsi, trend, pullback, entry, m1.Atr)

		// 如果不跳过1m入场信号判断，则走正常1m入场判断
		if !skip1mEntry {
			entry = s.judge1MEntry(pullback)
			if entry == entryNone {
				return nil
			}
		}
		if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
			return nil
		}

		return s.openPosition1m(ctx, k, trend, pullback, entryModePullback, 1.0, entry, m1)
	}

	// ========== 正常三层过滤逻辑（1m级别） ==========

	// 第1层：4H趋势判断
	if s.latest4h.Ema21 == 0 || s.latest4h.Ema55 == 0 {
		log.Printf("[策略1m] %s 4H指标未就绪，跳过入场", s.symbol)
		return nil
	}
	trend := s.judge4HTrend()
	if trend == trendNone {
		return nil
	}

	// 第2层：1H 入场形态确认
	if s.latest1h.Ema21 == 0 || s.latest1h.Ema55 == 0 {
		log.Printf("[策略1m] %s 1H指标未就绪，跳过入场", s.symbol)
		return nil
	}
	entryDecision := s.evaluate1HEntryDecision(trend)
	if entryDecision.Pullback == pullbackNone {
		return nil
	}

	// 第3层：1m入场信号
	entry := s.judge1MEntry(entryDecision.Pullback)
	if entry == entryNone {
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		return nil
	}

	return s.openPosition1m(ctx, k, trend, entryDecision.Pullback, entryDecision.Mode, entryDecision.Scale, entry, m1)
}

func (s *TrendFollowingStrategy) shouldBlockForHarvestPathRisk(ctx context.Context, k *marketpb.Kline, entry entrySignal) bool {
	if s.getParam(paramHarvestPathModelEnabled, 0) <= 0 {
		return false
	}
	entrySide, ok := toHarvestPathEntrySide(entry)
	if !ok {
		return false
	}

	input := harvestpathmodel.Context{
		Symbol:    s.symbol,
		EventTime: k.GetEventTime(),
		LastPrice: k.GetClose(),
		EntrySide: entrySide,
		Candles1m: s.buildHarvestPathCandles(),
		OrderBook: s.buildHarvestPathOrderBook(),
	}
	blockThreshold, volatilityRegime, thresholdSource := s.harvestPathBlockThresholdForCurrentRegime()
	detector := harvestpathmodel.NewDetector(
		int(s.getParam(paramHarvestPathLookback, 20)),
		blockThreshold,
	)
	signal, err := detector.Evaluate(input)
	if err != nil || signal == nil {
		return false
	}
	s.applyHarvestPathLSTM(ctx, detector, signal)
	s.applyHarvestPathBook(detector, signal, input.OrderBook, entrySide, k.GetClose())
	s.publishHarvestPathSignal(ctx, k, entrySide, signal, volatilityRegime, thresholdSource, blockThreshold)

	logThreshold := s.getParam(paramHarvestPathLogThreshold, 0.60)
	if signal.HarvestPathProbability >= logThreshold {
		log.Printf("[harvest-path] %s entry=%s target=%s prob=%.2f rule_prob=%.2f lstm_prob=%.2f book_prob=%.2f block_threshold=%.2f regime=%s threshold_source=%s stop_density=%.2f trigger=%.2f zone=[%.2f,%.2f] path_action=%s",
			s.symbol, entrySide, signal.TargetSide, signal.HarvestPathProbability, signal.RuleProbability,
			signal.LSTMProbability, signal.BookProbability, blockThreshold, volatilityRegime, thresholdSource, signal.StopDensityScore, signal.TriggerScore, signal.TargetZoneLow, signal.TargetZoneHigh, signal.PathAction)
	}
	if detector.ShouldBlock(signal) {
		log.Printf("[harvest-path] %s hold entry=%s and wait for reclaim prob=%.2f threshold=%.2f regime=%s ref=%.2f market=%.2f",
			s.symbol, entrySide, signal.HarvestPathProbability, blockThreshold, volatilityRegime, signal.ReferencePrice, signal.MarketPrice)
		return true
	}
	return false
}

func (s *TrendFollowingStrategy) harvestPathBlockThresholdForCurrentRegime() (float64, string, string) {
	base := s.getParam(paramHarvestPathBlockThreshold, 0.80)
	if s == nil || s.harvestPathLSTMPredictor == nil {
		return base, "", "global"
	}
	if s.getParam(paramHarvestPathLSTMEnabled, 0) <= 0 {
		return base, "", "global"
	}
	if s.getParam(paramHarvestPathRegimeAwareThresholdEnabled, 1) <= 0 {
		return base, "", "global"
	}
	regime, ok := s.currentHarvestPathVolatilityRegime(int(s.getParam(paramHarvestPathRegimeLookback, 120)))
	if !ok {
		return base, "", "global"
	}
	threshold, matched := s.harvestPathLSTMPredictor.RegimeThreshold(s.symbol, regime, base)
	if matched {
		return threshold, regime, "regime"
	}
	if threshold != base {
		return threshold, regime, "artifact_default"
	}
	return base, regime, "global"
}

func (s *TrendFollowingStrategy) currentHarvestPathVolatilityRegime(lookback int) (string, bool) {
	if s == nil || len(s.klines1m) == 0 {
		return "", false
	}
	if lookback <= 0 {
		lookback = 120
	}
	start := len(s.klines1m) - lookback
	if start < 0 {
		start = 0
	}
	atrPcts := make([]float64, 0, len(s.klines1m)-start)
	for _, item := range s.klines1m[start:] {
		if item.Close <= 0 || item.Atr <= 0 {
			continue
		}
		atrPcts = append(atrPcts, item.Atr/item.Close)
	}
	if len(atrPcts) < 3 {
		return "", false
	}
	current := atrPcts[len(atrPcts)-1]
	sorted := append([]float64(nil), atrPcts...)
	sort.Float64s(sorted)
	lowCut := percentile(sorted, 0.33)
	highCut := percentile(sorted, 0.67)
	switch {
	case current <= lowCut:
		return "LOW", true
	case current >= highCut:
		return "HIGH", true
	default:
		return "MID", true
	}
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := q * float64(len(sorted)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return sorted[lower]
	}
	weight := pos - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func (s *TrendFollowingStrategy) applyHarvestPathLSTM(ctx context.Context, detector *harvestpathmodel.Detector, signal *harvestpathmodel.Signal) {
	if s == nil || detector == nil || signal == nil {
		return
	}
	if s.getParam(paramHarvestPathLSTMEnabled, 0) <= 0 {
		return
	}
	if s.harvestPathLSTMPredictor == nil {
		return
	}
	prediction, err := s.harvestPathLSTMPredictor.Predict(ctx, s.symbol)
	if err != nil {
		log.Printf("[harvest-path] %s lstm liquidity sweep risk recognition failed: %v", s.symbol, err)
		return
	}
	if prediction == nil {
		return
	}
	detector.BlendLSTMProbability(signal, prediction.HarvestPathProbability, s.getParam(paramHarvestPathLSTMWeight, 0.35))
}

func (s *TrendFollowingStrategy) applyHarvestPathBook(
	detector *harvestpathmodel.Detector,
	signal *harvestpathmodel.Signal,
	orderBook *harvestpathmodel.OrderBookSnapshot,
	entrySide harvestpathmodel.EntrySide,
	lastPrice float64,
) {
	if s == nil || detector == nil || signal == nil || orderBook == nil {
		return
	}
	if s.getParam(paramHarvestPathBookEnabled, 0) <= 0 {
		return
	}
	encoder := harvestpathmodel.NewBookEncoder(
		int(s.getParam(paramHarvestPathBookTopN, 10)),
		s.getParam(paramHarvestPathBookSpreadBpsCap, 12),
	)
	features := encoder.Encode(orderBook, lastPrice, entrySide)
	if features == nil {
		return
	}
	detector.BlendBookProbability(
		signal,
		features.Probability,
		features.SpreadBps,
		features.DepthImbalance,
		features.DirectionalPressure,
		features.Summary,
		s.getParam(paramHarvestPathBookWeight, 0.35),
	)
}

func (s *TrendFollowingStrategy) publishHarvestPathSignal(
	ctx context.Context,
	k *marketpb.Kline,
	entrySide harvestpathmodel.EntrySide,
	signal *harvestpathmodel.Signal,
	volatilityRegime string,
	thresholdSource string,
	appliedThreshold float64,
) {
	if s == nil || s.harvestPathProducer == nil || signal == nil || k == nil {
		return
	}
	threshold := s.getParam(paramHarvestPathPublishThreshold, 0.60)
	if signal.HarvestPathProbability < threshold {
		return
	}

	msg := map[string]interface{}{
		"symbol":                   s.symbol,
		"event_time":               signal.EventTime,
		"interval":                 k.GetInterval(),
		"model":                    "harvest_path_liquidity_sweep_risk_v1",
		"entry_side":               string(entrySide),
		"target_side":              string(signal.TargetSide),
		"target_zone_low":          signal.TargetZoneLow,
		"target_zone_high":         signal.TargetZoneHigh,
		"reference_price":          signal.ReferencePrice,
		"market_price":             signal.MarketPrice,
		"stop_density_score":       signal.StopDensityScore,
		"trigger_score":            signal.TriggerScore,
		"rule_probability":         signal.RuleProbability,
		"lstm_probability":         signal.LSTMProbability,
		"book_probability":         signal.BookProbability,
		"book_spread_bps":          signal.BookSpreadBps,
		"book_imbalance":           signal.BookImbalance,
		"book_pressure":            signal.BookPressure,
		"book_summary":             signal.BookSummary,
		"volatility_regime":        volatilityRegime,
		"threshold_source":         thresholdSource,
		"applied_threshold":        appliedThreshold,
		"harvest_path_probability": signal.HarvestPathProbability,
		"expected_path_depth":      signal.ExpectedPathDepth,
		"expected_reversal_speed":  signal.ExpectedReversalSpeed,
		"path_action":              signal.PathAction,
		"risk_level":               signal.RiskLevel,
		"is_closed":                k.GetIsClosed(),
		"is_final":                 k.GetIsFinal(),
		"is_tradable":              k.GetIsTradable(),
		"volume":                   k.GetVolume(),
		"quote_volume":             k.GetQuoteVolume(),
		"taker_buy_volume":         k.GetTakerBuyVolume(),
	}
	if err := s.harvestPathProducer.SendMarketData(ctx, msg); err != nil {
		log.Printf("[harvest-path] %s publish path signal failed: %v", s.symbol, err)
	}
}

func (s *TrendFollowingStrategy) buildHarvestPathOrderBook() *harvestpathmodel.OrderBookSnapshot {
	if s == nil || s.latestDepth == nil {
		return nil
	}
	snapshot := &harvestpathmodel.OrderBookSnapshot{
		Timestamp: s.latestDepth.Timestamp,
		Bids:      append([]harvestpathmodel.BookLevel(nil), s.latestDepth.Bids...),
		Asks:      append([]harvestpathmodel.BookLevel(nil), s.latestDepth.Asks...),
	}
	return snapshot
}

func (s *TrendFollowingStrategy) buildHarvestPathCandles() []harvestpathmodel.Candle {
	if len(s.klines1m) == 0 {
		return nil
	}
	items := make([]harvestpathmodel.Candle, 0, len(s.klines1m))
	for _, item := range s.klines1m {
		items = append(items, harvestpathmodel.Candle{
			OpenTime:       item.OpenTime,
			Open:           item.Open,
			High:           item.High,
			Low:            item.Low,
			Close:          item.Close,
			Volume:         item.Volume,
			QuoteVolume:    item.QuoteVol,
			TakerBuyVolume: item.TakerBuy,
			Atr:            item.Atr,
		})
	}
	return items
}

func toHarvestPathEntrySide(entry entrySignal) (harvestpathmodel.EntrySide, bool) {
	switch entry {
	case entryLong:
		return harvestpathmodel.EntrySideLong, true
	case entryShort:
		return harvestpathmodel.EntrySideShort, true
	default:
		return "", false
	}
}

// judge1MEntry 判断1分钟入场信号
//
// 入场条件（OR模式默认）：
//   - 结构突破：1m收盘价突破近N根K线的高低点
//   - RSI信号：RSI穿越50中线 或 达到偏置阈值
//
// 多头入场：
//   - 结构突破：1m收盘价 > 近期高点
//   - RSI信号：RSI穿越50向上 或 RSI ≥ 多头偏置阈值
//
// 空头入场：
//   - 结构突破：1m收盘价 < 近期低点
//   - RSI信号：RSI穿越50向下 或 RSI ≤ 空头偏置阈值
func (s *TrendFollowingStrategy) judge1MEntry(pullback pullbackResult) entrySignal {
	m1 := s.latest1m
	if m1.Rsi == 0 || m1.Atr == 0 {
		return entryNone
	}

	lookback := int(s.getParam(param1mBreakoutLookback, 20))
	rsiBiasLong := s.getParam(param1mRsiBiasLong, 55)
	rsiBiasShort := s.getParam(param1mRsiBiasShort, 45)
	requireBoth := s.getParam(paramRequireBothSignals, 0) > 0

	// 计算近期高低点（排除当前K线）
	var recentHigh, recentLow float64
	if len(s.klines1m) > 1 {
		start := len(s.klines1m) - 1 - lookback
		if start < 0 {
			start = 0
		}
		recentHigh = s.klines1m[start].High
		recentLow = s.klines1m[start].Low
		for i := start + 1; i < len(s.klines1m)-1; i++ {
			if s.klines1m[i].High > recentHigh {
				recentHigh = s.klines1m[i].High
			}
			if s.klines1m[i].Low < recentLow {
				recentLow = s.klines1m[i].Low
			}
		}
	} else {
		return entryNone // 数据不足
	}

	// 前一根K线的RSI
	var prevRsi float64
	if len(s.klines1m) >= 2 {
		prevRsi = s.klines1m[len(s.klines1m)-2].Rsi
	}

	if pullback == pullbackLong {
		// 多头入场信号
		breakoutUp := m1.Close > recentHigh      // 结构突破
		rsiCross := m1.Rsi > 50 && prevRsi <= 50 // RSI穿越50向上
		rsiBias := m1.Rsi >= rsiBiasLong         // RSI偏置

		structureSignal := breakoutUp
		rsiSignal := rsiCross || rsiBias

		if requireBoth {
			if structureSignal && rsiSignal {
				return entryLong
			}
		} else {
			if structureSignal || rsiSignal {
				return entryLong
			}
		}
	} else if pullback == pullbackShort {
		// 空头入场信号
		breakoutDown := m1.Close < recentLow     // 结构突破
		rsiCross := m1.Rsi < 50 && prevRsi >= 50 // RSI穿越50向下
		rsiBias := m1.Rsi <= rsiBiasShort        // RSI偏置

		structureSignal := breakoutDown
		rsiSignal := rsiCross || rsiBias

		if requireBoth {
			if structureSignal && rsiSignal {
				return entryShort
			}
		} else {
			if structureSignal || rsiSignal {
				return entryShort
			}
		}
	}

	return entryNone
}

// openPosition1m 1m模式开仓：计算止损止盈并发送入场信号。
func (s *TrendFollowingStrategy) openPosition1m(ctx context.Context, k *marketpb.Kline, trend trendDirection, pullback pullbackResult, mode entryMode, scale float64, entry entrySignal, m1 klineSnapshot) error {
	atrMult := s.getParam(param1mAtrMult, 1.5)
	stopDist := atrMult * m1.Atr

	var action, side string
	var stopLoss, tp1, tp2 float64

	if entry == entryLong {
		action = "BUY"
		side = "LONG"
		stopLoss = m1.Close - stopDist
		tp1 = m1.Close + stopDist   // +1R
		tp2 = m1.Close + 3*stopDist // +3R
	} else {
		action = "SELL"
		side = "SHORT"
		stopLoss = m1.Close + stopDist
		tp1 = m1.Close - stopDist   // +1R
		tp2 = m1.Close - 3*stopDist // +3R
	}

	// 仓位计算
	quantity := s.calculatePositionSize(m1.Close, stopLoss)

	// 统一使用 1H 决策返回的仓位缩放，避免深回调缩放和交叉缩放叠加。
	if scale > 0 {
		quantity *= scale
	}
	adjustedQuantity, blocked, blockReason := s.applyWeightRecommendation(quantity)
	if blocked {
		extras := map[string]interface{}{
			"base_quantity": quantity,
			"reason":        blockReason,
		}
		appendSnapshotExtras(extras, "h4", s.latest4h)
		appendSnapshotExtras(extras, "h1", s.latest1h)
		appendSnapshotExtras(extras, "m15", s.latest15m)
		appendSnapshotExtras(extras, "m1", m1)
		s.writeDecisionLogIfEnabled("entry", "skip", blockReason, k, extras)
		log.Printf("[策略1m入场跳过] %s %s | base_qty=%.4f | %s", s.symbol, action, quantity, blockReason)
		return nil
	}
	quantity = adjustedQuantity

	// 构建入场原因
	reason := s.buildEntryReason1m(trend, pullback, mode, entry, m1, stopLoss, tp1, tp2)
	signalReason := s.newSignalReason(
		reason,
		"OPEN_ENTRY",
		fmt.Sprintf("4H趋势=%s", describeTrendContext(trend)),
		fmt.Sprintf("1H形态=%s | 1M入场确认", describeEntrySetupContext(mode, pullback)),
		fmt.Sprintf("RSI=%.2f ATR=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f", m1.Rsi, m1.Atr, stopLoss, tp1, tp2),
		"1m", "trend_following", "open", side,
	)

	// 记录持仓
	exitPolicy, exitState := s.initializeExitPolicyState(m1.Close, stopLoss)
	s.pos = position{
		side:          sideLong,
		entryMode:     mode,
		entryPrice:    m1.Close,
		quantity:      quantity,
		stopLoss:      stopLoss,
		takeProfit1:   tp1,
		takeProfit2:   tp2,
		entryBarTime:  m1.OpenTime,
		atr:           m1.Atr,
		hitTP1:        false,
		partialClosed: false,
		exitPolicy:    exitPolicy,
		exitState:     exitState,
	}
	if entry == entryShort {
		s.pos.side = sideShort
	}

	// 计算风险收益比
	riskReward := 0.0
	tpStopDist := math.Abs(m1.Close - stopLoss)
	if tpStopDist > 0 {
		riskReward = math.Abs(tp1-m1.Close) / tpStopDist
	}

	// 发送开仓信号
	signal := map[string]interface{}{
		"strategy_id":   s.strategySignalID("1m"),
		"symbol":        s.symbol,
		"interval":      "1m",
		"action":        action,
		"side":          side,
		"signal_type":   "OPEN",
		"quantity":      quantity,
		"entry_price":   m1.Close,
		"stop_loss":     stopLoss,
		"take_profits":  []float64{tp1, tp2},
		"reason":        signalReason.Summary,
		"signal_reason": signalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           m1.Atr,
		"risk_reward":   riskReward,
		"indicators": map[string]interface{}{
			"h4_ema21":       s.latest4h.Ema21,
			"h4_ema55":       s.latest4h.Ema55,
			"h4_adx":         s.latestStrategyH4Adx(),
			"h1_ema21":       s.latest1h.Ema21,
			"h1_ema55":       s.latest1h.Ema55,
			"h1_rsi":         s.latest1h.Rsi,
			"h1_macd_hist":   s.latestStrategyH1MacdHist(),
			"m15_ema21":      s.latest15m.Ema21,
			"m15_rsi":        s.latest15m.Rsi,
			"m15_atr":        s.latest15m.Atr,
			"m1_ema21":       m1.Ema21,
			"m1_rsi":         m1.Rsi,
			"m1_atr":         m1.Atr,
			"entry_mode":     stringEntryMode(mode),
			"position_scale": scale,
		},
	}

	log.Printf("[策略1m入场] %s %s | 价格=%.2f | 止损=%.2f | TP1=%.2f | TP2=%.2f | %s",
		s.symbol, action, m1.Close, stopLoss, tp1, tp2, reason)

	return s.sendSignal(ctx, signal, k)
}

// buildEntryReason1m 构建1m入场信号原因描述。
func (s *TrendFollowingStrategy) buildEntryReason1m(trend trendDirection, pullback pullbackResult, mode entryMode, entry entrySignal, m1 klineSnapshot, stopLoss, tp1, tp2 float64) string {
	return fmt.Sprintf(
		"[1m] 4H趋势=%s | 1H形态=%s | 1M入场=%s | RSI=%.2f ATR=%.2f | 止损=%.2f(1.5×ATR) TP1=%.2f(+1R) TP2=%.2f(+3R)",
		describeTrendContext(trend), describeEntrySetupContext(mode, pullback), stringEntry(entry), m1.Rsi, m1.Atr, stopLoss, tp1, tp2,
	)
}

// checkExitConditions1m 1m模式出场条件检查
//
// 出场优先级：
//  1. 止损：价格穿越止损价
//  2. +1R：只把止损移动到保本
//  3. +3R：部分止盈（平半）
//  4. EMA破位：持仓≥N根1m K线后，价格穿越EMA21-缓冲带
func (s *TrendFollowingStrategy) checkExitConditions1m(ctx context.Context, k *marketpb.Kline) error {
	m1 := s.latest1m

	// 先执行 1m 快速保护减仓，再继续走原有 1m 止损/止盈/EMA 破位流程。
	if fastProtect := s.evaluateFastProtectOn1m(); fastProtect != nil {
		s.writeDecisionLogIfEnabled("exit", "signal", fastProtect.ReasonCode, k, map[string]interface{}{
			"event_code":      string(fastProtect.Code),
			"event_desc":      fastProtect.ReasonLabel,
			"interval":        fastProtect.Interval,
			"reduce_pct":      fastProtect.ReducePct,
			"raise_stop_to":   fastProtect.RaiseStopTo,
			"stop_raise_mode": fastProtect.StopRaiseMode,
			"trigger_price":   fastProtect.TriggerPrice,
			"current_r":       fastProtect.CurrentR,
			"position_stage":  fastProtect.PositionStage,
		})
		if err := s.applyFastProtectOn1m(ctx, k, fastProtect); err != nil {
			return err
		}
	}

	// 计算持仓K线数
	heldBars := 0
	for _, snap := range s.klines1m {
		if snap.OpenTime >= s.pos.entryBarTime {
			heldBars++
		}
	}

	minHoldingBars := int(s.getParam(param1mMinHoldingBars, 5))
	exitConfirmBars := int(s.getParam(paramEmaExitConfirmBars, 2))
	emaBufferATR := s.getParam(param1mEmaExitBufferATR, 0.30)

	decision := s.evaluateTrendExitDecision(m1, heldBars, minHoldingBars, exitConfirmBars, emaBufferATR, "1m")
	if decision.SignalType == "" {
		// 无出场信号，记录持仓状态
		log.Printf("[策略1m] %s 持仓中 | 方向=%v 价格=%.2f 止损=%.2f TP1=%.2f TP2=%.2f 持仓K线=%d",
			s.symbol, s.pos.side, m1.Close, s.pos.stopLoss, s.pos.takeProfit1, s.pos.takeProfit2, heldBars)
		return nil
	}
	return s.applyTrendExitDecision(ctx, k, m1, "1m", decision)
}
