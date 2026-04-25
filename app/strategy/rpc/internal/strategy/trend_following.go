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

	"exchange-system/app/strategy/rpc/internal/kafka"
	harvestpathmodel "exchange-system/app/strategy/rpc/internal/strategy/harvestpath"
	marketpb "exchange-system/common/pb/market"
)

// ============================================================================
// 多时间周期趋势跟踪策略 (Strategy3)
//
// 架构：4H趋势判断 + 1H回调确认 + 15M入场信号
//   - 4小时：判断主趋势方向（EMA21 vs EMA55 vs 价格）
//   - 1小时：确认回调位置是否健康（价格在EMA21~EMA55区间 + RSI过滤）
//   - 15分钟：寻找入场信号（结构突破 / RSI穿越 + ATR动态止损）
//
// 指标来源：直接使用 Market 服务预计算的 EMA/RSI/ATR（4500根历史预热）
// 止损：ATR × 1.5 动态止损
// 止盈：1.5×ATR（第一目标）/ 3×ATR（第二目标）+ 移动止损
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
	entryPrice    float64
	stopLoss      float64
	takeProfit1   float64 // 第一止盈位（1.5×ATR）
	takeProfit2   float64 // 第二止盈位（3×ATR）
	entryBarTime  int64   // 入场K线时间
	atr           float64 // 入场时ATR，用于EMA破位缓冲带计算
	hitTP1        bool    // 是否已达到第一止盈位（触发移动止损）
	breakBelowCnt int     // EMA破位确认计数
	breakAboveCnt int     // EMA破位确认计数
}

// TrendFollowingStrategy 多时间周期趋势跟踪策略
type TrendFollowingStrategy struct {
	symbol                   string
	params                   map[string]float64
	producer                 *kafka.Producer
	harvestPathProducer      *kafka.Producer
	harvestPathLSTMPredictor *harvestpathmodel.LSTMPredictor

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
}

type RuntimeOptions struct {
	HarvestPathLSTMPredictor *harvestpathmodel.LSTMPredictor
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
		signalLogDir:             signalLogDir,
		signalLogFiles:           make(map[string]*os.File),
		decisionLogFiles:         make(map[string]*os.File),
	}
}

func runtimeHarvestPathLSTMPredictor(opts *RuntimeOptions) *harvestpathmodel.LSTMPredictor {
	if opts == nil {
		return nil
	}
	return opts.HarvestPathLSTMPredictor
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
	paramSignalMode         = "signal_mode"            // 信号模式: 0=15m(默认) | 1=1m分钟周期
	param1mTradingPaused    = "1m_trading_paused"      // 1m成交暂停: 0=正常(默认) | 1=暂停1m成交，仅用15m+1h+4h判断
	param1mRsiPeriod        = "1m_rsi_period"          // 1m RSI周期（默认14）
	param1mAtrMult          = "1m_atr_multiplier"      // 1m 止损ATR倍数（默认1.5）
	param1mBreakoutLookback = "1m_breakout_lookback"   // 1m 突破回顾期（默认20）
	param1mRsiOverbought    = "1m_rsi_overbought"      // 1m RSI超买阈值（默认70）
	param1mRsiOversold      = "1m_rsi_oversold"        // 1m RSI超卖阈值（默认30）
	param1mRsiBiasLong      = "1m_rsi_bias_long"       // 1m 多头RSI偏置（默认55）
	param1mRsiBiasShort     = "1m_rsi_bias_short"      // 1m 空头RSI偏置（默认45）
	param1mMinHoldingBars   = "1m_min_holding_bars"    // 1m 最小持仓K线数（默认5）
	param1mEmaExitBufferATR = "1m_ema_exit_buffer_atr" // 1m EMA破位缓冲ATR倍数（默认0.3）

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

	// 保存K线快照到对应周期历史
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
		if len(s.klines15m) > 20 {
			s.klines15m = s.klines15m[len(s.klines15m)-20:]
		}
	case "1m":
		s.latest1m = snap
		s.klines1m = append(s.klines1m, snap)
		if len(s.klines1m) > 120 {
			s.klines1m = s.klines1m[len(s.klines1m)-120:]
		}
	default:
		return nil // 忽略其他周期
	}

	// 判断信号模式：0=15m(默认) | 1=1m
	signalMode := int(s.getParam(paramSignalMode, 0))

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
				return s.checkExitConditions(ctx, k)
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
			return s.checkExitConditions1m(ctx, k)
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
		return s.checkExitConditions(ctx, k)
	}

	// 空仓时 → 检查入场条件
	return s.checkEntryConditions(ctx, k)
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

// judge1HPullback 判断1小时回调条件
//
// 多头回调条件：
//   - 价格位置：EMA21 > 价格 > EMA55（健康回调区间）
//   - 结构完整：价格 > 前低点（避免反转）
//   - EMA趋势：EMA21持续上升（避免反转）
//   - RSI过滤：RSI ∈ [42, 60]（合理多头区间）
//
// 空头回调条件：
//   - 价格位置：EMA21 < 价格 < EMA55（健康反弹区间）
//   - 结构完整：价格 < 前高点（避免反转）
//   - EMA趋势：EMA21持续下降（避免反转）
//   - RSI过滤：RSI ∈ [40, 58]（合理空头区间）
func (s *TrendFollowingStrategy) judge1HPullback(trend trendDirection) pullbackResult {
	pullback, _ := s.evaluate1HPullback(trend)
	return pullback
}

func (s *TrendFollowingStrategy) evaluate1HPullback(trend trendDirection) (pullbackResult, map[string]interface{}) {
	h1 := s.latest1h
	extras := map[string]interface{}{
		"trend": stringTrend(trend),
	}
	appendSnapshotExtras(extras, "h1", h1)
	if h1.Ema21 == 0 || h1.Ema55 == 0 || h1.Rsi == 0 {
		extras["price_in_range"] = false
		extras["rsi_ok"] = false
		extras["structure_ok"] = false
		extras["ema_trend_ok"] = false
		return pullbackNone, extras
	}

	rsiLongLow := s.getParam(paramH1RsiLongLow, 42)
	rsiLongHigh := s.getParam(paramH1RsiLongHigh, 60)
	rsiShortLow := s.getParam(paramH1RsiShortLow, 40)
	rsiShortHigh := s.getParam(paramH1RsiShortHigh, 58)

	// 检查 EMA21 是否持续上升/下降（需要至少2根K线）
	emaTrendOk := true
	prevEma21 := 0.0
	if len(s.klines1h) >= 2 {
		prevEma21 = s.klines1h[len(s.klines1h)-2].Ema21
		if trend == trendLong {
			emaTrendOk = h1.Ema21 > prevEma21 // EMA21 持续上升
		} else if trend == trendShort {
			emaTrendOk = h1.Ema21 < prevEma21 // EMA21 持续下降
		}
	}
	extras["ema_trend_ok"] = emaTrendOk
	extras["prev_h1_ema21"] = prevEma21

	if trend == trendLong {
		// 多头回调：EMA21 > 价格 > EMA55
		priceInRange := h1.Ema21 > h1.Close && h1.Close > h1.Ema55
		rsiOk := h1.Rsi >= rsiLongLow && h1.Rsi <= rsiLongHigh

		// 结构完整：价格 > 前低点
		structureOk := true
		prevLow := 0.0
		if len(s.klines1h) >= 2 {
			prevLow = s.klines1h[len(s.klines1h)-2].Low
			structureOk = h1.Close > prevLow
		}
		extras["price_in_range"] = priceInRange
		extras["rsi_ok"] = rsiOk
		extras["structure_ok"] = structureOk
		extras["h1_rsi_low"] = rsiLongLow
		extras["h1_rsi_high"] = rsiLongHigh
		extras["prev_h1_low"] = prevLow

		if priceInRange && rsiOk && structureOk && emaTrendOk {
			return pullbackLong, extras
		}
	} else if trend == trendShort {
		// 空头回调：EMA21 < 价格 < EMA55
		priceInRange := h1.Ema21 < h1.Close && h1.Close < h1.Ema55
		rsiOk := h1.Rsi >= rsiShortLow && h1.Rsi <= rsiShortHigh

		// 结构完整：价格 < 前高点
		structureOk := true
		prevHigh := 0.0
		if len(s.klines1h) >= 2 {
			prevHigh = s.klines1h[len(s.klines1h)-2].High
			structureOk = h1.Close < prevHigh
		}
		extras["price_in_range"] = priceInRange
		extras["rsi_ok"] = rsiOk
		extras["structure_ok"] = structureOk
		extras["h1_rsi_low"] = rsiShortLow
		extras["h1_rsi_high"] = rsiShortHigh
		extras["prev_h1_high"] = prevHigh

		if priceInRange && rsiOk && structureOk && emaTrendOk {
			return pullbackShort, extras
		}
	}

	return pullbackNone, extras
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
				s.writeDecisionLogIfEnabled("entry", "skip", "m15_no_entry", k, map[string]interface{}{
					"trend":    stringTrend(trend),
					"pullback": stringPullback(pullback),
					"m15_rsi":  m15.Rsi,
					"m15_atr":  m15.Atr,
				})
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

		return s.openPosition(ctx, k, trend, pullback, entry, m15)
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
	if s.latest1h.Ema21 == 0 || s.latest1h.Ema55 == 0 {
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

	// 第2层：1H回调确认
	pullback, pullbackExtras := s.evaluate1HPullback(trend)
	if pullback == pullbackNone {
		log.Printf("[策略] %s 1H无回调 | trend=%v EMA21=%.2f EMA55=%.2f 价格=%.2f RSI=%.2f",
			s.symbol, trend, s.latest1h.Ema21, s.latest1h.Ema55, s.latest1h.Close, s.latest1h.Rsi)
		s.writeDecisionLogIfEnabled("entry", "skip", "h1_no_pullback", k, pullbackExtras)
		return nil
	}

	// 第3层：15M入场信号
	entry := s.judge15MEntry(pullback)
	if entry == entryNone {
		log.Printf("[策略] %s 15M无入场 | pullback=%v RSI=%.2f ATR=%.2f",
			s.symbol, pullback, s.latest15m.Rsi, s.latest15m.Atr)
		extras := map[string]interface{}{
			"trend":    stringTrend(trend),
			"pullback": stringPullback(pullback),
		}
		appendSnapshotExtras(extras, "m15", s.latest15m)
		s.writeDecisionLogIfEnabled("entry", "skip", "m15_no_entry", k, extras)
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		extras := map[string]interface{}{
			"trend":    stringTrend(trend),
			"pullback": stringPullback(pullback),
			"entry":    stringEntry(entry),
		}
		appendSnapshotExtras(extras, "h4", s.latest4h)
		appendSnapshotExtras(extras, "h1", s.latest1h)
		appendSnapshotExtras(extras, "m15", s.latest15m)
		s.writeDecisionLogIfEnabled("entry", "skip", "harvest_path_block", k, extras)
		return nil
	}

	return s.openPosition(ctx, k, trend, pullback, entry, s.latest15m)
}

// openPosition 计算止损止盈并发送入场信号
func (s *TrendFollowingStrategy) openPosition(ctx context.Context, k *marketpb.Kline, trend trendDirection, pullback pullbackResult, entry entrySignal, m15 klineSnapshot) error {
	atrMult := s.getParam(paramStopLossATRMult, 1.5)
	stopDist := atrMult * m15.Atr

	var action, side string
	var stopLoss, tp1, tp2 float64

	if entry == entryLong {
		action = "BUY"
		side = "LONG"
		stopLoss = m15.Close - stopDist
		tp1 = m15.Close + stopDist   // 1.5×ATR
		tp2 = m15.Close + 2*stopDist // 3×ATR
	} else {
		action = "SELL"
		side = "SHORT"
		stopLoss = m15.Close + stopDist
		tp1 = m15.Close - stopDist   // 1.5×ATR
		tp2 = m15.Close - 2*stopDist // 3×ATR
	}

	// 仓位计算
	quantity := s.calculatePositionSize(m15.Close, stopLoss)

	// 深回调缩放
	if s.isDeepPullback() {
		scale := s.getParam(paramDeepPullbackScale, 0.9)
		quantity *= scale
	}

	// 构建信号原因
	reason := s.buildEntryReason(trend, pullback, entry, m15, stopLoss, tp1, tp2)
	signalReason := s.newSignalReason(
		reason,
		"OPEN_ENTRY",
		fmt.Sprintf("4H趋势=%s", describeTrendContext(trend)),
		fmt.Sprintf("1H回调=%s | 15M入场确认", describePullbackContext15m(pullback)),
		fmt.Sprintf("RSI=%.2f ATR=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f", m15.Rsi, m15.Atr, stopLoss, tp1, tp2),
		"15m", "trend_following", "open", side,
	)

	// 记录持仓
	s.pos = position{
		side:         sideLong,
		entryPrice:   m15.Close,
		stopLoss:     stopLoss,
		takeProfit1:  tp1,
		takeProfit2:  tp2,
		entryBarTime: m15.OpenTime,
		atr:          m15.Atr,
		hitTP1:       false,
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
		"strategy_id":   fmt.Sprintf("trend-following-%s", s.symbol),
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
			"h4_ema21":  s.latest4h.Ema21,
			"h4_ema55":  s.latest4h.Ema55,
			"h1_ema21":  s.latest1h.Ema21,
			"h1_ema55":  s.latest1h.Ema55,
			"h1_rsi":    s.latest1h.Rsi,
			"m15_ema21": m15.Ema21,
			"m15_rsi":   m15.Rsi,
			"m15_atr":   m15.Atr,
		},
	}

	log.Printf("[策略入场] %s %s | 价格=%.2f | 止损=%.2f | TP1=%.2f | TP2=%.2f | %s",
		s.symbol, action, m15.Close, stopLoss, tp1, tp2, reason)

	extras := map[string]interface{}{
		"action":       action,
		"side":         side,
		"entry_price":  m15.Close,
		"stop_loss":    stopLoss,
		"take_profits": []float64{tp1, tp2},
	}
	appendSnapshotExtras(extras, "h4", s.latest4h)
	appendSnapshotExtras(extras, "h1", s.latest1h)
	appendSnapshotExtras(extras, "m15", m15)
	s.writeDecisionLogIfEnabled("entry", "signal", "open_signal_sent", k, extras)
	return s.sendSignal(ctx, signal, k)
}

// buildEntryReason 构建入场信号原因描述
func (s *TrendFollowingStrategy) buildEntryReason(trend trendDirection, pullback pullbackResult, entry entrySignal, m15 klineSnapshot, stopLoss, tp1, tp2 float64) string {
	return fmt.Sprintf(
		"4H趋势=%s | 1H回调=%s | 15M入场 | RSI=%.2f ATR=%.2f | 止损=%.2f(1.5×ATR) TP1=%.2f TP2=%.2f",
		describeTrendContext(trend), describePullbackContext15m(pullback), m15.Rsi, m15.Atr, stopLoss, tp1, tp2,
	)
}

// ---------------------------------------------------------------------------
// 出场条件检查
// ---------------------------------------------------------------------------

// checkExitConditions 检查出场条件（止损 / 止盈 / 移动止损 / EMA破位）
func (s *TrendFollowingStrategy) checkExitConditions(ctx context.Context, k *marketpb.Kline) error {
	m15 := s.latest15m
	price := m15.Close

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

	var exitAction string
	var exitReason string

	// 1. 止损检查
	if s.pos.side == sideLong && price <= s.pos.stopLoss {
		exitAction = "SELL"
		exitReason = fmt.Sprintf("多头止损：价格%.2f ≤ 止损%.2f", price, s.pos.stopLoss)
	} else if s.pos.side == sideShort && price >= s.pos.stopLoss {
		exitAction = "BUY"
		exitReason = fmt.Sprintf("空头止损：价格%.2f ≥ 止损%.2f", price, s.pos.stopLoss)
	}

	// 2. 止盈检查
	if exitAction == "" {
		if s.pos.side == sideLong {
			if price >= s.pos.takeProfit2 {
				exitAction = "SELL"
				exitReason = fmt.Sprintf("多头止盈2：价格%.2f ≥ TP2%.2f（3×ATR）", price, s.pos.takeProfit2)
			} else if price >= s.pos.takeProfit1 {
				// 达到TP1：移动止损到入场价（保本）
				if !s.pos.hitTP1 {
					s.pos.hitTP1 = true
					s.pos.stopLoss = s.pos.entryPrice
					log.Printf("[策略] %s 多头移动止损→保本：原止损→入场价%.2f", s.symbol, s.pos.entryPrice)
				}
				// TP2 止盈由下一次K线检查
			}
		} else if s.pos.side == sideShort {
			if price <= s.pos.takeProfit2 {
				exitAction = "BUY"
				exitReason = fmt.Sprintf("空头止盈2：价格%.2f ≤ TP2%.2f（3×ATR）", price, s.pos.takeProfit2)
			} else if price <= s.pos.takeProfit1 {
				if !s.pos.hitTP1 {
					s.pos.hitTP1 = true
					s.pos.stopLoss = s.pos.entryPrice
					log.Printf("[策略] %s 空头移动止损→保本：原止损→入场价%.2f", s.symbol, s.pos.entryPrice)
				}
			}
		}
	}

	// 3. EMA破位出场（持仓 ≥ 5根K线后才检查）
	if exitAction == "" && heldBars >= minHoldingBars && m15.Ema21 != 0 {
		buffer := emaBufferATR * s.pos.atr
		if s.pos.side == sideLong {
			// 多头破位：价格 < EMA21 - 缓冲带
			if price < m15.Ema21-buffer {
				s.pos.breakBelowCnt++
				s.pos.breakAboveCnt = 0
				if s.pos.breakBelowCnt >= exitConfirmBars {
					exitAction = "SELL"
					exitReason = fmt.Sprintf("多头EMA破位：价格%.2f < EMA21%.2f - 缓冲%.2f（连续%d根确认）",
						price, m15.Ema21, buffer, s.pos.breakBelowCnt)
				}
			} else {
				s.pos.breakBelowCnt = 0
			}
		} else if s.pos.side == sideShort {
			// 空头破位：价格 > EMA21 + 缓冲带
			if price > m15.Ema21+buffer {
				s.pos.breakAboveCnt++
				s.pos.breakBelowCnt = 0
				if s.pos.breakAboveCnt >= exitConfirmBars {
					exitAction = "BUY"
					exitReason = fmt.Sprintf("空头EMA破位：价格%.2f > EMA21%.2f + 缓冲%.2f（连续%d根确认）",
						price, m15.Ema21, buffer, s.pos.breakAboveCnt)
				}
			} else {
				s.pos.breakAboveCnt = 0
			}
		}
	}

	if exitAction == "" {
		// 无出场信号，记录持仓状态
		log.Printf("[策略] %s 持仓中 | 方向=%v 价格=%.2f 止损=%.2f TP1=%.2f TP2=%.2f 持仓K线=%d",
			s.symbol, s.pos.side, price, s.pos.stopLoss, s.pos.takeProfit1, s.pos.takeProfit2, heldBars)
		return nil
	}

	// 计算盈亏
	pnl := s.calculatePnL(price)

	// 更新风控状态
	s.updateRiskState(pnl)

	// 构建出场信号
	side := "LONG"
	if s.pos.side == sideShort {
		side = "SHORT"
	}

	signal := map[string]interface{}{
		"strategy_id":  fmt.Sprintf("trend-following-%s", s.symbol),
		"symbol":       s.symbol,
		"interval":     "15m",
		"action":       exitAction,
		"side":         side,
		"signal_type":  "CLOSE",
		"quantity":     0.01, // 出场用相同数量
		"entry_price":  price,
		"stop_loss":    s.pos.stopLoss,
		"take_profits": []float64{s.pos.takeProfit1, s.pos.takeProfit2},
		"reason": s.newSignalReason(
			exitReason,
			"CLOSE_EXIT",
			fmt.Sprintf("持仓方向=%s | 周期=15m", side),
			exitReason,
			fmt.Sprintf("平仓价=%.2f | 已实现盈亏=%.2f | 止损=%.2f", price, pnl, s.pos.stopLoss),
			"15m", "trend_following", "close", side,
		).Summary,
		"signal_reason": s.newSignalReason(
			exitReason,
			"CLOSE_EXIT",
			fmt.Sprintf("持仓方向=%s | 周期=15m", side),
			exitReason,
			fmt.Sprintf("平仓价=%.2f | 已实现盈亏=%.2f | 止损=%.2f", price, pnl, s.pos.stopLoss),
			"15m", "trend_following", "close", side,
		),
		"timestamp":   time.Now().UnixMilli(),
		"atr":         m15.Atr,
		"risk_reward": 0, // 出场信号无需风险收益比
		"indicators": map[string]interface{}{
			"m15_ema21": m15.Ema21,
			"m15_rsi":   m15.Rsi,
			"m15_atr":   m15.Atr,
			"pnl":       pnl,
		},
	}

	// 清除持仓
	s.pos = position{}

	log.Printf("[策略出场] %s %s | 价格=%.2f | 盈亏=%.2f | %s",
		s.symbol, exitAction, price, pnl, exitReason)

	return s.sendSignal(ctx, signal, k)
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
	indicators, _ := signal["indicators"].(map[string]interface{})

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

	log.Printf("[策略信号] %s %s %s | 方向=%s | 价格=%.2f | 数量=%.4f | 止损=%.2f | 止盈=%s | %s | %s",
		s.symbol, "15m", action, side, entryPrice, quantity, stopLoss, tpStr, indicatorsStr, reason)

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
		"h4_ema21", "h4_ema55",
		"h1_ema21", "h1_ema55", "h1_rsi",
		"m15_ema21", "m15_rsi", "m15_atr",
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
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		valJSON, err := json.Marshal(o.values[key])
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
	Summary          string   `json:"summary"`
	Phase            string   `json:"phase"`
	TrendContext     string   `json:"trend_context,omitempty"`
	SetupContext     string   `json:"setup_context,omitempty"`
	PathContext      string   `json:"path_context,omitempty"`
	ExecutionContext string   `json:"execution_context,omitempty"`
	Tags             []string `json:"tags,omitempty"`
}

func (s *TrendFollowingStrategy) newSignalReason(summary, phase, trendContext, setupContext, executionContext string, tags ...string) signalReasonPayload {
	return signalReasonPayload{
		Summary:          summary,
		Phase:            phase,
		TrendContext:     trendContext,
		SetupContext:     setupContext,
		PathContext:      s.harvestPathReasonContext(),
		ExecutionContext: executionContext,
		Tags:             tags,
	}
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

func describePullbackContext15m(pullback pullbackResult) string {
	switch pullback {
	case pullbackLong:
		return "多头回调（EMA21>价格>EMA55，RSI∈[42,60]）"
	case pullbackShort:
		return "空头回调（EMA21<价格<EMA55，RSI∈[40,58]）"
	default:
		return "无"
	}
}

func describePullbackContext1m(pullback pullbackResult) string {
	switch pullback {
	case pullbackLong:
		return "多头回调"
	case pullbackShort:
		return "空头回调"
	default:
		return "无"
	}
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
}

// round2 四舍五入保留2位小数
func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

type decisionLogEntry struct {
	Timestamp   string                 `json:"timestamp"`
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
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		valJSON, err := json.Marshal(o.values[key])
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
		"action", "side", "entry_price", "stop_loss", "take_profits",
		"trend", "pullback", "entry",
		"price_in_range", "rsi_ok", "structure_ok", "ema_trend_ok",
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
	return strings.Join(parts, " -> ")
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
	case !isFinal && !isTradable:
		status = "blocked"
	}

	if dirtyReason != "" && dirtyReason != "clean" {
		return fmt.Sprintf("%s=%s(%s)", prefix, status, dirtyReason)
	}
	return fmt.Sprintf("%s=%s", prefix, status)
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

	orderedExtras := newOrderedDecisionExtras(extras)
	entry := decisionLogEntry{
		Timestamp:   formatDecisionLogTime(now),
		Symbol:      s.symbol,
		Interval:    k.Interval,
		Stage:       stage,
		Decision:    decision,
		Reason:      translateDecisionReason(reason),
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
}

func translateDecisionReason(reason string) string {
	switch reason {
	case "risk_limits_block":
		return "风控限制阻止开仓"
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
		return "1小时没有满足回调条件"
	case "m15_no_entry":
		return "15分钟没有满足入场条件"
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

		return s.openPosition1m(ctx, k, trend, pullback, entry, m1)
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

	// 第2层：1H回调确认
	if s.latest1h.Ema21 == 0 || s.latest1h.Ema55 == 0 {
		log.Printf("[策略1m] %s 1H指标未就绪，跳过入场", s.symbol)
		return nil
	}
	pullback := s.judge1HPullback(trend)
	if pullback == pullbackNone {
		return nil
	}

	// 第3层：1m入场信号
	entry := s.judge1MEntry(pullback)
	if entry == entryNone {
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		return nil
	}

	return s.openPosition1m(ctx, k, trend, pullback, entry, m1)
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

// openPosition1m 1m模式开仓：计算止损止盈并发送入场信号
func (s *TrendFollowingStrategy) openPosition1m(ctx context.Context, k *marketpb.Kline, trend trendDirection, pullback pullbackResult, entry entrySignal, m1 klineSnapshot) error {
	atrMult := s.getParam(param1mAtrMult, 1.5)
	stopDist := atrMult * m1.Atr

	var action, side string
	var stopLoss, tp1, tp2 float64

	if entry == entryLong {
		action = "BUY"
		side = "LONG"
		stopLoss = m1.Close - stopDist
		tp1 = m1.Close + stopDist   // 1.5×ATR
		tp2 = m1.Close + 2*stopDist // 3×ATR
	} else {
		action = "SELL"
		side = "SHORT"
		stopLoss = m1.Close + stopDist
		tp1 = m1.Close - stopDist   // 1.5×ATR
		tp2 = m1.Close - 2*stopDist // 3×ATR
	}

	// 仓位计算
	quantity := s.calculatePositionSize(m1.Close, stopLoss)

	// 深回调缩放
	if s.isDeepPullback() {
		scale := s.getParam(paramDeepPullbackScale, 0.9)
		quantity *= scale
	}

	// 构建入场原因
	reason := s.buildEntryReason1m(trend, pullback, entry, m1, stopLoss, tp1, tp2)
	signalReason := s.newSignalReason(
		reason,
		"OPEN_ENTRY",
		fmt.Sprintf("4H趋势=%s", describeTrendContext(trend)),
		fmt.Sprintf("1H回调=%s | 1M入场确认", describePullbackContext1m(pullback)),
		fmt.Sprintf("RSI=%.2f ATR=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f", m1.Rsi, m1.Atr, stopLoss, tp1, tp2),
		"1m", "trend_following", "open", side,
	)

	// 记录持仓
	s.pos = position{
		side:         sideLong,
		entryPrice:   m1.Close,
		stopLoss:     stopLoss,
		takeProfit1:  tp1,
		takeProfit2:  tp2,
		entryBarTime: m1.OpenTime,
		atr:          m1.Atr,
		hitTP1:       false,
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
		"strategy_id":   fmt.Sprintf("trend-following-1m-%s", s.symbol),
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
			"h4_ema21":  s.latest4h.Ema21,
			"h4_ema55":  s.latest4h.Ema55,
			"h1_ema21":  s.latest1h.Ema21,
			"h1_ema55":  s.latest1h.Ema55,
			"h1_rsi":    s.latest1h.Rsi,
			"m15_ema21": s.latest15m.Ema21,
			"m15_rsi":   s.latest15m.Rsi,
			"m15_atr":   s.latest15m.Atr,
			"m1_ema21":  m1.Ema21,
			"m1_rsi":    m1.Rsi,
			"m1_atr":    m1.Atr,
		},
	}

	log.Printf("[策略1m入场] %s %s | 价格=%.2f | 止损=%.2f | TP1=%.2f | TP2=%.2f | %s",
		s.symbol, action, m1.Close, stopLoss, tp1, tp2, reason)

	return s.sendSignal(ctx, signal, k)
}

// buildEntryReason1m 构建1m入场信号原因描述
func (s *TrendFollowingStrategy) buildEntryReason1m(trend trendDirection, pullback pullbackResult, entry entrySignal, m1 klineSnapshot, stopLoss, tp1, tp2 float64) string {
	return fmt.Sprintf(
		"[1m] 4H趋势=%s | 1H回调=%s | 1M入场 | RSI=%.2f ATR=%.2f | 止损=%.2f(1.5×ATR) TP1=%.2f TP2=%.2f",
		describeTrendContext(trend), describePullbackContext1m(pullback), m1.Rsi, m1.Atr, stopLoss, tp1, tp2,
	)
}

// checkExitConditions1m 1m模式出场条件检查
//
// 出场优先级：
//  1. 止损：价格穿越止损价
//  2. 止盈：TP2全平 / TP1移动止损到保本
//  3. EMA破位：持仓≥N根1m K线后，价格穿越EMA21-缓冲带
func (s *TrendFollowingStrategy) checkExitConditions1m(ctx context.Context, k *marketpb.Kline) error {
	m1 := s.latest1m
	price := m1.Close

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

	var exitAction string
	var exitReason string

	// 1. 止损检查
	if s.pos.side == sideLong && price <= s.pos.stopLoss {
		exitAction = "SELL"
		exitReason = fmt.Sprintf("[1m] 多头止损：价格%.2f ≤ 止损%.2f", price, s.pos.stopLoss)
	} else if s.pos.side == sideShort && price >= s.pos.stopLoss {
		exitAction = "BUY"
		exitReason = fmt.Sprintf("[1m] 空头止损：价格%.2f ≥ 止损%.2f", price, s.pos.stopLoss)
	}

	// 2. 止盈检查
	if exitAction == "" {
		if s.pos.side == sideLong {
			if price >= s.pos.takeProfit2 {
				exitAction = "SELL"
				exitReason = fmt.Sprintf("[1m] 多头止盈2：价格%.2f ≥ TP2%.2f（3×ATR）", price, s.pos.takeProfit2)
			} else if price >= s.pos.takeProfit1 {
				if !s.pos.hitTP1 {
					s.pos.hitTP1 = true
					s.pos.stopLoss = s.pos.entryPrice
					log.Printf("[策略1m] %s 多头移动止损→保本：原止损→入场价%.2f", s.symbol, s.pos.entryPrice)
				}
			}
		} else if s.pos.side == sideShort {
			if price <= s.pos.takeProfit2 {
				exitAction = "BUY"
				exitReason = fmt.Sprintf("[1m] 空头止盈2：价格%.2f ≤ TP2%.2f（3×ATR）", price, s.pos.takeProfit2)
			} else if price <= s.pos.takeProfit1 {
				if !s.pos.hitTP1 {
					s.pos.hitTP1 = true
					s.pos.stopLoss = s.pos.entryPrice
					log.Printf("[策略1m] %s 空头移动止损→保本：原止损→入场价%.2f", s.symbol, s.pos.entryPrice)
				}
			}
		}
	}

	// 3. EMA破位出场（持仓 >= minHoldingBars 根1m K线后才检查）
	if exitAction == "" && heldBars >= minHoldingBars && m1.Ema21 != 0 {
		buffer := emaBufferATR * s.pos.atr
		if s.pos.side == sideLong {
			if price < m1.Ema21-buffer {
				s.pos.breakBelowCnt++
				s.pos.breakAboveCnt = 0
				if s.pos.breakBelowCnt >= exitConfirmBars {
					exitAction = "SELL"
					exitReason = fmt.Sprintf("[1m] 多头EMA破位：价格%.2f < EMA21%.2f - 缓冲%.2f（连续%d根确认）",
						price, m1.Ema21, buffer, s.pos.breakBelowCnt)
				}
			} else {
				s.pos.breakBelowCnt = 0
			}
		} else if s.pos.side == sideShort {
			if price > m1.Ema21+buffer {
				s.pos.breakAboveCnt++
				s.pos.breakBelowCnt = 0
				if s.pos.breakAboveCnt >= exitConfirmBars {
					exitAction = "BUY"
					exitReason = fmt.Sprintf("[1m] 空头EMA破位：价格%.2f > EMA21%.2f + 缓冲%.2f（连续%d根确认）",
						price, m1.Ema21, buffer, s.pos.breakAboveCnt)
				}
			} else {
				s.pos.breakAboveCnt = 0
			}
		}
	}

	if exitAction == "" {
		// 无出场信号，记录持仓状态
		log.Printf("[策略1m] %s 持仓中 | 方向=%v 价格=%.2f 止损=%.2f TP1=%.2f TP2=%.2f 持仓K线=%d",
			s.symbol, s.pos.side, price, s.pos.stopLoss, s.pos.takeProfit1, s.pos.takeProfit2, heldBars)
		return nil
	}

	// 计算盈亏
	pnl := s.calculatePnL(price)

	// 更新风控状态
	s.updateRiskState(pnl)

	// 构建出场信号
	side := "LONG"
	if s.pos.side == sideShort {
		side = "SHORT"
	}

	signal := map[string]interface{}{
		"strategy_id":  fmt.Sprintf("trend-following-1m-%s", s.symbol),
		"symbol":       s.symbol,
		"interval":     "1m",
		"action":       exitAction,
		"side":         side,
		"signal_type":  "CLOSE",
		"quantity":     0.01,
		"entry_price":  price,
		"stop_loss":    s.pos.stopLoss,
		"take_profits": []float64{s.pos.takeProfit1, s.pos.takeProfit2},
		"reason": s.newSignalReason(
			exitReason,
			"CLOSE_EXIT",
			fmt.Sprintf("持仓方向=%s | 周期=1m", side),
			exitReason,
			fmt.Sprintf("平仓价=%.2f | 已实现盈亏=%.2f | 止损=%.2f", price, pnl, s.pos.stopLoss),
			"1m", "trend_following", "close", side,
		).Summary,
		"signal_reason": s.newSignalReason(
			exitReason,
			"CLOSE_EXIT",
			fmt.Sprintf("持仓方向=%s | 周期=1m", side),
			exitReason,
			fmt.Sprintf("平仓价=%.2f | 已实现盈亏=%.2f | 止损=%.2f", price, pnl, s.pos.stopLoss),
			"1m", "trend_following", "close", side,
		),
		"timestamp":   time.Now().UnixMilli(),
		"atr":         m1.Atr,
		"risk_reward": 0,
		"indicators": map[string]interface{}{
			"m1_ema21": m1.Ema21,
			"m1_rsi":   m1.Rsi,
			"m1_atr":   m1.Atr,
			"pnl":      pnl,
		},
	}

	// 清除持仓
	s.pos = position{}

	log.Printf("[策略1m出场] %s %s | 价格=%.2f | 盈亏=%.2f | %s",
		s.symbol, exitAction, price, pnl, exitReason)

	return s.sendSignal(ctx, signal, k)
}
