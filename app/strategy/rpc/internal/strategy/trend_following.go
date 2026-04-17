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
	"sync"
	"time"

	"exchange-system/app/strategy/rpc/internal/kafka"
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
	OpenTime int64
	Open     float64
	High     float64
	Low      float64
	Close    float64
	Ema21    float64
	Ema55    float64
	Rsi      float64
	Atr      float64
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
	symbol   string
	params   map[string]float64
	producer *kafka.Producer

	// mu 保护以下字段的并发访问
	mu sync.Mutex

	// 多时间周期K线历史（仅保存最近N根，用于结构突破等需要OHLC的场景）
	// 指标直接从 Kline 消息读取，不需要历史来计算
	klines4h  []klineSnapshot
	klines1h  []klineSnapshot
	klines15m []klineSnapshot
	klines1m  []klineSnapshot // 1m K线历史（1m信号模式下使用）

	// 最新指标快照（每个周期只需保留最新的，由 OnKline 更新）
	latest4h  klineSnapshot
	latest1h  klineSnapshot
	latest15m klineSnapshot
	latest1m  klineSnapshot // 最新1m K线快照

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
}

// ---------------------------------------------------------------------------
// 构造函数
// ---------------------------------------------------------------------------

// NewTrendFollowingStrategy 创建多时间周期趋势跟踪策略实例
func NewTrendFollowingStrategy(symbol string, params map[string]float64, producer *kafka.Producer, signalLogDir string) *TrendFollowingStrategy {
	if params == nil {
		params = map[string]float64{}
	}
	return &TrendFollowingStrategy{
		symbol:         symbol,
		params:         params,
		producer:       producer,
		signalLogDir:   signalLogDir,
		signalLogFiles: make(map[string]*os.File),
	}
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
	param1mRsiPeriod        = "1m_rsi_period"          // 1m RSI周期（默认14）
	param1mAtrMult          = "1m_atr_multiplier"      // 1m 止损ATR倍数（默认1.5）
	param1mBreakoutLookback = "1m_breakout_lookback"   // 1m 突破回顾期（默认20）
	param1mRsiOverbought    = "1m_rsi_overbought"      // 1m RSI超买阈值（默认70）
	param1mRsiOversold      = "1m_rsi_oversold"        // 1m RSI超卖阈值（默认30）
	param1mRsiBiasLong      = "1m_rsi_bias_long"       // 1m 多头RSI偏置（默认55）
	param1mRsiBiasShort     = "1m_rsi_bias_short"      // 1m 空头RSI偏置（默认45）
	param1mMinHoldingBars   = "1m_min_holding_bars"    // 1m 最小持仓K线数（默认5）
	param1mEmaExitBufferATR = "1m_ema_exit_buffer_atr" // 1m EMA破位缓冲ATR倍数（默认0.3）
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
		OpenTime: k.OpenTime,
		Open:     k.Open,
		High:     k.High,
		Low:      k.Low,
		Close:    k.Close,
		Ema21:    k.Ema21,
		Ema55:    k.Ema55,
		Rsi:      k.Rsi,
		Atr:      k.Atr,
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
	h1 := s.latest1h
	if h1.Ema21 == 0 || h1.Ema55 == 0 || h1.Rsi == 0 {
		return pullbackNone
	}

	rsiLongLow := s.getParam(paramH1RsiLongLow, 42)
	rsiLongHigh := s.getParam(paramH1RsiLongHigh, 60)
	rsiShortLow := s.getParam(paramH1RsiShortLow, 40)
	rsiShortHigh := s.getParam(paramH1RsiShortHigh, 58)

	// 检查 EMA21 是否持续上升/下降（需要至少2根K线）
	emaTrendOk := true
	if len(s.klines1h) >= 2 {
		prevEma21 := s.klines1h[len(s.klines1h)-2].Ema21
		if trend == trendLong {
			emaTrendOk = h1.Ema21 > prevEma21 // EMA21 持续上升
		} else if trend == trendShort {
			emaTrendOk = h1.Ema21 < prevEma21 // EMA21 持续下降
		}
	}

	if trend == trendLong {
		// 多头回调：EMA21 > 价格 > EMA55
		priceInRange := h1.Ema21 > h1.Close && h1.Close > h1.Ema55
		rsiOk := h1.Rsi >= rsiLongLow && h1.Rsi <= rsiLongHigh

		// 结构完整：价格 > 前低点
		structureOk := true
		if len(s.klines1h) >= 2 {
			prevLow := s.klines1h[len(s.klines1h)-2].Low
			structureOk = h1.Close > prevLow
		}

		if priceInRange && rsiOk && structureOk && emaTrendOk {
			return pullbackLong
		}
	} else if trend == trendShort {
		// 空头回调：EMA21 < 价格 < EMA55
		priceInRange := h1.Ema21 < h1.Close && h1.Close < h1.Ema55
		rsiOk := h1.Rsi >= rsiShortLow && h1.Rsi <= rsiShortHigh

		// 结构完整：价格 < 前高点
		structureOk := true
		if len(s.klines1h) >= 2 {
			prevHigh := s.klines1h[len(s.klines1h)-2].High
			structureOk = h1.Close < prevHigh
		}

		if priceInRange && rsiOk && structureOk && emaTrendOk {
			return pullbackShort
		}
	}

	return pullbackNone
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
				return nil
			}
		}

		return s.openPosition(ctx, k, trend, pullback, entry, m15)
	}

	// ========== 以下为正常三层过滤逻辑 ==========

	// 第1层：4H趋势判断
	// 检查 4H/1H 指标是否就绪（需要先收到对应周期的闭K线）
	if s.latest4h.Ema21 == 0 || s.latest4h.Ema55 == 0 {
		log.Printf("[策略] %s 4H指标未就绪（等待4H闭K线），跳过入场", s.symbol)
		return nil
	}
	if s.latest1h.Ema21 == 0 || s.latest1h.Ema55 == 0 {
		log.Printf("[策略] %s 1H指标未就绪（等待1H闭K线），跳过入场", s.symbol)
		return nil
	}

	trend := s.judge4HTrend()
	if trend == trendNone {
		log.Printf("[策略] %s 4H震荡/无趋势 | EMA21=%.2f EMA55=%.2f 价格=%.2f",
			s.symbol, s.latest4h.Ema21, s.latest4h.Ema55, s.latest4h.Close)
		return nil
	}

	// 第2层：1H回调确认
	pullback := s.judge1HPullback(trend)
	if pullback == pullbackNone {
		log.Printf("[策略] %s 1H无回调 | trend=%v EMA21=%.2f EMA55=%.2f 价格=%.2f RSI=%.2f",
			s.symbol, trend, s.latest1h.Ema21, s.latest1h.Ema55, s.latest1h.Close, s.latest1h.Rsi)
		return nil
	}

	// 第3层：15M入场信号
	entry := s.judge15MEntry(pullback)
	if entry == entryNone {
		log.Printf("[策略] %s 15M无入场 | pullback=%v RSI=%.2f ATR=%.2f",
			s.symbol, pullback, s.latest15m.Rsi, s.latest15m.Atr)
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
		"strategy_id":  fmt.Sprintf("trend-following-%s", s.symbol),
		"symbol":       s.symbol,
		"interval":     "15m",
		"action":       action,
		"side":         side,
		"signal_type":  "OPEN",
		"quantity":     quantity,
		"entry_price":  m15.Close,
		"stop_loss":    stopLoss,
		"take_profits": []float64{tp1, tp2},
		"reason":       reason,
		"timestamp":    time.Now().UnixMilli(),
		"atr":          m15.Atr,
		"risk_reward":  riskReward,
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

	return s.sendSignal(ctx, signal, k)
}

// buildEntryReason 构建入场信号原因描述
func (s *TrendFollowingStrategy) buildEntryReason(trend trendDirection, pullback pullbackResult, entry entrySignal, m15 klineSnapshot, stopLoss, tp1, tp2 float64) string {
	trendStr := "无"
	if trend == trendLong {
		trendStr = "多头（价格>EMA21>EMA55）"
	} else if trend == trendShort {
		trendStr = "空头（价格<EMA21<EMA55）"
	}

	pullbackStr := "无"
	if pullback == pullbackLong {
		pullbackStr = "多头回调（EMA21>价格>EMA55，RSI∈[42,60]）"
	} else if pullback == pullbackShort {
		pullbackStr = "空头回调（EMA21<价格<EMA55，RSI∈[40,58]）"
	}

	return fmt.Sprintf(
		"4H趋势=%s | 1H回调=%s | 15M入场 | RSI=%.2f ATR=%.2f | 止损=%.2f(1.5×ATR) TP1=%.2f TP2=%.2f",
		trendStr, pullbackStr, m15.Rsi, m15.Atr, stopLoss, tp1, tp2,
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
		"reason":       exitReason,
		"timestamp":    time.Now().UnixMilli(),
		"atr":          m15.Atr,
		"risk_reward":  0, // 出场信号无需风险收益比
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
	Timestamp   string                 `json:"timestamp"`
	StrategyID  string                 `json:"strategyId"`
	Symbol      string                 `json:"symbol"`
	Interval    string                 `json:"interval"`
	Action      string                 `json:"action"`
	Side        string                 `json:"side"`
	EntryPrice  float64                `json:"entryPrice"`
	Quantity    float64                `json:"quantity"`
	StopLoss    float64                `json:"stopLoss"`
	TakeProfits []float64              `json:"takeProfits"`
	Reason      string                 `json:"reason"`
	Indicators  map[string]interface{} `json:"indicators"`
	IsTradable  bool                   `json:"isTradable"`
	IsFinal     bool                   `json:"isFinal"`
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
	interval, _ := signal["interval"].(string)
	indicators, _ := signal["indicators"].(map[string]interface{})
	strategyID, _ := signal["strategy_id"].(string)

	var takeProfits []float64
	if tp, ok := signal["take_profits"].([]float64); ok {
		takeProfits = tp
	}

	entry := signalLogEntry{
		Timestamp:   now.Format("2006-01-02T15:04:05.000Z"),
		StrategyID:  strategyID,
		Symbol:      s.symbol,
		Interval:    interval,
		Action:      action,
		Side:        side,
		EntryPrice:  entryPrice,
		Quantity:    quantity,
		StopLoss:    stopLoss,
		TakeProfits: takeProfits,
		Reason:      reason,
		Indicators:  indicators,
		IsTradable:  k.IsTradable,
		IsFinal:     k.IsFinal,
	}

	// 数值保留2位小数
	entry.Quantity = round2(entry.Quantity)
	entry.EntryPrice = round2(entry.EntryPrice)
	entry.StopLoss = round2(entry.StopLoss)
	for i, tp := range entry.TakeProfits {
		entry.TakeProfits[i] = round2(tp)
	}
	if entry.Indicators != nil {
		rounded := make(map[string]interface{}, len(entry.Indicators))
		for k, v := range entry.Indicators {
			if f, ok := v.(float64); ok {
				rounded[k] = round2(f)
			} else {
				rounded[k] = v
			}
		}
		entry.Indicators = rounded
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

	return s.openPosition1m(ctx, k, trend, pullback, entry, m1)
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
		"strategy_id":  fmt.Sprintf("trend-following-1m-%s", s.symbol),
		"symbol":       s.symbol,
		"interval":     "1m",
		"action":       action,
		"side":         side,
		"signal_type":  "OPEN",
		"quantity":     quantity,
		"entry_price":  m1.Close,
		"stop_loss":    stopLoss,
		"take_profits": []float64{tp1, tp2},
		"reason":       reason,
		"timestamp":    time.Now().UnixMilli(),
		"atr":          m1.Atr,
		"risk_reward":  riskReward,
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
	trendStr := "无"
	if trend == trendLong {
		trendStr = "多头（价格>EMA21>EMA55）"
	} else if trend == trendShort {
		trendStr = "空头（价格<EMA21<EMA55）"
	}

	pullbackStr := "无"
	if pullback == pullbackLong {
		pullbackStr = "多头回调"
	} else if pullback == pullbackShort {
		pullbackStr = "空头回调"
	}

	return fmt.Sprintf(
		"[1m] 4H趋势=%s | 1H回调=%s | 1M入场 | RSI=%.2f ATR=%.2f | 止损=%.2f(1.5×ATR) TP1=%.2f TP2=%.2f",
		trendStr, pullbackStr, m1.Rsi, m1.Atr, stopLoss, tp1, tp2,
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
		"reason":       exitReason,
		"timestamp":    time.Now().UnixMilli(),
		"atr":          m1.Atr,
		"risk_reward":  0,
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
