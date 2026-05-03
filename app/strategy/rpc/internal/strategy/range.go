package strategy

import (
	"context"
	"fmt"
	"math"
	"time"

	marketpb "exchange-system/common/pb/market"
)

// rangeStats 汇总一次震荡判定和当前周期入场判定所需的关键统计量。
type rangeStats struct {
	RangeHigh             float64
	RangeLow              float64
	RangeMid              float64
	RangeWidth            float64
	EntryBuffer           float64
	NearRangeLow          bool
	NearRangeHigh         bool
	LongRSIOk             bool
	ShortRSIOk            bool
	LongRSITurnOk         bool
	ShortRSITurnOk        bool
	Lookback              int
	PrevRsi               float64
	H4Adx                 float64
	H4AdxOk               bool
	H4AtrFalling          bool
	H4EmaCloseness        float64
	H4EmaClosenessOk      bool
	H4Score               int
	H4OscillationOK       bool
	H1Adx                 float64
	H1AdxOk               bool
	H1BollMid             float64
	H1BollUpper           float64
	H1BollLower           float64
	H1BollWidthPct        float64
	H1BollWidthOk         bool
	H1BollWidthMinOk      bool
	H1RangeOK             bool
	H1Zone                string
	H1PositionPct         float64
	H1CandleLongOk        bool
	H1CandleShortOk       bool
	H1Rsi                 float64
	H1RsiLongOk           bool
	H1RsiShortOk          bool
	H4TrendLongOk         bool
	H4TrendShortOk        bool
	M15BollUpper          float64
	M15BollLower          float64
	M15BollWidth          float64
	M15BollWidthPct       float64
	LongHasRSIBounce      bool
	ShortHasRSIBounce     bool
	LongHasRSIOversold    bool
	ShortHasRSIOverbought bool
	LongHasBBBounce       bool
	ShortHasBBBounce      bool
	LongHasPinBar         bool
	ShortHasPinBar        bool
	LongHasEngulfing      bool
	ShortHasEngulfing     bool
	LongSignalCount       int
	ShortSignalCount      int
	FakeBreakoutLong      bool
	FakeBreakoutShort     bool
	RiskReward            float64
	StopMethod            string
	TakeProfitMethod      string
}

// checkRangeEntryConditions 执行 15M 震荡变体开仓判定。
func (s *TrendFollowingStrategy) checkRangeEntryConditions(ctx context.Context, k *marketpb.Kline) error {
	if !s.checkRiskLimits() {
		s.writeDecisionLogIfEnabled("entry", "skip", "risk_limits_block", k, nil)
		return nil
	}
	m15 := s.latest15m
	if m15.Atr == 0 || m15.Rsi == 0 || len(s.klines1h) == 0 || len(s.klines4h) < 2 {
		s.writeDecisionLogIfEnabled("entry", "skip", "range_m15_not_ready", k, s.rangeNotReadyExtras("m15", m15))
		return nil
	}
	entry, stats := s.judgeRangeEntry(m15, s.klines15m, int(s.getParam(paramRangeLookback, 8)))
	if entry == entryNone {
		s.writeDecisionLogIfEnabled("entry", "skip", "range_no_entry", k, s.rangeDecisionExtras("m15", m15, stats))
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		extras := s.rangeDecisionExtras("m15", m15, stats)
		extras["entry"] = stringEntry(entry)
		s.writeDecisionLogIfEnabled("entry", "skip", "harvest_path_block", k, extras)
		return nil
	}
	return s.openRangePosition(ctx, k, "15m", entry, m15, stats)
}

// checkRangeEntryConditions1m 在 1m 模式下沿用同一套震荡过滤，但以 1m 作为触发周期。
func (s *TrendFollowingStrategy) checkRangeEntryConditions1m(ctx context.Context, k *marketpb.Kline) error {
	if !s.checkRiskLimits() {
		s.writeDecisionLogIfEnabled("entry", "skip", "risk_limits_block", k, nil)
		return nil
	}
	m1 := s.latest1m
	if m1.Atr == 0 || m1.Rsi == 0 || len(s.klines1h) == 0 || len(s.klines4h) < 2 {
		s.writeDecisionLogIfEnabled("entry", "skip", "range_1m_not_ready", k, s.rangeNotReadyExtras("m1", m1))
		return nil
	}
	entry, stats := s.judgeRangeEntry(m1, s.klines1m, int(s.getParam(paramRangeLookback, 8)))
	if entry == entryNone {
		s.writeDecisionLogIfEnabled("entry", "skip", "range_no_entry", k, s.rangeDecisionExtras("m1", m1, stats))
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		extras := s.rangeDecisionExtras("m1", m1, stats)
		extras["entry"] = stringEntry(entry)
		s.writeDecisionLogIfEnabled("entry", "skip", "harvest_path_block", k, extras)
		return nil
	}
	return s.openRangePosition(ctx, k, "1m", entry, m1, stats)
}

// judgeRangeEntry 使用 “4H 判震荡 + 1H 定位区间 + 当前周期多信号聚合” 的统一规则做开仓判断。
func (s *TrendFollowingStrategy) judgeRangeEntry(current klineSnapshot, history []klineSnapshot, lookback int) (entrySignal, rangeStats) {
	stats := rangeStats{Lookback: lookback}
	if current.Atr <= 0 || current.Rsi <= 0 || len(history) <= 1 {
		return entryNone, stats
	}
	h4Stats, ok := s.evaluateH4Oscillation()
	if !ok {
		return entryNone, stats
	}
	stats.H4Adx = h4Stats.H4Adx
	stats.H4AdxOk = h4Stats.H4AdxOk
	stats.H4AtrFalling = h4Stats.H4AtrFalling
	stats.H4EmaCloseness = h4Stats.H4EmaCloseness
	stats.H4EmaClosenessOk = h4Stats.H4EmaClosenessOk
	stats.H4Score = h4Stats.H4Score
	stats.H4OscillationOK = h4Stats.H4OscillationOK
	if !stats.H4OscillationOK {
		return entryNone, stats
	}

	h1Stats, ok := s.evaluateH1RangeContext(current.Close)
	if !ok {
		return entryNone, stats
	}
	stats.H1Adx = h1Stats.H1Adx
	stats.H1AdxOk = h1Stats.H1AdxOk
	stats.H1BollMid = h1Stats.H1BollMid
	stats.H1BollUpper = h1Stats.H1BollUpper
	stats.H1BollLower = h1Stats.H1BollLower
	stats.H1BollWidthPct = h1Stats.H1BollWidthPct
	stats.H1BollWidthOk = h1Stats.H1BollWidthOk
	stats.H1BollWidthMinOk = h1Stats.H1BollWidthMinOk
	stats.H1RangeOK = h1Stats.H1RangeOK
	stats.H1Zone = h1Stats.H1Zone
	stats.H1PositionPct = h1Stats.H1PositionPct
	stats.H1CandleLongOk = h1Stats.H1CandleLongOk
	stats.H1CandleShortOk = h1Stats.H1CandleShortOk
	stats.H1Rsi = h1Stats.H1Rsi
	stats.H1RsiLongOk = h1Stats.H1RsiLongOk
	stats.H1RsiShortOk = h1Stats.H1RsiShortOk
	stats.H4TrendLongOk = h1Stats.H4TrendLongOk
	stats.H4TrendShortOk = h1Stats.H4TrendShortOk
	if !stats.H1RangeOK {
		return entryNone, stats
	}

	entryStats, ok := s.evaluateRangeEntrySignals(current, history, lookback)
	if !ok {
		return entryNone, stats
	}
	stats.RangeHigh = entryStats.RangeHigh
	stats.RangeLow = entryStats.RangeLow
	stats.RangeMid = entryStats.RangeMid
	stats.RangeWidth = entryStats.RangeWidth
	stats.EntryBuffer = entryStats.EntryBuffer
	stats.NearRangeLow = entryStats.NearRangeLow
	stats.NearRangeHigh = entryStats.NearRangeHigh
	stats.LongRSIOk = entryStats.LongRSIOk
	stats.ShortRSIOk = entryStats.ShortRSIOk
	stats.LongRSITurnOk = entryStats.LongRSITurnOk
	stats.ShortRSITurnOk = entryStats.ShortRSITurnOk
	stats.Lookback = entryStats.Lookback
	stats.PrevRsi = entryStats.PrevRsi
	stats.M15BollUpper = entryStats.M15BollUpper
	stats.M15BollLower = entryStats.M15BollLower
	stats.M15BollWidth = entryStats.M15BollWidth
	stats.M15BollWidthPct = entryStats.M15BollWidthPct
	stats.LongHasRSIBounce = entryStats.LongHasRSIBounce
	stats.ShortHasRSIBounce = entryStats.ShortHasRSIBounce
	stats.LongHasRSIOversold = entryStats.LongHasRSIOversold
	stats.ShortHasRSIOverbought = entryStats.ShortHasRSIOverbought
	stats.LongHasBBBounce = entryStats.LongHasBBBounce
	stats.ShortHasBBBounce = entryStats.ShortHasBBBounce
	stats.LongHasPinBar = entryStats.LongHasPinBar
	stats.ShortHasPinBar = entryStats.ShortHasPinBar
	stats.LongHasEngulfing = entryStats.LongHasEngulfing
	stats.ShortHasEngulfing = entryStats.ShortHasEngulfing
	stats.LongSignalCount = entryStats.LongSignalCount
	stats.ShortSignalCount = entryStats.ShortSignalCount

	if s.shouldEnterRangeLong(stats) {
		if stats.H1Zone == "breakout_down" {
			stats.FakeBreakoutLong = true
		}
		return entryLong, stats
	}
	if s.shouldEnterRangeShort(stats) {
		if stats.H1Zone == "breakout_up" {
			stats.FakeBreakoutShort = true
		}
		return entryShort, stats
	}
	return entryNone, stats
}

// evaluateH4Oscillation 根据 4H ADX、EMA 接近度和 ATR 是否回落计算震荡评分。
func (s *TrendFollowingStrategy) evaluateH4Oscillation() (rangeStats, bool) {
	stats := rangeStats{}
	adxPeriod := int(s.getParam(paramRangeH4AdxPeriod, 14))
	if adxPeriod <= 1 {
		adxPeriod = 14
	}
	if len(s.klines4h) < maxInt(adxPeriod*2, 2) {
		return stats, false
	}
	adx, ok := calculateADX(s.klines4h, adxPeriod)
	if !ok {
		return stats, false
	}
	latest := s.latest4h
	prev := s.klines4h[len(s.klines4h)-2]
	if latest.Ema55 <= 0 || latest.Atr <= 0 || prev.Atr <= 0 {
		return stats, false
	}
	stats.H4Adx = adx
	stats.H4AdxOk = adx < s.getParam(paramRangeH4AdxMax, 20)
	stats.H4EmaCloseness = math.Abs(latest.Ema21-latest.Ema55) / latest.Ema55
	stats.H4EmaClosenessOk = stats.H4EmaCloseness <= s.getParam(paramRangeH4EmaClosenessMax, 0.005)
	stats.H4AtrFalling = latest.Atr < prev.Atr
	if stats.H4AdxOk {
		stats.H4Score++
	}
	if stats.H4EmaClosenessOk {
		stats.H4Score++
	}
	if stats.H4AtrFalling {
		stats.H4Score++
	}
	stats.H4OscillationOK = float64(stats.H4Score) >= s.getParam(paramRangeH4ScoreMin, 2)
	return stats, true
}

// evaluateH1RangeContext 判断 1H 是否处于震荡区间，并计算当前价格相对 1H 区间的位置。
func (s *TrendFollowingStrategy) evaluateH1RangeContext(price float64) (rangeStats, bool) {
	stats := rangeStats{}
	adxPeriod := int(s.getParam(paramRangeH1AdxPeriod, 14))
	bollPeriod := int(s.getParam(paramRangeH1BollPeriod, 20))
	if adxPeriod <= 1 {
		adxPeriod = 14
	}
	if bollPeriod <= 1 {
		bollPeriod = 20
	}
	adx, ok := calculateADX(s.klines1h, adxPeriod)
	if !ok {
		return stats, false
	}
	mid, upper, lower, ok := calculateBollinger(s.klines1h, bollPeriod, s.getParam(paramRangeH1BollStdDev, 2))
	if !ok || mid <= 0 || upper <= lower {
		return stats, false
	}
	stats.H1Adx = adx
	stats.H1AdxOk = adx < s.getParam(paramRangeH1AdxMax, 20)
	stats.H1BollMid = mid
	stats.H1BollUpper = upper
	stats.H1BollLower = lower
	stats.H1BollWidthPct = (upper - lower) / mid
	stats.H1BollWidthOk = stats.H1BollWidthPct <= s.getParam(paramRangeH1BollWidthMaxPct, 0.05)
	stats.H1BollWidthMinOk = stats.H1BollWidthPct >= s.getParam(paramRangeMinH1BollWidthPct, 0)
	stats.H1RangeOK = stats.H1AdxOk && stats.H1BollWidthOk
	stats.H1PositionPct = 0.5
	if width := upper - lower; width > 0 {
		stats.H1PositionPct = (price - lower) / width
	}
	proximity := s.getParam(paramRangeZoneProximity, 0.2)
	switch {
	case price < lower:
		stats.H1Zone = "breakout_down"
	case price > upper:
		stats.H1Zone = "breakout_up"
	case stats.H1PositionPct < proximity:
		stats.H1Zone = "lower"
	case stats.H1PositionPct > 1-proximity:
		stats.H1Zone = "upper"
	default:
		stats.H1Zone = "middle"
	}
	stats.H1CandleLongOk = s.latest1h.Close >= s.latest1h.Open
	stats.H1CandleShortOk = s.latest1h.Close <= s.latest1h.Open
	stats.H1Rsi = s.latest1h.Rsi
	stats.H1RsiLongOk = s.rangeH1RsiFilterOk(entryLong, stats.H1Rsi)
	stats.H1RsiShortOk = s.rangeH1RsiFilterOk(entryShort, stats.H1Rsi)
	stats.H4TrendLongOk = s.rangeH4TrendFilterOk(entryLong)
	stats.H4TrendShortOk = s.rangeH4TrendFilterOk(entryShort)
	return stats, true
}

// evaluateRangeEntrySignals 计算当前周期的 Boll 触边、RSI、K 线形态等信号聚合结果。
func (s *TrendFollowingStrategy) evaluateRangeEntrySignals(current klineSnapshot, history []klineSnapshot, lookback int) (rangeStats, bool) {
	stats := rangeStats{Lookback: lookback}
	period := int(s.getParam(paramRangeM15BollPeriod, 20))
	if period <= 1 {
		period = 20
	}
	if len(history) < maxInt(period, 2) {
		return stats, false
	}
	stats.Lookback = lookback
	mid, upper, lower, ok := calculateBollinger(history, period, s.getParam(paramRangeM15BollStdDev, 2.5))
	if !ok || upper <= lower {
		return stats, false
	}
	stats.RangeHigh = upper
	stats.RangeLow = lower
	stats.RangeMid = mid
	stats.RangeWidth = upper - lower
	stats.M15BollUpper = upper
	stats.M15BollLower = lower
	stats.M15BollWidth = stats.RangeWidth
	if mid > 0 {
		stats.M15BollWidthPct = stats.RangeWidth / mid
	}
	stats.EntryBuffer = stats.RangeWidth * s.getParam(paramRangeM15BollTouchBuffer, 0.05)
	stats.NearRangeLow = current.Low <= lower+stats.EntryBuffer || current.Close <= lower+stats.EntryBuffer
	stats.NearRangeHigh = current.High >= upper-stats.EntryBuffer || current.Close >= upper-stats.EntryBuffer

	prev := history[len(history)-2]
	stats.PrevRsi = prev.Rsi
	longOversoldMax := s.getParam(paramRangeM15RsiLongMax, 30)
	shortOverboughtMin := s.getParam(paramRangeM15RsiShortMin, 70)
	longConfirm := s.getParam(paramRangeM15RsiConfirmLong, 32)
	shortConfirm := s.getParam(paramRangeM15RsiConfirmShort, 68)
	turnMin := s.getParam(paramRangeM15RsiTurnMin, 0.5)

	stats.LongHasRSIOversold = current.Rsi <= longOversoldMax
	stats.ShortHasRSIOverbought = current.Rsi >= shortOverboughtMin
	stats.LongHasRSIBounce = prev.Rsi > 0 && prev.Rsi < longOversoldMax && current.Rsi >= longConfirm
	stats.ShortHasRSIBounce = prev.Rsi > 0 && prev.Rsi > shortOverboughtMin && current.Rsi <= shortConfirm
	stats.LongRSITurnOk = prev.Rsi > 0 && current.Rsi >= prev.Rsi+turnMin
	stats.ShortRSITurnOk = prev.Rsi > 0 && current.Rsi <= prev.Rsi-turnMin
	stats.LongRSIOk = stats.LongHasRSIOversold || stats.LongHasRSIBounce
	stats.ShortRSIOk = stats.ShortHasRSIOverbought || stats.ShortHasRSIBounce

	stats.LongHasBBBounce, stats.ShortHasBBBounce = s.rangeBBBounceSignals(history, lower, upper, stats.EntryBuffer)
	stats.LongHasPinBar, stats.ShortHasPinBar = s.rangePinBarSignals(current)
	stats.LongHasEngulfing, stats.ShortHasEngulfing = s.rangeEngulfingSignals(history)
	stats.LongSignalCount = countTrue(
		stats.LongHasRSIBounce,
		stats.LongHasBBBounce,
		stats.LongHasPinBar,
		stats.LongHasEngulfing,
		stats.LongHasRSIOversold,
	)
	stats.ShortSignalCount = countTrue(
		stats.ShortHasRSIBounce,
		stats.ShortHasBBBounce,
		stats.ShortHasPinBar,
		stats.ShortHasEngulfing,
		stats.ShortHasRSIOverbought,
	)
	return stats, true
}

// shouldEnterRangeLong 根据 1H 区间位置、信号聚合数量和过滤开关决定是否做多。
func (s *TrendFollowingStrategy) shouldEnterRangeLong(stats rangeStats) bool {
	if !stats.H4OscillationOK || !stats.H1RangeOK {
		return false
	}
	if !stats.H1BollWidthMinOk || !stats.H1RsiLongOk || !stats.H4TrendLongOk {
		return false
	}
	if s.getParam(paramRangeH1CandleFilter, 1) > 0 && !stats.H1CandleLongOk {
		return false
	}
	if !stats.LongRSIOk || !stats.LongRSITurnOk {
		return false
	}
	if s.getParam(paramRangeRequireBBBounce, 1) > 0 && !stats.LongHasBBBounce {
		return false
	}
	if stats.LongSignalCount < int(s.getParam(paramRangeSignalMinCount, 2)) {
		return false
	}
	if stats.H1Zone == "lower" && stats.NearRangeLow {
		return true
	}
	return s.getParam(paramRangeFakeBreakout, 1) > 0 && stats.H1Zone == "breakout_down" && stats.LongHasBBBounce
}

// shouldEnterRangeShort 根据 1H 区间位置、信号聚合数量和过滤开关决定是否做空。
func (s *TrendFollowingStrategy) shouldEnterRangeShort(stats rangeStats) bool {
	if !stats.H4OscillationOK || !stats.H1RangeOK {
		return false
	}
	if !stats.H1BollWidthMinOk || !stats.H1RsiShortOk || !stats.H4TrendShortOk {
		return false
	}
	if s.getParam(paramRangeH1CandleFilter, 1) > 0 && !stats.H1CandleShortOk {
		return false
	}
	if !stats.ShortRSIOk || !stats.ShortRSITurnOk {
		return false
	}
	if s.getParam(paramRangeRequireBBBounce, 1) > 0 && !stats.ShortHasBBBounce {
		return false
	}
	if stats.ShortSignalCount < int(s.getParam(paramRangeSignalMinCount, 2)) {
		return false
	}
	if stats.H1Zone == "upper" && stats.NearRangeHigh {
		return true
	}
	return s.getParam(paramRangeFakeBreakout, 1) > 0 && stats.H1Zone == "breakout_up" && stats.ShortHasBBBounce
}

// rangeH4TrendFilterOk 在开启 4H 趋势过滤时，要求方向与 4H EMA 结构一致；若 4H 仍高度收敛，则允许双向。
func (s *TrendFollowingStrategy) rangeH4TrendFilterOk(entry entrySignal) bool {
	if s.getParam(paramRangeUseH4TrendFilter, 0) <= 0 {
		return true
	}
	if s.latest4h.Ema55 <= 0 {
		return false
	}
	closeness := math.Abs(s.latest4h.Ema21-s.latest4h.Ema55) / s.latest4h.Ema55
	if closeness <= s.getParam(paramRangeH4EmaClosenessMax, 0.005) {
		return true
	}
	if entry == entryLong {
		return s.latest4h.Ema21 >= s.latest4h.Ema55
	}
	return s.latest4h.Ema21 <= s.latest4h.Ema55
}

// rangeH1RsiFilterOk 在开启 1H RSI 过滤时，限制做多不要追到高位、做空不要追到低位。
func (s *TrendFollowingStrategy) rangeH1RsiFilterOk(entry entrySignal, h1Rsi float64) bool {
	if s.getParam(paramRangeUseH1RsiFilter, 0) <= 0 || h1Rsi <= 0 {
		return true
	}
	if entry == entryLong {
		limit := s.getParam(paramRangeH1RsiLongMax, 0)
		return limit <= 0 || h1Rsi < limit
	}
	limit := s.getParam(paramRangeH1RsiShortMin, 0)
	return limit <= 0 || h1Rsi > limit
}

// rangeBBBounceSignals 判断最近窗口是否出现过触及 Boll 边界后重新收回轨道内的反弹信号。
func (s *TrendFollowingStrategy) rangeBBBounceSignals(history []klineSnapshot, lower, upper, entryBuffer float64) (bool, bool) {
	if len(history) == 0 {
		return false, false
	}
	windowSize := int(s.getParam(paramRangeLookback, 8))
	if windowSize <= 0 {
		windowSize = 8
	}
	start := len(history) - windowSize
	if start < 0 {
		start = 0
	}
	touchedLower := false
	touchedUpper := false
	for _, item := range history[start:] {
		if item.Low <= lower+entryBuffer {
			touchedLower = true
		}
		if item.High >= upper-entryBuffer {
			touchedUpper = true
		}
	}
	current := history[len(history)-1]
	return touchedLower && current.Close > lower, touchedUpper && current.Close < upper
}

// rangePinBarSignals 判断当前 K 线是否形成适合震荡反转的 Pin Bar 形态。
func (s *TrendFollowingStrategy) rangePinBarSignals(current klineSnapshot) (bool, bool) {
	if s.getParam(paramRangeUsePinBar, 1) <= 0 {
		return false, false
	}
	body := math.Abs(current.Close - current.Open)
	if body < 0.01 {
		body = 0.01
	}
	lowerShadow := math.Min(current.Open, current.Close) - current.Low
	upperShadow := current.High - math.Max(current.Open, current.Close)
	ratio := s.getParam(paramRangePinBarRatio, 2)
	longPin := lowerShadow >= ratio*body && upperShadow < body
	shortPin := upperShadow >= ratio*body && lowerShadow < body
	return longPin, shortPin
}

// rangeEngulfingSignals 判断当前 K 线是否形成吞没形态，作为震荡区间边界的反转确认。
func (s *TrendFollowingStrategy) rangeEngulfingSignals(history []klineSnapshot) (bool, bool) {
	if s.getParam(paramRangeUseEngulfing, 1) <= 0 || len(history) < 2 {
		return false, false
	}
	prev := history[len(history)-2]
	current := history[len(history)-1]
	longEngulfing := prev.Close < prev.Open &&
		current.Close > current.Open &&
		current.Close > prev.Open &&
		current.Open < prev.Close
	shortEngulfing := prev.Close > prev.Open &&
		current.Close < current.Open &&
		current.Close < prev.Open &&
		current.Open > prev.Close
	return longEngulfing, shortEngulfing
}

// rangeNotReadyExtras 输出震荡策略未就绪时的缺项诊断，便于快速判断是高周期历史不足还是触发周期指标缺失。
func (s *TrendFollowingStrategy) rangeNotReadyExtras(prefix string, snap klineSnapshot) map[string]interface{} {
	extras := map[string]interface{}{
		"missing_h4_history": len(s.klines4h) < 2,
		"missing_h1_history": len(s.klines1h) == 0,
		"missing_range_atr":  snap.Atr == 0,
		"missing_range_rsi":  snap.Rsi == 0,
		"h4_history_len":     len(s.klines4h),
		"h1_history_len":     len(s.klines1h),
	}
	switch prefix {
	case "m15":
		extras["missing_m15_atr"] = snap.Atr == 0
		extras["missing_m15_rsi"] = snap.Rsi == 0
	case "m1":
		extras["missing_m1_atr"] = snap.Atr == 0
		extras["missing_m1_rsi"] = snap.Rsi == 0
	}
	appendSnapshotExtras(extras, prefix, snap)
	appendSnapshotExtras(extras, "h4", s.latest4h)
	appendSnapshotExtras(extras, "h1", s.latest1h)
	return extras
}

// calculateBollinger 计算指定窗口的布林中轨、上轨和下轨。
func calculateBollinger(history []klineSnapshot, period int, stdDev float64) (float64, float64, float64, bool) {
	if period <= 1 || len(history) < period {
		return 0, 0, 0, false
	}
	window := history[len(history)-period:]
	sum := 0.0
	for _, item := range window {
		sum += item.Close
	}
	mid := sum / float64(period)
	if mid <= 0 {
		return 0, 0, 0, false
	}
	variance := 0.0
	for _, item := range window {
		diff := item.Close - mid
		variance += diff * diff
	}
	std := math.Sqrt(variance / float64(period))
	offset := stdDev * std
	return mid, mid + offset, mid - offset, true
}

// calculateADX 基于最近窗口的 TR/+DM/-DM 计算 ADX 近似值，用于识别趋势强弱。
func calculateADX(history []klineSnapshot, period int) (float64, bool) {
	if period <= 1 || len(history) < period*2 {
		return 0, false
	}
	trs := make([]float64, 0, len(history)-1)
	plusDMs := make([]float64, 0, len(history)-1)
	minusDMs := make([]float64, 0, len(history)-1)
	for i := 1; i < len(history); i++ {
		cur := history[i]
		prev := history[i-1]
		upMove := cur.High - prev.High
		downMove := prev.Low - cur.Low
		plusDM := 0.0
		if upMove > downMove && upMove > 0 {
			plusDM = upMove
		}
		minusDM := 0.0
		if downMove > upMove && downMove > 0 {
			minusDM = downMove
		}
		tr := math.Max(cur.High-cur.Low, math.Max(math.Abs(cur.High-prev.Close), math.Abs(cur.Low-prev.Close)))
		trs = append(trs, tr)
		plusDMs = append(plusDMs, plusDM)
		minusDMs = append(minusDMs, minusDM)
	}
	if len(trs) < period {
		return 0, false
	}
	dxs := make([]float64, 0, len(trs)-period+1)
	for end := period; end <= len(trs); end++ {
		start := end - period
		sumTR := 0.0
		sumPlusDM := 0.0
		sumMinusDM := 0.0
		for i := start; i < end; i++ {
			sumTR += trs[i]
			sumPlusDM += plusDMs[i]
			sumMinusDM += minusDMs[i]
		}
		if sumTR <= 0 {
			continue
		}
		plusDI := 100 * sumPlusDM / sumTR
		minusDI := 100 * sumMinusDM / sumTR
		denominator := plusDI + minusDI
		if denominator <= 0 {
			continue
		}
		dxs = append(dxs, 100*math.Abs(plusDI-minusDI)/denominator)
	}
	if len(dxs) < period {
		return 0, false
	}
	sumDX := 0.0
	for _, dx := range dxs[len(dxs)-period:] {
		sumDX += dx
	}
	return sumDX / float64(period), true
}

// rangeDecisionExtras 组装震荡策略的结构化日志上下文，便于直接看到 1H 与当前周期的判定细节。
func (s *TrendFollowingStrategy) rangeDecisionExtras(prefix string, snap klineSnapshot, stats rangeStats) map[string]interface{} {
	extras := map[string]interface{}{
		"range_h4_adx":               stats.H4Adx,
		"range_h4_adx_ok":            stats.H4AdxOk,
		"range_h4_atr_falling":       stats.H4AtrFalling,
		"range_h4_ema_closeness":     stats.H4EmaCloseness,
		"range_h4_ema_closeness_ok":  stats.H4EmaClosenessOk,
		"range_h4_score":             stats.H4Score,
		"range_h4_oscillation_ok":    stats.H4OscillationOK,
		"range_high":                 stats.RangeHigh,
		"range_low":                  stats.RangeLow,
		"range_mid":                  stats.RangeMid,
		"range_width":                stats.RangeWidth,
		"range_entry_buffer":         stats.EntryBuffer,
		"range_near_low":             stats.NearRangeLow,
		"range_near_high":            stats.NearRangeHigh,
		"range_long_rsi_ok":          stats.LongRSIOk,
		"range_short_rsi_ok":         stats.ShortRSIOk,
		"range_long_rsi_turn_ok":     stats.LongRSITurnOk,
		"range_short_rsi_turn_ok":    stats.ShortRSITurnOk,
		"range_prev_rsi":             stats.PrevRsi,
		"range_lookback":             stats.Lookback,
		"range_h1_adx":               stats.H1Adx,
		"range_h1_adx_ok":            stats.H1AdxOk,
		"range_h1_zone":              stats.H1Zone,
		"range_h1_position_pct":      stats.H1PositionPct,
		"range_h1_candle_long_ok":    stats.H1CandleLongOk,
		"range_h1_candle_short_ok":   stats.H1CandleShortOk,
		"range_h1_rsi":               stats.H1Rsi,
		"range_h1_rsi_long_ok":       stats.H1RsiLongOk,
		"range_h1_rsi_short_ok":      stats.H1RsiShortOk,
		"range_h1_boll_width_pct":    stats.H1BollWidthPct,
		"range_h1_boll_width_ok":     stats.H1BollWidthOk,
		"range_h1_boll_width_min_ok": stats.H1BollWidthMinOk,
		"range_h1_regime_ok":         stats.H1RangeOK,
		"range_h4_trend_long_ok":     stats.H4TrendLongOk,
		"range_h4_trend_short_ok":    stats.H4TrendShortOk,
		"range_m15_boll_upper":       stats.M15BollUpper,
		"range_m15_boll_lower":       stats.M15BollLower,
		"range_m15_boll_width":       stats.M15BollWidth,
		"range_m15_boll_width_pct":   stats.M15BollWidthPct,
		"range_long_rsi_bounce":      stats.LongHasRSIBounce,
		"range_short_rsi_bounce":     stats.ShortHasRSIBounce,
		"range_long_rsi_oversold":    stats.LongHasRSIOversold,
		"range_short_rsi_overbought": stats.ShortHasRSIOverbought,
		"range_long_bb_bounce":       stats.LongHasBBBounce,
		"range_short_bb_bounce":      stats.ShortHasBBBounce,
		"range_long_pin_bar":         stats.LongHasPinBar,
		"range_short_pin_bar":        stats.ShortHasPinBar,
		"range_long_engulfing":       stats.LongHasEngulfing,
		"range_short_engulfing":      stats.ShortHasEngulfing,
		"range_long_signal_count":    stats.LongSignalCount,
		"range_short_signal_count":   stats.ShortSignalCount,
		"range_fake_breakout_long":   stats.FakeBreakoutLong,
		"range_fake_breakout_short":  stats.FakeBreakoutShort,
		"range_risk_reward":          stats.RiskReward,
		"range_stop_method":          stats.StopMethod,
		"range_take_profit_method":   stats.TakeProfitMethod,
	}
	appendSnapshotExtras(extras, "h4", s.latest4h)
	appendSnapshotExtras(extras, "h1", s.latest1h)
	appendSnapshotExtras(extras, prefix, snap)
	return extras
}

// rangeTradePlan 汇总震荡策略一次开仓所需的止损、止盈和风险收益比。
type rangeTradePlan struct {
	StopLoss         float64
	TakeProfit1      float64
	TakeProfit2      float64
	RiskReward       float64
	StopMethod       string
	TakeProfitMethod string
}

// rangeExitDecision 描述当前这根 K 线是否触发震荡持仓管理动作。
type rangeExitDecision struct {
	Action          string
	Reason          string
	SignalType      string
	Quantity        float64
	Partial         bool
	ExitReasonKind  string
	ExitReasonLabel string
}

// buildRangeTradePlan 按文档中的 SL/TP 模式生成震荡策略开仓计划。
func (s *TrendFollowingStrategy) buildRangeTradePlan(entry entrySignal, snap klineSnapshot, stats rangeStats) (rangeTradePlan, bool) {
	plan := rangeTradePlan{
		StopMethod:       s.rangeStopMethodName(),
		TakeProfitMethod: s.rangeTakeProfitMethodName(),
	}
	stopAtr := snap.Atr
	if s.latest1h.Atr > 0 {
		stopAtr = s.latest1h.Atr
	}
	stopOffset := s.getParam(paramRangeStopAtrOffset, 0.3)
	switch int(s.getParam(paramRangeStopMethod, 0)) {
	case 1:
		plan.StopMethod = "m15bb"
		if entry == entryLong {
			plan.StopLoss = stats.M15BollLower - stopOffset*snap.Atr
		} else {
			plan.StopLoss = stats.M15BollUpper + stopOffset*snap.Atr
		}
	default:
		plan.StopMethod = "h1bb"
		if entry == entryLong {
			plan.StopLoss = stats.H1BollLower - stopOffset*stopAtr
		} else {
			plan.StopLoss = stats.H1BollUpper + stopOffset*stopAtr
		}
	}

	tpAtrOffset := s.getParam(paramRangeTakeProfitAtrOff, 0)
	switch int(s.getParam(paramRangeTakeProfitMethod, 0)) {
	case 1:
		plan.TakeProfitMethod = "mid"
		plan.TakeProfit1 = stats.H1BollMid
		plan.TakeProfit2 = stats.H1BollMid
	case 2:
		plan.TakeProfitMethod = "mid_atr"
		if entry == entryLong {
			plan.TakeProfit1 = stats.H1BollMid + tpAtrOffset*stopAtr
		} else {
			plan.TakeProfit1 = stats.H1BollMid - tpAtrOffset*stopAtr
		}
		plan.TakeProfit2 = plan.TakeProfit1
	case 3:
		plan.TakeProfitMethod = "rsi"
	case 4:
		plan.TakeProfitMethod = "split"
		plan.TakeProfit1 = stats.H1BollMid
		if entry == entryLong {
			plan.TakeProfit2 = stats.H1BollUpper
		} else {
			plan.TakeProfit2 = stats.H1BollLower
		}
	default:
		plan.TakeProfitMethod = "band"
		if entry == entryLong {
			plan.TakeProfit1 = stats.H1BollUpper
			plan.TakeProfit2 = stats.H1BollUpper
		} else {
			plan.TakeProfit1 = stats.H1BollLower
			plan.TakeProfit2 = stats.H1BollLower
		}
	}

	target := plan.primaryTarget()
	stopDist := math.Abs(snap.Close - plan.StopLoss)
	if target > 0 && stopDist > 0 {
		plan.RiskReward = math.Abs(target-snap.Close) / stopDist
	}
	if plan.StopLoss == 0 || stopDist <= 0 {
		return plan, false
	}
	minRR := s.getParam(paramRangeMinRR, 0)
	if minRR > 0 && target > 0 && plan.RiskReward < minRR {
		return plan, false
	}
	return plan, true
}

// primaryTarget 返回震荡策略当前持仓管理应优先参考的目标价。
func (p rangeTradePlan) primaryTarget() float64 {
	if p.TakeProfit2 != 0 {
		return p.TakeProfit2
	}
	return p.TakeProfit1
}

// rangeStopMethodName 把数值化止损模式转换为便于日志阅读的名字。
func (s *TrendFollowingStrategy) rangeStopMethodName() string {
	if int(s.getParam(paramRangeStopMethod, 0)) == 1 {
		return "m15bb"
	}
	return "h1bb"
}

// rangeTakeProfitMethodName 把数值化止盈模式转换为便于日志阅读的名字。
func (s *TrendFollowingStrategy) rangeTakeProfitMethodName() string {
	switch int(s.getParam(paramRangeTakeProfitMethod, 0)) {
	case 1:
		return "mid"
	case 2:
		return "mid_atr"
	case 3:
		return "rsi"
	case 4:
		return "split"
	default:
		return "band"
	}
}

// openRangePosition 复用现有风控和信号发送链路，并按震荡策略专用 SL/TP 模式生成开仓信号。
func (s *TrendFollowingStrategy) openRangePosition(ctx context.Context, k *marketpb.Kline, interval string, entry entrySignal, snap klineSnapshot, stats rangeStats) error {
	action := "BUY"
	side := "LONG"
	if entry == entryShort {
		action = "SELL"
		side = "SHORT"
	}

	plan, ok := s.buildRangeTradePlan(entry, snap, stats)
	if !ok {
		extras := s.rangeDecisionExtras(interval, snap, stats)
		extras["range_stop_method"] = plan.StopMethod
		extras["range_take_profit_method"] = plan.TakeProfitMethod
		extras["range_risk_reward"] = plan.RiskReward
		extras["range_min_rr"] = s.getParam(paramRangeMinRR, 0)
		s.writeDecisionLogIfEnabled("entry", "skip", "range_rr_too_low", k, extras)
		return nil
	}
	stats.RiskReward = plan.RiskReward
	stats.StopMethod = plan.StopMethod
	stats.TakeProfitMethod = plan.TakeProfitMethod

	quantity := s.calculatePositionSize(snap.Close, plan.StopLoss)
	adjustedQuantity, blocked, blockReason := s.applyWeightRecommendation(quantity)
	if blocked {
		extras := s.rangeDecisionExtras(interval, snap, stats)
		extras["base_quantity"] = quantity
		extras["reason"] = blockReason
		s.writeDecisionLogIfEnabled("entry", "skip", blockReason, k, extras)
		return nil
	}
	quantity = adjustedQuantity

	reason := s.buildRangeEntryReason(interval, entry, snap, plan.StopLoss, plan.TakeProfit1, plan.TakeProfit2, stats)
	signalReason := s.newSignalReason(
		reason,
		"OPEN_ENTRY",
		fmt.Sprintf("4H震荡=%t | 1H区间=%s | Range=%s", stats.H4OscillationOK, stats.H1Zone, stringEntry(entry)),
		fmt.Sprintf("%s 区间边界反转%s", interval, rangeFakeBreakoutLabel(stats, entry)),
		fmt.Sprintf("RSI=%.2f ATR=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f RR=%.2f", snap.Rsi, snap.Atr, plan.StopLoss, plan.TakeProfit1, plan.TakeProfit2, plan.RiskReward),
		interval, s.strategySignalTag(), "open", side,
	)
	signalReason.Range = signalReasonRangePayloadFromStats(stats)

	s.pos = position{
		side:          sideLong,
		entryPrice:    snap.Close,
		quantity:      quantity,
		stopLoss:      plan.StopLoss,
		takeProfit1:   plan.TakeProfit1,
		takeProfit2:   plan.TakeProfit2,
		entryBarTime:  snap.OpenTime,
		atr:           snap.Atr,
		hitTP1:        false,
		partialClosed: false,
		maxProfit:     0,
	}
	if entry == entryShort {
		s.pos.side = sideShort
	}
	indicators := map[string]interface{}{
		intervalKey(interval, "ema21"): snap.Ema21,
		intervalKey(interval, "rsi"):   snap.Rsi,
		intervalKey(interval, "atr"):   snap.Atr,
	}
	signal := map[string]interface{}{
		"strategy_id":   s.strategySignalID(interval),
		"symbol":        s.symbol,
		"interval":      interval,
		"action":        action,
		"side":          side,
		"signal_type":   "OPEN",
		"quantity":      quantity,
		"entry_price":   snap.Close,
		"stop_loss":     plan.StopLoss,
		"take_profits":  []float64{plan.TakeProfit1, plan.TakeProfit2},
		"reason":        signalReason.Summary,
		"signal_reason": signalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           snap.Atr,
		"risk_reward":   plan.RiskReward,
		"indicators":    indicators,
	}

	extras := s.rangeDecisionExtras(interval, snap, stats)
	extras["action"] = action
	extras["side"] = side
	extras["entry_price"] = snap.Close
	extras["stop_loss"] = plan.StopLoss
	extras["take_profits"] = []float64{plan.TakeProfit1, plan.TakeProfit2}
	s.writeDecisionLogIfEnabled("entry", "signal", "open_signal_sent", k, extras)
	return s.sendSignal(ctx, signal, k)
}

// checkRangeExitConditions 处理 15m 震荡策略持仓管理，包括保本、阶梯追踪、时间止损和动态止盈。
func (s *TrendFollowingStrategy) checkRangeExitConditions(ctx context.Context, k *marketpb.Kline) error {
	return s.checkRangeExitConditionsForInterval(ctx, k, "15m", s.latest15m, s.klines15m)
}

// checkRangeExitConditions1m 处理 1m 模式下的震荡策略持仓管理，规则与 15m 模式一致。
func (s *TrendFollowingStrategy) checkRangeExitConditions1m(ctx context.Context, k *marketpb.Kline) error {
	return s.checkRangeExitConditionsForInterval(ctx, k, "1m", s.latest1m, s.klines1m)
}

// checkRangeExitConditionsForInterval 执行震荡持仓管理的统一流程，并在需要时发送平仓信号。
func (s *TrendFollowingStrategy) checkRangeExitConditionsForInterval(ctx context.Context, k *marketpb.Kline, interval string, snap klineSnapshot, history []klineSnapshot) error {
	decision := s.evaluateRangeExitDecision(interval, snap, history)
	if decision.Action == "" {
		return nil
	}
	pnl := s.calculatePnL(snap.Close)

	side := "LONG"
	if s.pos.side == sideShort {
		side = "SHORT"
	}
	signalType := decision.SignalType
	if signalType == "" {
		signalType = "CLOSE"
	}
	quantity := decision.Quantity
	if quantity <= 0 {
		quantity = s.pos.quantity
	}
	signalReason := s.newSignalReason(
		decision.Reason,
		"CLOSE_EXIT",
		fmt.Sprintf("持仓方向=%s | 周期=%s | TP方法=%s | 信号=%s", side, interval, s.rangeTakeProfitMethodName(), signalType),
		decision.Reason,
		fmt.Sprintf("平仓价=%.2f | 预计盈亏=%.2f | 止损=%.2f | 数量=%.4f", snap.Close, pnl, s.pos.stopLoss, quantity),
		interval, s.strategySignalTag(), "close", side,
	)
	signalReason = signalReason.withExitReason(decision.ExitReasonKind, decision.ExitReasonLabel)
	signal := map[string]interface{}{
		"strategy_id":   s.strategySignalID(interval),
		"symbol":        s.symbol,
		"interval":      interval,
		"action":        decision.Action,
		"side":          side,
		"signal_type":   signalType,
		"quantity":      quantity,
		"entry_price":   snap.Close,
		"stop_loss":     s.pos.stopLoss,
		"take_profits":  []float64{s.pos.takeProfit1, s.pos.takeProfit2},
		"reason":        signalReason.Summary,
		"signal_reason": signalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           snap.Atr,
		"risk_reward":   0,
		"indicators": map[string]interface{}{
			intervalKey(interval, "ema21"): snap.Ema21,
			intervalKey(interval, "rsi"):   snap.Rsi,
			intervalKey(interval, "atr"):   snap.Atr,
			"pnl":                          pnl,
		},
	}
	if decision.Partial {
		s.pos.quantity = math.Max(0, s.pos.quantity-quantity)
		s.pos.partialClosed = true
		s.pos.hitTP1 = true
		if s.pos.side == sideLong {
			s.pos.stopLoss = math.Max(s.pos.stopLoss, s.pos.entryPrice)
		} else {
			s.pos.stopLoss = math.Min(s.pos.stopLoss, s.pos.entryPrice)
		}
	} else {
		s.updateRiskState(pnl)
		s.pos = position{}
	}
	return s.sendSignal(ctx, signal, k)
}

// evaluateRangeExitDecision 根据当前持仓状态返回震荡策略这一根K线应执行的管理动作。
func (s *TrendFollowingStrategy) evaluateRangeExitDecision(interval string, snap klineSnapshot, history []klineSnapshot) rangeExitDecision {
	if s.pos.side == sideNone {
		return rangeExitDecision{}
	}
	heldBars := rangeHeldBars(history, s.pos.entryBarTime)
	s.updateRangeMaxProfit(snap.Close)
	s.applyRangeTrailingStop(snap)
	s.applyRangeBreakevenStop(snap.Close)
	s.applyRangeSteppedTrail(snap.Close)

	if s.getParam(paramRangeUseTimeStop, 0) > 0 {
		maxBars := int(s.getParam(paramRangeMaxBars, 0))
		if maxBars > 0 && heldBars >= maxBars {
			return rangeExitDecision{
				Action:          s.rangeExitAction(),
				Reason:          fmt.Sprintf("[%s range] 时间止损：持仓K线=%d ≥ 上限=%d", interval, heldBars, maxBars),
				SignalType:      "CLOSE",
				Quantity:        s.pos.quantity,
				ExitReasonKind:  "time_stop",
				ExitReasonLabel: "时间止损",
			}
		}
	}
	if s.pos.side == sideLong && snap.Close <= s.pos.stopLoss {
		exitReasonKind, exitReasonLabel := s.currentStopExitReason()
		return rangeExitDecision{
			Action:          "SELL",
			Reason:          fmt.Sprintf("[%s range] 多头止损：价格%.2f ≤ 止损%.2f", interval, snap.Close, s.pos.stopLoss),
			SignalType:      "CLOSE",
			Quantity:        s.pos.quantity,
			ExitReasonKind:  exitReasonKind,
			ExitReasonLabel: exitReasonLabel,
		}
	}
	if s.pos.side == sideShort && snap.Close >= s.pos.stopLoss {
		exitReasonKind, exitReasonLabel := s.currentStopExitReason()
		return rangeExitDecision{
			Action:          "BUY",
			Reason:          fmt.Sprintf("[%s range] 空头止损：价格%.2f ≥ 止损%.2f", interval, snap.Close, s.pos.stopLoss),
			SignalType:      "CLOSE",
			Quantity:        s.pos.quantity,
			ExitReasonKind:  exitReasonKind,
			ExitReasonLabel: exitReasonLabel,
		}
	}
	if s.rangeTakeProfitMethodName() == "rsi" {
		if s.pos.side == sideLong && snap.Rsi >= s.getParam(paramRangeM15RsiShortMin, 70) {
			return rangeExitDecision{
				Action:          "SELL",
				Reason:          fmt.Sprintf("[%s range] RSI止盈：RSI=%.2f ≥ %.2f", interval, snap.Rsi, s.getParam(paramRangeM15RsiShortMin, 70)),
				SignalType:      "CLOSE",
				Quantity:        s.pos.quantity,
				ExitReasonKind:  "rsi_take_profit",
				ExitReasonLabel: "RSI止盈",
			}
		}
		if s.pos.side == sideShort && snap.Rsi <= s.getParam(paramRangeM15RsiLongMax, 30) {
			return rangeExitDecision{
				Action:          "BUY",
				Reason:          fmt.Sprintf("[%s range] RSI止盈：RSI=%.2f ≤ %.2f", interval, snap.Rsi, s.getParam(paramRangeM15RsiLongMax, 30)),
				SignalType:      "CLOSE",
				Quantity:        s.pos.quantity,
				ExitReasonKind:  "rsi_take_profit",
				ExitReasonLabel: "RSI止盈",
			}
		}
		return rangeExitDecision{}
	}
	if s.rangeTakeProfitMethodName() == "split" && !s.pos.partialClosed && rangePriceReachedTarget(s.pos.side, snap.Close, s.pos.takeProfit1) {
		partialQty := s.pos.quantity * s.getParam(paramRangeSplitTPRatio, 0.5)
		if partialQty <= 0 || partialQty >= s.pos.quantity {
			partialQty = s.pos.quantity * 0.5
		}
		return rangeExitDecision{
			Action:          s.rangeExitAction(),
			Reason:          fmt.Sprintf("[%s range] 分批止盈：价格%.2f 命中中轨目标%.2f，减仓%.4f", interval, snap.Close, s.pos.takeProfit1, partialQty),
			SignalType:      "PARTIAL_CLOSE",
			Quantity:        partialQty,
			Partial:         true,
			ExitReasonKind:  "partial_take_profit",
			ExitReasonLabel: "分批止盈",
		}
	}
	if rangePriceReachedTarget(s.pos.side, snap.Close, s.pos.takeProfit2) {
		return rangeExitDecision{
			Action:          s.rangeExitAction(),
			Reason:          fmt.Sprintf("[%s range] 目标止盈：价格%.2f 命中目标%.2f", interval, snap.Close, s.pos.takeProfit2),
			SignalType:      "CLOSE",
			Quantity:        s.pos.quantity,
			ExitReasonKind:  "take_profit",
			ExitReasonLabel: "目标止盈",
		}
	}
	return rangeExitDecision{}
}

// updateRangeMaxProfit 更新持仓的最大浮盈，供 ATR 移动止损判断是否激活。
func (s *TrendFollowingStrategy) updateRangeMaxProfit(price float64) {
	profit := 0.0
	if s.pos.side == sideLong {
		profit = price - s.pos.entryPrice
	} else if s.pos.side == sideShort {
		profit = s.pos.entryPrice - price
	}
	if profit > s.pos.maxProfit {
		s.pos.maxProfit = profit
	}
}

// applyRangeTrailingStop 在浮盈达到阈值后，以当前周期 ATR 为尺度更新震荡持仓止损。
func (s *TrendFollowingStrategy) applyRangeTrailingStop(snap klineSnapshot) {
	if s.getParam(paramRangeUseTrailingStop, 0) <= 0 || snap.Atr <= 0 {
		return
	}
	activate := s.getParam(paramRangeTrailActivate, 0)
	trailAtr := s.getParam(paramRangeTrailAtrMult, 0)
	if activate <= 0 || trailAtr <= 0 || s.pos.maxProfit < activate*snap.Atr {
		return
	}
	if s.pos.side == sideLong {
		s.pos.stopLoss = math.Max(s.pos.stopLoss, snap.Close-trailAtr*snap.Atr)
		return
	}
	s.pos.stopLoss = math.Min(s.pos.stopLoss, snap.Close+trailAtr*snap.Atr)
}

// applyRangeBreakevenStop 在价格接近主目标时，把止损抬到成本价以降低回撤。
func (s *TrendFollowingStrategy) applyRangeBreakevenStop(price float64) {
	if s.getParam(paramRangeUseBreakevenStop, 0) <= 0 {
		return
	}
	target := s.rangePrimaryTargetPrice()
	if target == 0 {
		return
	}
	activate := s.getParam(paramRangeBreakevenActivate, 0.6)
	if activate <= 0 {
		activate = 0.6
	}
	targetDist := math.Abs(target - s.pos.entryPrice)
	if targetDist <= 0 {
		return
	}
	if s.pos.side == sideLong && price >= s.pos.entryPrice+targetDist*activate {
		s.pos.stopLoss = math.Max(s.pos.stopLoss, s.pos.entryPrice)
		return
	}
	if s.pos.side == sideShort && price <= s.pos.entryPrice-targetDist*activate {
		s.pos.stopLoss = math.Min(s.pos.stopLoss, s.pos.entryPrice)
	}
}

// applyRangeSteppedTrail 在接近目标位时，把止损推进到盈利区间的指定比例。
func (s *TrendFollowingStrategy) applyRangeSteppedTrail(price float64) {
	if s.getParam(paramRangeUseSteppedTrail, 0) <= 0 {
		return
	}
	target := s.rangePrimaryTargetPrice()
	if target == 0 {
		return
	}
	stepPct := s.getParam(paramRangeTrailStep1Pct, 0.9)
	stepSL := s.getParam(paramRangeTrailStep1SL, 0.65)
	if stepPct <= 0 || stepSL <= 0 {
		return
	}
	targetDist := math.Abs(target - s.pos.entryPrice)
	if targetDist <= 0 {
		return
	}
	if s.pos.side == sideLong && price >= s.pos.entryPrice+targetDist*stepPct {
		s.pos.stopLoss = math.Max(s.pos.stopLoss, s.pos.entryPrice+targetDist*stepSL)
		return
	}
	if s.pos.side == sideShort && price <= s.pos.entryPrice-targetDist*stepPct {
		s.pos.stopLoss = math.Min(s.pos.stopLoss, s.pos.entryPrice-targetDist*stepSL)
	}
}

// rangePrimaryTargetPrice 返回当前持仓管理优先参考的主目标价。
func (s *TrendFollowingStrategy) rangePrimaryTargetPrice() float64 {
	if s.pos.takeProfit2 != 0 {
		return s.pos.takeProfit2
	}
	return s.pos.takeProfit1
}

// rangeExitAction 根据当前持仓方向返回对应的平仓动作。
func (s *TrendFollowingStrategy) rangeExitAction() string {
	if s.pos.side == sideShort {
		return "BUY"
	}
	return "SELL"
}

// rangeHeldBars 统计当前持仓从入场K线开始已经持有了多少根同周期K线。
func rangeHeldBars(history []klineSnapshot, entryBarTime int64) int {
	count := 0
	for _, snap := range history {
		if snap.OpenTime >= entryBarTime {
			count++
		}
	}
	return count
}

// rangePriceReachedTarget 判断价格是否已经命中对应方向的目标位。
func rangePriceReachedTarget(side positionSide, price, target float64) bool {
	if target == 0 {
		return false
	}
	if side == sideLong {
		return price >= target
	}
	return price <= target
}

// signalReasonRangePayloadFromStats 将震荡策略判定结果整理成前端友好的布尔摘要。
func signalReasonRangePayloadFromStats(stats rangeStats) *signalReasonRangePayload {
	out := &signalReasonRangePayload{
		H1RangeOK:      stats.H1RangeOK,
		H1AdxOK:        stats.H1AdxOk,
		H1BollWidthOK:  stats.H1BollWidthOk,
		M15TouchLower:  stats.NearRangeLow,
		M15RsiTurnUp:   stats.LongRSITurnOk,
		M15TouchUpper:  stats.NearRangeHigh,
		M15RsiTurnDown: stats.ShortRSITurnOk,
	}
	if !out.H1RangeOK && !out.H1AdxOK && !out.H1BollWidthOK && !out.M15TouchLower && !out.M15RsiTurnUp && !out.M15TouchUpper && !out.M15RsiTurnDown {
		return nil
	}
	return out
}

// buildRangeEntryReason 输出“4H 震荡 + 1H 区间定位 + 当前周期多信号聚合”的可读原因文本。
func (s *TrendFollowingStrategy) buildRangeEntryReason(interval string, entry entrySignal, snap klineSnapshot, stopLoss, tp1, tp2 float64, stats rangeStats) string {
	direction := "下轨反转做多"
	level := stats.H1BollLower
	signals := stats.LongSignalCount
	fakeBreakout := stats.FakeBreakoutLong
	if entry == entryShort {
		direction = "上轨反转做空"
		level = stats.H1BollUpper
		signals = stats.ShortSignalCount
		fakeBreakout = stats.FakeBreakoutShort
	}
	return fmt.Sprintf(
		"[%s range] 4H震荡评分=%d 1H区间=%s(ADX=%.2f 带宽=%.4f) | %s 触发位=%.2f 收盘=%.2f RSI=%.2f->%.2f 信号数=%d fake_breakout=%t | 止损=%.2f TP1=%.2f TP2=%.2f",
		interval, stats.H4Score, stats.H1Zone, stats.H1Adx, stats.H1BollWidthPct, direction, level, snap.Close, stats.PrevRsi, snap.Rsi, signals, fakeBreakout, stopLoss, tp1, tp2,
	)
}

// rangeFakeBreakoutLabel 在 setup 文案里补充当前是否属于假突破反向入场。
func rangeFakeBreakoutLabel(stats rangeStats, entry entrySignal) string {
	if entry == entryLong && stats.FakeBreakoutLong {
		return "（含假突破反向）"
	}
	if entry == entryShort && stats.FakeBreakoutShort {
		return "（含假突破反向）"
	}
	return ""
}

// countTrue 统计一组布尔条件中为真的数量，供震荡策略多信号聚合使用。
func countTrue(values ...bool) int {
	total := 0
	for _, item := range values {
		if item {
			total++
		}
	}
	return total
}

// maxInt 返回两个整数中的较大值，避免多处手写条件比较。
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
