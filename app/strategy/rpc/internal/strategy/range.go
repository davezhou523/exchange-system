package strategy

import (
	"context"
	"fmt"
	"math"
	"time"

	marketpb "exchange-system/common/pb/market"
)

// rangeStats 汇总一次震荡区间判定所需的关键统计量，便于复用到决策日志和信号原因里。
type rangeStats struct {
	RangeHigh       float64
	RangeLow        float64
	RangeMid        float64
	RangeWidth      float64
	EntryBuffer     float64
	MidNoTradeBand  float64
	NearRangeLow    bool
	NearRangeHigh   bool
	LongRSIOk       bool
	ShortRSIOk      bool
	FlatEMAOk       bool
	MidBlocked      bool
	TrendTooStrong  bool
	Lookback        int
	EmaSpreadPct    float64
	EmaDriftRatio   float64
	MeanRevertScore float64
}

// checkRangeEntryConditions 执行 15m Range 变体的最小开仓判定。
func (s *TrendFollowingStrategy) checkRangeEntryConditions(ctx context.Context, k *marketpb.Kline) error {
	if !s.checkRiskLimits() {
		s.writeDecisionLogIfEnabled("entry", "skip", "risk_limits_block", k, nil)
		return nil
	}
	m15 := s.latest15m
	if m15.Atr == 0 || m15.Rsi == 0 {
		s.writeDecisionLogIfEnabled("entry", "skip", "range_m15_not_ready", k, nil)
		return nil
	}
	entry, stats := s.judgeRangeEntry(m15, s.klines15m, int(s.getParam(paramRangeLookback, 12)))
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

// checkRangeEntryConditions1m 执行 1m Range 变体的最小开仓判定。
func (s *TrendFollowingStrategy) checkRangeEntryConditions1m(ctx context.Context, k *marketpb.Kline) error {
	if !s.checkRiskLimits() {
		s.writeDecisionLogIfEnabled("entry", "skip", "risk_limits_block", k, nil)
		return nil
	}
	m1 := s.latest1m
	if m1.Atr == 0 || m1.Rsi == 0 {
		s.writeDecisionLogIfEnabled("entry", "skip", "range_1m_not_ready", k, nil)
		return nil
	}
	entry, stats := s.judgeRangeEntry(m1, s.klines1m, int(s.getParam(paramRangeLookback, 20)))
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

// judgeRangeEntry 用统一规则判断当前周期是否处于可做均值回归的震荡边界。
func (s *TrendFollowingStrategy) judgeRangeEntry(current klineSnapshot, history []klineSnapshot, lookback int) (entrySignal, rangeStats) {
	stats := rangeStats{Lookback: lookback}
	if current.Atr <= 0 || current.Rsi <= 0 || len(history) <= 1 {
		return entryNone, stats
	}
	if lookback <= 0 {
		lookback = 12
		stats.Lookback = lookback
	}
	window := breakoutWindow(history, lookback)
	if len(window) == 0 {
		return entryNone, stats
	}
	stats.RangeHigh = window[0].High
	stats.RangeLow = window[0].Low
	for _, item := range window {
		if item.High > stats.RangeHigh {
			stats.RangeHigh = item.High
		}
		if item.Low < stats.RangeLow {
			stats.RangeLow = item.Low
		}
	}
	if stats.RangeHigh <= stats.RangeLow {
		return entryNone, stats
	}
	stats.RangeMid = (stats.RangeHigh + stats.RangeLow) / 2
	stats.RangeWidth = stats.RangeHigh - stats.RangeLow
	stats.EntryBuffer = current.Atr * s.getParam(paramRangeEntryAtrBuffer, 0.35)
	stats.MidNoTradeBand = stats.RangeWidth * s.getParam(paramRangeMidNoTradeWidthPct, 0.18)
	stats.NearRangeLow = current.Close <= stats.RangeLow+stats.EntryBuffer
	stats.NearRangeHigh = current.Close >= stats.RangeHigh-stats.EntryBuffer
	stats.LongRSIOk = current.Rsi <= s.getParam(paramRangeRsiLongMax, 42)
	stats.ShortRSIOk = current.Rsi >= s.getParam(paramRangeRsiShortMin, 58)
	stats.EmaSpreadPct = rangeEmaSpreadPct(current)
	stats.FlatEMAOk = s.rangeFlatEMAOK(current, stats)
	stats.MidBlocked = s.rangeMidBlocked(current, stats)
	stats.EmaDriftRatio = s.rangeEmaDriftRatio(current, window, stats)
	stats.TrendTooStrong = s.rangeTrendTooStrong(stats)
	if stats.RangeWidth > 0 {
		stats.MeanRevertScore = math.Abs(current.Close-stats.RangeMid) / stats.RangeWidth
	}

	if stats.MidBlocked || stats.TrendTooStrong {
		return entryNone, stats
	}
	if stats.NearRangeLow && stats.LongRSIOk && stats.FlatEMAOk {
		return entryLong, stats
	}
	if stats.NearRangeHigh && stats.ShortRSIOk && stats.FlatEMAOk {
		return entryShort, stats
	}
	return entryNone, stats
}

// rangeMidBlocked 判断价格是否过于靠近区间中轴，避免在赔率最差的位置做震荡单。
func (s *TrendFollowingStrategy) rangeMidBlocked(current klineSnapshot, stats rangeStats) bool {
	if stats.RangeWidth <= 0 {
		return false
	}
	band := stats.MidNoTradeBand
	if band <= 0 {
		return false
	}
	return math.Abs(current.Close-stats.RangeMid) <= band
}

// rangeFlatEMAOK 判断均线是否足够收敛，避免在强趋势阶段误把回调当作震荡。
func (s *TrendFollowingStrategy) rangeFlatEMAOK(current klineSnapshot, stats rangeStats) bool {
	if s.getParam(paramRangeRequireFlatEma, 1) <= 0 {
		return true
	}
	if current.Close <= 0 || current.Ema21 <= 0 || current.Ema55 <= 0 {
		return false
	}
	return stats.EmaSpreadPct <= 0.003
}

// rangeEmaDriftRatio 估算 EMA21 在观察窗口起点到当前时刻的漂移幅度，用于识别单边趋势是否已经主导区间。
func (s *TrendFollowingStrategy) rangeEmaDriftRatio(current klineSnapshot, window []klineSnapshot, stats rangeStats) float64 {
	if len(window) == 0 || stats.RangeWidth <= 0 {
		return 0
	}
	start := window[0]
	if start.Ema21 <= 0 || current.Ema21 <= 0 {
		return 0
	}
	return math.Abs(current.Ema21-start.Ema21) / stats.RangeWidth
}

// rangeTrendTooStrong 判断均线漂移是否过大，若趋势已明显单边则直接禁止做震荡。
func (s *TrendFollowingStrategy) rangeTrendTooStrong(stats rangeStats) bool {
	maxDrift := s.getParam(paramRangeTrendEmaDriftMax, 0.35)
	if maxDrift <= 0 {
		return false
	}
	return stats.EmaDriftRatio >= maxDrift
}

// rangeEmaSpreadPct 计算 EMA21 与 EMA55 的相对距离，用于判断是否仍处于平缓震荡结构。
func rangeEmaSpreadPct(current klineSnapshot) float64 {
	if current.Close <= 0 || current.Ema21 <= 0 || current.Ema55 <= 0 {
		return 0
	}
	return math.Abs(current.Ema21-current.Ema55) / current.Close
}

// rangeDecisionExtras 组装震荡策略的结构化日志上下文，便于直接看到为何未开仓。
func (s *TrendFollowingStrategy) rangeDecisionExtras(prefix string, snap klineSnapshot, stats rangeStats) map[string]interface{} {
	extras := map[string]interface{}{
		"range_high":             stats.RangeHigh,
		"range_low":              stats.RangeLow,
		"range_mid":              stats.RangeMid,
		"range_width":            stats.RangeWidth,
		"range_entry_buffer_atr": stats.EntryBuffer,
		"range_mid_no_trade":     stats.MidNoTradeBand,
		"range_near_low":         stats.NearRangeLow,
		"range_near_high":        stats.NearRangeHigh,
		"range_long_rsi_ok":      stats.LongRSIOk,
		"range_short_rsi_ok":     stats.ShortRSIOk,
		"range_flat_ema_ok":      stats.FlatEMAOk,
		"range_mid_blocked":      stats.MidBlocked,
		"range_trend_too_strong": stats.TrendTooStrong,
		"range_lookback":         stats.Lookback,
		"range_ema_spread_pct":   stats.EmaSpreadPct,
		"range_ema_drift_ratio":  stats.EmaDriftRatio,
		"range_mean_revert":      stats.MeanRevertScore,
	}
	appendSnapshotExtras(extras, prefix, snap)
	return extras
}

// openRangePosition 复用现有风控和信号发送链路，生成 Range 专用开仓信号。
func (s *TrendFollowingStrategy) openRangePosition(ctx context.Context, k *marketpb.Kline, interval string, entry entrySignal, snap klineSnapshot, stats rangeStats) error {
	atrMult := s.getParam(paramStopLossATRMult, 1.5)
	if interval == "1m" {
		atrMult = s.getParam(param1mAtrMult, 1.5)
	}
	stopDist := atrMult * snap.Atr
	targetMult := s.getParam(paramRangeTargetAtrMult, 1.2)

	action := "BUY"
	side := "LONG"
	stopLoss := snap.Close - stopDist
	tp1 := math.Min(stats.RangeMid, snap.Close+targetMult*snap.Atr)
	tp2 := stats.RangeHigh - stats.EntryBuffer*0.5
	if tp2 <= tp1 {
		tp2 = snap.Close + 2*targetMult*snap.Atr
	}
	if entry == entryShort {
		action = "SELL"
		side = "SHORT"
		stopLoss = snap.Close + stopDist
		tp1 = math.Max(stats.RangeMid, snap.Close-targetMult*snap.Atr)
		tp2 = stats.RangeLow + stats.EntryBuffer*0.5
		if tp2 >= tp1 {
			tp2 = snap.Close - 2*targetMult*snap.Atr
		}
	}

	quantity := s.calculatePositionSize(snap.Close, stopLoss)
	adjustedQuantity, blocked, blockReason := s.applyWeightRecommendation(quantity)
	if blocked {
		extras := s.rangeDecisionExtras(interval, snap, stats)
		extras["base_quantity"] = quantity
		extras["reason"] = blockReason
		s.writeDecisionLogIfEnabled("entry", "skip", blockReason, k, extras)
		return nil
	}
	quantity = adjustedQuantity

	reason := s.buildRangeEntryReason(interval, entry, snap, stopLoss, tp1, tp2, stats)
	signalReason := s.newSignalReason(
		reason,
		"OPEN_ENTRY",
		fmt.Sprintf("Range=%s", stringEntry(entry)),
		fmt.Sprintf("%s 区间边界回归均值", interval),
		fmt.Sprintf("RSI=%.2f ATR=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f", snap.Rsi, snap.Atr, stopLoss, tp1, tp2),
		interval, s.strategySignalTag(), "open", side,
	)

	s.pos = position{
		side:         sideLong,
		entryPrice:   snap.Close,
		stopLoss:     stopLoss,
		takeProfit1:  tp1,
		takeProfit2:  tp2,
		entryBarTime: snap.OpenTime,
		atr:          snap.Atr,
		hitTP1:       false,
	}
	if entry == entryShort {
		s.pos.side = sideShort
	}

	riskReward := 0.0
	if tpStopDist := math.Abs(snap.Close - stopLoss); tpStopDist > 0 {
		riskReward = math.Abs(tp1-snap.Close) / tpStopDist
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
		"stop_loss":     stopLoss,
		"take_profits":  []float64{tp1, tp2},
		"reason":        signalReason.Summary,
		"signal_reason": signalReason,
		"timestamp":     time.Now().UnixMilli(),
		"atr":           snap.Atr,
		"risk_reward":   riskReward,
		"indicators":    indicators,
	}

	extras := s.rangeDecisionExtras(interval, snap, stats)
	extras["action"] = action
	extras["side"] = side
	extras["entry_price"] = snap.Close
	extras["stop_loss"] = stopLoss
	extras["take_profits"] = []float64{tp1, tp2}
	s.writeDecisionLogIfEnabled("entry", "signal", "open_signal_sent", k, extras)
	return s.sendSignal(ctx, signal, k)
}

// buildRangeEntryReason 统一输出震荡策略的可读原因文本，便于信号日志和人工排查。
func (s *TrendFollowingStrategy) buildRangeEntryReason(interval string, entry entrySignal, snap klineSnapshot, stopLoss, tp1, tp2 float64, stats rangeStats) string {
	direction := "下沿做多"
	level := stats.RangeLow
	if entry == entryShort {
		direction = "上沿做空"
		level = stats.RangeHigh
	}
	return fmt.Sprintf(
		"[%s range] %s | 触发位=%.2f 收盘=%.2f RSI=%.2f 区间中轴=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f",
		interval, direction, level, snap.Close, snap.Rsi, stats.RangeMid, stopLoss, tp1, tp2,
	)
}
