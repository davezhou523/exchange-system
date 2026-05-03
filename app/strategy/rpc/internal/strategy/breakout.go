package strategy

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	marketpb "exchange-system/common/pb/market"
)

// breakoutStats 汇总一次突破判定所需的关键统计量，便于复用在信号和决策日志里。
type breakoutStats struct {
	RecentHigh   float64
	RecentLow    float64
	AvgVolume    float64
	VolumeRatio  float64
	BreakoutBuf  float64
	BreakoutUp   bool
	BreakoutDown bool
	LongRSIOk    bool
	ShortRSIOk   bool
	LongEMAOk    bool
	ShortEMAOk   bool
	VolumeOk     bool
	Lookback     int
}

// checkBreakoutEntryConditions 执行 15m Breakout 变体的最小开仓判定。
func (s *TrendFollowingStrategy) checkBreakoutEntryConditions(ctx context.Context, k *marketpb.Kline) error {
	if !s.checkRiskLimits() {
		s.writeDecisionLogIfEnabled("entry", "skip", "risk_limits_block", k, nil)
		return nil
	}
	m15 := s.latest15m
	if m15.Atr == 0 || m15.Rsi == 0 {
		s.writeDecisionLogIfEnabled("entry", "skip", "breakout_m15_not_ready", k, nil)
		return nil
	}
	entry, stats := s.judgeBreakoutEntry(m15, s.klines15m, int(s.getParam(paramM15BreakoutLookback, 6)))
	if entry == entryNone {
		s.writeDecisionLogIfEnabled("entry", "skip", "breakout_no_entry", k, s.breakoutDecisionExtras("m15", m15, stats))
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		extras := s.breakoutDecisionExtras("m15", m15, stats)
		extras["entry"] = stringEntry(entry)
		s.writeDecisionLogIfEnabled("entry", "skip", "harvest_path_block", k, extras)
		return nil
	}
	return s.openBreakoutPosition(ctx, k, "15m", entry, m15, stats)
}

// checkBreakoutEntryConditions1m 执行 1m Breakout 变体的最小开仓判定。
func (s *TrendFollowingStrategy) checkBreakoutEntryConditions1m(ctx context.Context, k *marketpb.Kline) error {
	if !s.checkRiskLimits() {
		return nil
	}
	m1 := s.latest1m
	if m1.Atr == 0 || m1.Rsi == 0 {
		return nil
	}
	entry, stats := s.judgeBreakoutEntry(m1, s.klines1m, int(s.getParam(param1mBreakoutLookback, 20)))
	if entry == entryNone {
		return nil
	}
	if s.shouldBlockForHarvestPathRisk(ctx, k, entry) {
		return nil
	}
	return s.openBreakoutPosition(ctx, k, "1m", entry, m1, stats)
}

// judgeBreakoutEntry 用统一规则判断当前周期是否形成有效突破信号。
func (s *TrendFollowingStrategy) judgeBreakoutEntry(current klineSnapshot, history []klineSnapshot, lookback int) (entrySignal, breakoutStats) {
	stats := breakoutStats{Lookback: lookback}
	if current.Atr <= 0 || current.Rsi <= 0 || len(history) <= 1 {
		return entryNone, stats
	}
	if lookback <= 0 {
		lookback = 6
		stats.Lookback = lookback
	}
	window := breakoutWindow(history, lookback)
	if len(window) == 0 {
		return entryNone, stats
	}
	stats.RecentHigh = window[0].High
	stats.RecentLow = window[0].Low
	totalVolume := 0.0
	validVolumeCount := 0
	for _, item := range window {
		if item.High > stats.RecentHigh {
			stats.RecentHigh = item.High
		}
		if item.Low < stats.RecentLow {
			stats.RecentLow = item.Low
		}
		if item.Volume > 0 {
			totalVolume += item.Volume
			validVolumeCount++
		}
	}
	if validVolumeCount > 0 {
		stats.AvgVolume = totalVolume / float64(validVolumeCount)
	}
	if stats.AvgVolume > 0 {
		stats.VolumeRatio = current.Volume / stats.AvgVolume
	}
	stats.BreakoutBuf = current.Atr * s.getParam(paramBreakoutEntryBufferATR, 0.10)
	stats.BreakoutUp = current.Close > stats.RecentHigh+stats.BreakoutBuf
	stats.BreakoutDown = current.Close < stats.RecentLow-stats.BreakoutBuf
	stats.LongRSIOk = current.Rsi >= s.getParam(paramBreakoutRsiLongMin, 55)
	stats.ShortRSIOk = current.Rsi <= s.getParam(paramBreakoutRsiShortMax, 45)
	stats.LongEMAOk, stats.ShortEMAOk = s.breakoutEMAAlignment(current)
	stats.VolumeOk = s.breakoutVolumeOK(stats)

	if stats.BreakoutUp && stats.LongRSIOk && stats.LongEMAOk && stats.VolumeOk {
		return entryLong, stats
	}
	if stats.BreakoutDown && stats.ShortRSIOk && stats.ShortEMAOk && stats.VolumeOk {
		return entryShort, stats
	}
	return entryNone, stats
}

// breakoutWindow 返回用于突破判定的历史窗口，并始终排除当前这根 K 线。
func breakoutWindow(history []klineSnapshot, lookback int) []klineSnapshot {
	if len(history) <= 1 {
		return nil
	}
	end := len(history) - 1
	start := end - lookback
	if start < 0 {
		start = 0
	}
	return history[start:end]
}

// breakoutEMAAlignment 用于限制突破方向与当前均线结构尽量一致，减少逆势追价。
func (s *TrendFollowingStrategy) breakoutEMAAlignment(current klineSnapshot) (bool, bool) {
	if s.getParam(paramBreakoutRequireEmaTrend, 1) <= 0 {
		return true, true
	}
	longOk := current.Close >= current.Ema21 && current.Ema21 >= current.Ema55
	shortOk := current.Close <= current.Ema21 && current.Ema21 <= current.Ema55
	return longOk, shortOk
}

// breakoutVolumeOK 使用量能放大过滤假突破，均量缺失时退化为只要求当前成交量为正。
func (s *TrendFollowingStrategy) breakoutVolumeOK(stats breakoutStats) bool {
	minRatio := s.getParam(paramBreakoutVolumeRatioMin, 1.20)
	if minRatio <= 0 {
		return true
	}
	if stats.AvgVolume <= 0 {
		return stats.VolumeRatio > 0
	}
	return stats.VolumeRatio >= minRatio
}

// breakoutDecisionExtras 组装突破判定的结构化日志上下文，便于直接看到为何未开仓。
func (s *TrendFollowingStrategy) breakoutDecisionExtras(prefix string, snap klineSnapshot, stats breakoutStats) map[string]interface{} {
	extras := map[string]interface{}{
		"breakout_recent_high":  stats.RecentHigh,
		"breakout_recent_low":   stats.RecentLow,
		"breakout_avg_volume":   stats.AvgVolume,
		"breakout_volume_ratio": stats.VolumeRatio,
		"breakout_buffer_atr":   stats.BreakoutBuf,
		"breakout_up":           stats.BreakoutUp,
		"breakout_down":         stats.BreakoutDown,
		"breakout_long_rsi_ok":  stats.LongRSIOk,
		"breakout_short_rsi_ok": stats.ShortRSIOk,
		"breakout_long_ema_ok":  stats.LongEMAOk,
		"breakout_short_ema_ok": stats.ShortEMAOk,
		"breakout_volume_ok":    stats.VolumeOk,
		"breakout_lookback":     stats.Lookback,
	}
	appendSnapshotExtras(extras, prefix, snap)
	return extras
}

// openBreakoutPosition 复用现有风控和信号发送链路，生成 Breakout 专用开仓信号。
func (s *TrendFollowingStrategy) openBreakoutPosition(ctx context.Context, k *marketpb.Kline, interval string, entry entrySignal, snap klineSnapshot, stats breakoutStats) error {
	atrMult := s.getParam(paramStopLossATRMult, 1.5)
	if interval == "1m" {
		atrMult = s.getParam(param1mAtrMult, 1.5)
	}
	stopDist := atrMult * snap.Atr
	action := "BUY"
	side := "LONG"
	stopLoss := snap.Close - stopDist
	tp1 := snap.Close + stopDist
	tp2 := snap.Close + 2*stopDist
	if entry == entryShort {
		action = "SELL"
		side = "SHORT"
		stopLoss = snap.Close + stopDist
		tp1 = snap.Close - stopDist
		tp2 = snap.Close - 2*stopDist
	}

	quantity := s.calculatePositionSize(snap.Close, stopLoss)
	adjustedQuantity, blocked, blockReason := s.applyWeightRecommendation(quantity)
	if blocked {
		extras := s.breakoutDecisionExtras(interval, snap, stats)
		extras["base_quantity"] = quantity
		extras["reason"] = blockReason
		s.writeDecisionLogIfEnabled("entry", "skip", blockReason, k, extras)
		return nil
	}
	quantity = adjustedQuantity

	reason := s.buildBreakoutEntryReason(interval, entry, snap, stopLoss, tp1, tp2, stats)
	signalReason := s.newSignalReason(
		reason,
		"OPEN_ENTRY",
		fmt.Sprintf("Breakout=%s", stringEntry(entry)),
		fmt.Sprintf("%s 结构突破 + 量能放大确认", interval),
		fmt.Sprintf("RSI=%.2f ATR=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f", snap.Rsi, snap.Atr, stopLoss, tp1, tp2),
		interval, s.strategySignalTag(), "open", side,
	)

	s.pos = position{
		side:          sideLong,
		entryPrice:    snap.Close,
		quantity:      quantity,
		stopLoss:      stopLoss,
		takeProfit1:   tp1,
		takeProfit2:   tp2,
		entryBarTime:  snap.OpenTime,
		atr:           snap.Atr,
		hitTP1:        false,
		partialClosed: false,
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
	if interval == "1m" {
		indicators["m15_ema21"] = s.latest15m.Ema21
		indicators["m15_rsi"] = s.latest15m.Rsi
		indicators["m15_atr"] = s.latest15m.Atr
	} else {
		indicators["h1_ema21"] = s.latest1h.Ema21
		indicators["h1_rsi"] = s.latest1h.Rsi
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

	extras := s.breakoutDecisionExtras(interval, snap, stats)
	extras["action"] = action
	extras["side"] = side
	extras["entry_price"] = snap.Close
	extras["stop_loss"] = stopLoss
	extras["take_profits"] = []float64{tp1, tp2}
	s.writeDecisionLogIfEnabled("entry", "signal", "open_signal_sent", k, extras)
	return s.sendSignal(ctx, signal, k)
}

// buildBreakoutEntryReason 统一输出突破策略的可读原因文本，便于信号日志和人工排查。
func (s *TrendFollowingStrategy) buildBreakoutEntryReason(interval string, entry entrySignal, snap klineSnapshot, stopLoss, tp1, tp2 float64, stats breakoutStats) string {
	direction := "向上突破"
	level := stats.RecentHigh
	if entry == entryShort {
		direction = "向下突破"
		level = stats.RecentLow
	}
	return fmt.Sprintf(
		"[%s breakout] %s | 触发位=%.2f 收盘=%.2f RSI=%.2f 量比=%.2f | 止损=%.2f TP1=%.2f TP2=%.2f",
		interval, direction, level, snap.Close, snap.Rsi, stats.VolumeRatio, stopLoss, tp1, tp2,
	)
}

// intervalKey 把周期和指标名拼成稳定的日志字段名，避免手写重复字符串。
func intervalKey(interval, suffix string) string {
	items := []string{interval, suffix}
	sort.Strings(items[:1])
	return fmt.Sprintf("%s_%s", interval, suffix)
}
