package aggregator

import (
	"exchange-system/common/indicator/atr"
	"exchange-system/common/indicator/ema"
	"exchange-system/common/indicator/rsi"
)

// --- 指标计算（按周期计算，避免指标粒度错位）---

// calcBucketIndicators 计算技术指标，使用**递推式**算法（O(1) 计算）。
// 核心原则：15m K线的 EMA/RSI/ATR 用 15m 收盘价序列计算，
// 而非用 1m 数据（否则指标含义错误）。
// 严禁使用 bucket 关闭之后的数据，避免未来函数（look-ahead bias）。
//
// ✅ 调用顺序保证：先调用本函数计算指标，再 push 当前 bucket 到 history。
// 因此 history 中只包含历史K线，不含当前 bucket：
// - IndicatorClosed：将当前K线数据临时用于计算（当前K线参与计算）
// - IndicatorPrevious：只用 history（当前K线不参与计算）
//
// ✅ 使用递推式指标（与交易所/TradingView 对齐）：
// - EMA: ema = prevEma + α * (price - prevEma), α = 2/(period+1)
// - RSI (Wilder/RMA): avgGain = (prevAvgGain*(p-1) + gain) / p
// - ATR (RMA/Wilder): atr = (prevAtr*(p-1) + TR) / p
//
// ✅ 递推状态管理：
//   - 递推状态（lastEma, avgGain, atr）在 push 时更新，保证与 history 数据一致
//   - IndicatorClosed 模式下，本函数会临时用当前K线做一次额外递推来计算结果，
//     但不修改 history 中的递推状态（避免状态超前于数据）
//   - push 时会再次递推更新状态，此时数据与状态完全一致
func (w *symbolWorker) calcBucketIndicators(b *bucket, intervalName string) {
	// 获取该周期的指标参数（优先用周期专属配置，回退到默认配置）
	params, ok := w.agg.intervalIndicators[intervalName]
	if !ok {
		params = IntervalIndicatorConfig{
			Ema21Period: w.agg.indicatorParams.Ema21Period,
			Ema55Period: w.agg.indicatorParams.Ema55Period,
			RsiPeriod:   w.agg.indicatorParams.RsiPeriod,
			AtrPeriod:   w.agg.indicatorParams.AtrPeriod,
		}
	}

	// 使用该周期的历史 ring buffer（此时不包含当前 bucket，因为先算再 push）
	history := w.intervalHistories[intervalName]
	if history == nil {
		return
	}

	// IndicatorClosed 模式：当前K线参与计算（临时递推一次，不修改状态）
	// IndicatorPrevious 模式：只用 history（当前K线不参与计算）
	includeCurrent := w.agg.indicatorMode == IndicatorClosed

	// --- EMA 递推计算 ---
	// ema = prevEma + α * (price - prevEma), α = 2/(period+1)
	if params.Ema21Period > 0 {
		b.Ema21 = w.computeEMAValue(history, b.Close, params.Ema21Period,
			&history.state.ema21, &history.state.ema21Init, includeCurrent)
	}
	if params.Ema55Period > 0 {
		b.Ema55 = w.computeEMAValue(history, b.Close, params.Ema55Period,
			&history.state.ema55, &history.state.ema55Init, includeCurrent)
	}

	// --- RSI 递推计算（Wilder/RMA 平滑）---
	// Wilder RSI: avgGain = (prevAvgGain*(period-1) + currentGain) / period
	if params.RsiPeriod > 0 {
		b.Rsi = w.computeRSIValue(history, b.Close, params.RsiPeriod, includeCurrent)
	}

	// --- ATR 递推计算（RMA/Wilder 平滑，与 Binance/TradingView 对齐）---
	// ATR = (prevATR*(period-1) + currentTR) / period
	if params.AtrPeriod > 0 {
		b.Atr = w.computeATRValue(history, b.High, b.Low, params.AtrPeriod, includeCurrent)
	}
}

// updateIndicatorState 在 push 后更新递推指标状态。
// 此时 history 已包含新K线数据，递推状态与数据保持一致。
func (w *symbolWorker) updateIndicatorState(history *klineRingBuffer, b *bucket, intervalName string) {
	params, ok := w.agg.intervalIndicators[intervalName]
	if !ok {
		params = IntervalIndicatorConfig{
			Ema21Period: w.agg.indicatorParams.Ema21Period,
			Ema55Period: w.agg.indicatorParams.Ema55Period,
			RsiPeriod:   w.agg.indicatorParams.RsiPeriod,
			AtrPeriod:   w.agg.indicatorParams.AtrPeriod,
		}
	}

	state := &history.state

	// EMA 递推：用当前K线的收盘价更新状态
	if params.Ema21Period > 0 && history.count >= params.Ema21Period {
		w.advanceEMAState(history, b.Close, params.Ema21Period,
			&state.ema21, &state.ema21Init)
	}
	if params.Ema55Period > 0 && history.count >= params.Ema55Period {
		w.advanceEMAState(history, b.Close, params.Ema55Period,
			&state.ema55, &state.ema55Init)
	}

	// RSI 递推：用当前K线的收盘价变化更新状态
	if params.RsiPeriod > 0 && history.count >= params.RsiPeriod+1 {
		w.advanceRSIState(history, b.Close, params.RsiPeriod)
	}

	// ATR 递推：用当前K线的 TR 更新状态
	if params.AtrPeriod > 0 && history.count >= params.AtrPeriod {
		w.advanceATRState(history, b.High, b.Low, params.AtrPeriod)
	}
}

// --- 递推式指标计算（O(1)，与交易所/TradingView 对齐）---
// 设计原则：
// 1. computeXxxValue: 读取当前状态，计算指标值（不修改状态）
//    IndicatorClosed 模式下会临时递推一次来包含当前K线
// 2. advanceXxxState: 在 push 后更新递推状态（修改状态）
//    保证递推状态始终与 history 中的数据一致

// ---------------------------------------------------------------------------
// EMA（指数移动平均）
// 公式：ema = prevEma + α * (price - prevEma)，α = 2/(period+1)
// 初始化：SMA(period) 作为首值，然后逐根递推
// ---------------------------------------------------------------------------

// computeEMAValue 计算 EMA 值（不修改递推状态）。
// IndicatorClosed 模式下，临时用当前价格做一次额外递推。
func (w *symbolWorker) computeEMAValue(history *klineRingBuffer, currentPrice float64, period int,
	lastEma *float64, initialized *bool, includeCurrent bool) float64 {

	if period <= 0 {
		return 0
	}

	closes := history.closes()

	// 数据不足以初始化
	if len(closes) < period {
		if includeCurrent && len(closes)+1 >= period {
			// 加上当前K线后刚好够初始化
			allCloses := make([]float64, len(closes), len(closes)+1)
			copy(allCloses, closes)
			allCloses = append(allCloses, currentPrice)
			emaVal := ema.Init(allCloses[:period], period)
			// 继续递推剩余部分
			for i := period; i < len(allCloses); i++ {
				emaVal = ema.Step(emaVal, allCloses[i], period)
			}
			return emaVal
		}
		if len(closes) > 0 {
			return closes[len(closes)-1]
		}
		if includeCurrent {
			return currentPrice
		}
		return 0
	}

	// 未初始化：需要从头计算（仅首次或重启后）
	if !*initialized {
		emaVal := ema.Init(closes, period)
		// 不更新 *lastEma 和 *initialized，状态更新由 advanceEMAState 在 push 时完成
		if includeCurrent {
			emaVal = ema.Step(emaVal, currentPrice, period)
		}
		return emaVal
	}

	// 已初始化：直接用缓存的 lastEma 递推
	emaVal := *lastEma
	if includeCurrent {
		emaVal = ema.Step(emaVal, currentPrice, period)
	}
	return emaVal
}

// advanceEMAState 在 push 后更新 EMA 递推状态。
func (w *symbolWorker) advanceEMAState(history *klineRingBuffer, currentPrice float64, period int,
	lastEma *float64, initialized *bool) {

	closes := history.closes()

	if !*initialized {
		if len(closes) < period {
			return // 数据不足，等待更多数据
		}
		// 初始化：SMA + 递推
		*lastEma = ema.Init(closes, period)
		*initialized = true
		return
	}

	// 已初始化：递推一次
	*lastEma = ema.Step(*lastEma, currentPrice, period)
}

// ---------------------------------------------------------------------------
// RSI（相对强弱指数）- Wilder/RMA 平滑
// 公式：avgGain = (prevAvgGain*(period-1) + currentGain) / period
// 初始化：前 period 个价格变化的简单平均，然后逐根 Wilder 递推
// ---------------------------------------------------------------------------

// computeRSIValue 计算 RSI 值（不修改递推状态）。
// Wilder/RMA 平滑：avgGain = (prevAvgGain*(period-1) + currentGain) / period
func (w *symbolWorker) computeRSIValue(history *klineRingBuffer, currentPrice float64, period int, includeCurrent bool) float64 {
	if period <= 0 {
		return 50.0
	}

	closes := history.closes()
	state := &history.state

	// 数据不足以计算 RSI（需要至少 period+1 根K线才有 period 个价格变化）
	totalCloses := len(closes)
	if includeCurrent {
		totalCloses++
	}
	if totalCloses <= period {
		return 50.0
	}

	// 未初始化：需要从头计算（标准 Wilder RSI）
	// 1. 用最前面 period 个价格变化的简单平均作为初始 avgGain/avgLoss
	// 2. 对后续所有价格变化做 Wilder 递推
	if !state.rsiInit || state.rsiPeriod != period {
		// 构建完整收盘价序列
		allCloses := closes
		if includeCurrent {
			allCloses = make([]float64, len(closes), len(closes)+1)
			copy(allCloses, closes)
			allCloses = append(allCloses, currentPrice)
		}
		if len(allCloses) <= period {
			return 50.0
		}
		return rsi.Calc(allCloses, period)
	}

	// 已初始化：用缓存的状态递推
	avgGain := state.avgGain
	avgLoss := state.avgLoss

	if includeCurrent && len(closes) > 0 {
		prevPrice := closes[len(closes)-1]
		avgGain, avgLoss = rsi.WilderStep(avgGain, avgLoss, currentPrice-prevPrice, period)
	}

	return rsi.FromAvgGainLoss(avgGain, avgLoss)
}

// advanceRSIState 在 push 后更新 RSI 递推状态。
func (w *symbolWorker) advanceRSIState(history *klineRingBuffer, currentPrice float64, period int) {
	closes := history.closes()
	state := &history.state

	if len(closes) < 2 {
		return
	}

	// 未初始化：用简单平均初始化（标准 Wilder 方法）
	// 1. 用最前面 period 个变化做简单平均
	// 2. 对后续所有变化做 Wilder 递推
	if !state.rsiInit || state.rsiPeriod != period {
		if len(closes) <= period {
			return
		}
		rsiState := rsi.InitWilder(closes, period)

		state.avgGain = rsiState.AvgGain
		state.avgLoss = rsiState.AvgLoss
		state.rsiPeriod = period
		state.rsiInit = true
		return
	}

	// 已初始化：递推
	prevPrice := closes[len(closes)-2]
	diff := currentPrice - prevPrice
	state.avgGain, state.avgLoss = rsi.WilderStep(state.avgGain, state.avgLoss, diff, period)
}

// ---------------------------------------------------------------------------
// ATR（平均真实范围）- RMA/Wilder 平滑（与 Binance/TradingView 对齐）
// 公式：ATR = (prevATR*(period-1) + currentTR) / period  （即 RMA）
// 初始化：前 period 个 TR 的简单平均，然后逐根 RMA 递推
// TR = max(H-L, |H-prevClose|, |L-prevClose|)
//
// 注：TradingView/Binance 的 ATR 使用 RMA（Running Moving Average），
//     也叫 Wilder 平滑，α = 1/period。
//     与 EMA（α=2/(period+1)）不同，RMA 对近期数据权重更低，更平滑。
// ---------------------------------------------------------------------------

// computeATRValue 计算 ATR 值（不修改递推状态）。
// RMA/Wilder 平滑：ATR = (prevATR*(period-1) + currentTR) / period
func (w *symbolWorker) computeATRValue(history *klineRingBuffer, currentHigh, currentLow float64, period int, includeCurrent bool) float64 {
	if period <= 0 || history.count == 0 {
		return 0
	}

	state := &history.state
	tr := atr.TrueRange(currentHigh, currentLow, history.lastClose())

	// 未初始化：需要从头计算（标准 RMA/Wilder ATR）
	// 1. 计算所有 TR
	// 2. 用最前面 period 个 TR 的简单平均作为初始 ATR
	// 3. 对后续所有 TR 做 RMA 递推
	if !state.atrInit || state.atrPeriod != period {
		highs := history.highs()
		lows := history.lows()
		prevCloses := history.prevCloses()

		n := len(highs)
		if n < period {
			return tr // 数据不足，返回当前 TR
		}

		atrState := atr.InitRma(highs, lows, prevCloses, period)

		if includeCurrent {
			return atr.RmaStep(atrState.ATR, tr, period)
		}
		return atrState.ATR
	}

	// 已初始化：用缓存的状态递推
	atrVal := state.atr
	if includeCurrent {
		atrVal = atr.RmaStep(atrVal, tr, period)
	}
	return atrVal
}

// advanceATRState 在 push 后更新 ATR 递推状态。
func (w *symbolWorker) advanceATRState(history *klineRingBuffer, currentHigh, currentLow float64, period int) {
	state := &history.state
	tr := atr.TrueRange(currentHigh, currentLow, history.prevCloseForLast())

	// 未初始化（标准 RMA/Wilder ATR）
	// 1. 用最前面 period 个 TR 的简单平均作为初始 ATR
	// 2. 对后续所有 TR 做 RMA 递推
	if !state.atrInit || state.atrPeriod != period {
		highs := history.highs()
		lows := history.lows()
		prevCloses := history.prevCloses()
		n := len(highs)
		if n < period {
			return
		}

		atrState := atr.InitRma(highs, lows, prevCloses, period)

		state.atr = atrState.ATR
		state.atrPeriod = period
		state.atrInit = true
		return
	}

	// 已初始化：RMA 递推
	state.atr = atr.RmaStep(state.atr, tr, period)
}
