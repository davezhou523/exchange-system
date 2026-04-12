package rsi

// ---------------------------------------------------------------------------
// RSI（相对强弱指数）- Wilder/RMA 平滑
// 公式：avgGain = (prevAvgGain*(period-1) + currentGain) / period
// 初始化：前 period 个价格变化的简单平均，然后逐根 Wilder 递推
//
// 与交易所/TradingView 对齐，纯函数式实现，不依赖任何业务类型。
// ---------------------------------------------------------------------------

// State RSI 递推状态
type State struct {
	AvgGain float64 // 平均涨幅
	AvgLoss float64 // 平均跌幅
}

// InitWilder 标准 Wilder RSI 初始化。
// 用前 period 个价格变化的简单平均作为初始 avgGain/avgLoss，
// 然后对后续所有价格变化做 Wilder 递推。
// 要求 len(closes) > period。
func InitWilder(closes []float64, period int) State {
	// 第1步：用最前面 period 个变化做简单平均
	gains, losses := 0.0, 0.0
	for i := 1; i <= period; i++ {
		diff := closes[i] - closes[i-1]
		if diff > 0 {
			gains += diff
		} else {
			losses -= diff
		}
	}
	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)

	// 第2步：对后续所有变化做 Wilder 递推
	for i := period + 1; i < len(closes); i++ {
		diff := closes[i] - closes[i-1]
		avgGain, avgLoss = WilderStep(avgGain, avgLoss, diff, period)
	}

	return State{AvgGain: avgGain, AvgLoss: avgLoss}
}

// WilderStep 一步 Wilder RSI 递推
func WilderStep(avgGain, avgLoss, diff float64, period int) (newAvgGain, newAvgLoss float64) {
	gain, loss := 0.0, 0.0
	if diff > 0 {
		gain = diff
	} else {
		loss = -diff
	}
	newAvgGain = (avgGain*float64(period-1) + gain) / float64(period)
	newAvgLoss = (avgLoss*float64(period-1) + loss) / float64(period)
	return newAvgGain, newAvgLoss
}

// FromAvgGainLoss 从 avgGain/avgLoss 计算 RSI 值
func FromAvgGainLoss(avgGain, avgLoss float64) float64 {
	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

// Calc 从完整收盘价序列计算 RSI（冷启动场景）
func Calc(closes []float64, period int) float64 {
	state := InitWilder(closes, period)
	return FromAvgGainLoss(state.AvgGain, state.AvgLoss)
}
