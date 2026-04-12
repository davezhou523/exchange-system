package atr

import "math"

// ---------------------------------------------------------------------------
// ATR（平均真实范围）- RMA/Wilder 平滑（与 Binance/TradingView 对齐）
// 公式：ATR = (prevATR*(period-1) + currentTR) / period  （即 RMA）
// 初始化：前 period 个 TR 的简单平均，然后逐根 RMA 递推
// TR = max(H-L, |H-prevClose|, |L-prevClose|)
//
// 注：TradingView/Binance 的 ATR 使用 RMA（Running Moving Average），
//     也叫 Wilder 平滑，α = 1/period。
//     与 EMA（α=2/(period+1)）不同，RMA 对近期数据权重更低，更平滑。
//
// 纯函数式实现，不依赖任何业务类型。
// ---------------------------------------------------------------------------

// State ATR 递推状态
type State struct {
	ATR float64 // 当前 ATR 值
}

// TrueRange 计算 True Range = max(H-L, |H-prevClose|, |L-prevClose|)
// prevClose <= 0 时（第1根K线），TR = H-L
func TrueRange(high, low, prevClose float64) float64 {
	tr := high - low
	if prevClose > 0 {
		diff1 := math.Abs(high - prevClose)
		diff2 := math.Abs(low - prevClose)
		if diff1 > tr {
			tr = diff1
		}
		if diff2 > tr {
			tr = diff2
		}
	}
	return tr
}

// ComputeTRs 批量计算 True Range 序列
// highs, lows, prevCloses 长度必须相同
// prevCloses[0] 为第1根K线的前收盘价，通常传 0
func ComputeTRs(highs, lows, prevCloses []float64) []float64 {
	n := len(highs)
	trs := make([]float64, n)
	for i := 0; i < n; i++ {
		trs[i] = TrueRange(highs[i], lows[i], prevCloses[i])
	}
	return trs
}

// InitRma 标准 RMA/Wilder ATR 初始化。
// 用前 period 个 TR 的 SMA 作为初始 ATR，然后逐根 RMA 递推。
// highs, lows, prevCloses 长度必须相同，且 >= period。
// prevCloses[0] 为第1根K线的前收盘价，通常传 0。
func InitRma(highs, lows, prevCloses []float64, period int) State {
	trs := ComputeTRs(highs, lows, prevCloses)
	return InitRmaFromTRs(trs, period)
}

// InitRmaFromTRs 从 TR 序列初始化 ATR。
// 用前 period 个 TR 的 SMA 作为初始 ATR，然后逐根 RMA 递推。
func InitRmaFromTRs(trs []float64, period int) State {
	// 用最前面 period 个 TR 的 SMA 作为初始 ATR
	totalTR := 0.0
	for i := 0; i < period; i++ {
		totalTR += trs[i]
	}
	atr := totalTR / float64(period)

	// 对后续所有 TR 做 RMA 递推
	for i := period; i < len(trs); i++ {
		atr = RmaStep(atr, trs[i], period)
	}

	return State{ATR: atr}
}

// RmaStep 一步 RMA/Wilder ATR 递推：ATR = (prevATR*(period-1) + currentTR) / period
func RmaStep(prevATR, currentTR float64, period int) float64 {
	return (prevATR*float64(period-1) + currentTR) / float64(period)
}
