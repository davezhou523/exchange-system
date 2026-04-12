package ema

// ---------------------------------------------------------------------------
// EMA（指数移动平均）
// 公式：ema = prevEma + α * (price - prevEma)，α = 2/(period+1)
// 初始化：SMA(period) 作为首值，然后逐根递推
//
// 与交易所/TradingView 对齐，纯函数式实现，不依赖任何业务类型。
// ---------------------------------------------------------------------------

// Alpha 返回 EMA 的平滑因子 α = 2/(period+1)
func Alpha(period int) float64 {
	return 2.0 / float64(period+1)
}

// Init 从收盘价序列初始化 EMA。
// 用前 period 个收盘价的 SMA 作为初始值，然后逐根 EMA 递推。
// 返回最后一根K线的 EMA 值。
// 要求 len(closes) >= period。
func Init(closes []float64, period int) float64 {
	alpha := Alpha(period)
	// SMA 初始化
	sma := 0.0
	for i := 0; i < period; i++ {
		sma += closes[i]
	}
	ema := sma / float64(period)
	// 逐根递推
	for i := period; i < len(closes); i++ {
		ema = ema + alpha*(closes[i]-ema)
	}
	return ema
}

// Step 一步 EMA 递推：ema = prevEma + α * (price - prevEma)
func Step(prevEma, price float64, period int) float64 {
	alpha := Alpha(period)
	return prevEma + alpha*(price-prevEma)
}
