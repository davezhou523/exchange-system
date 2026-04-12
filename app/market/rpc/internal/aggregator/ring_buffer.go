package aggregator

// --- klineRingBuffer: ring buffer for kline history ---

// indicatorState 缓存递推指标的中间状态，避免每次从头重算
// 使用 RMA/Wilder 平滑（指数递推），与交易所/TradingView 指标对齐
type indicatorState struct {
	// EMA 递推状态：ema = prevEma + α * (price - prevEma), α = 2/(period+1)
	ema21     float64 // 上一次 EMA21 值
	ema55     float64 // 上一次 EMA55 值
	ema21Init bool    // EMA21 是否已初始化（至少有 period 根K线）
	ema55Init bool    // EMA55 是否已初始化

	// RSI 递推状态（Wilder/RMA 平滑）：
	// avgGain = (prevAvgGain*(period-1) + currentGain) / period
	// avgLoss = (prevAvgLoss*(period-1) + currentLoss) / period
	avgGain   float64 // 上一次平均涨幅
	avgLoss   float64 // 上一次平均跌幅
	rsiInit   bool    // RSI 是否已初始化（至少有 period 根K线完成首轮计算）
	rsiPeriod int     // RSI 周期（用于递推）

	// ATR 递推状态（RMA/Wilder 平滑）：
	// atr = (prevAtr*(period-1) + currentTR) / period
	atr       float64 // 上一次 ATR 值
	atrInit   bool    // ATR 是否已初始化
	atrPeriod int     // ATR 周期（用于递推）
}

type klineRingBuffer struct {
	closePrices   []float64
	highPrices    []float64
	lowPrices     []float64
	prevCloseVals []float64 // close of previous kline (for ATR)
	openTimes     []int64
	cap           int
	head          int // next write position
	count         int

	// state 缓存递推指标的中间状态，O(1) 计算而非 O(n) 重算
	state indicatorState
}

func newKlineRingBuffer(cap int) *klineRingBuffer {
	return &klineRingBuffer{
		closePrices:   make([]float64, cap),
		highPrices:    make([]float64, cap),
		lowPrices:     make([]float64, cap),
		prevCloseVals: make([]float64, cap),
		openTimes:     make([]int64, cap),
		cap:           cap,
	}
}

func (r *klineRingBuffer) push(open, high, low, close float64, openTime int64) {
	// prevClose is the close of the element just before the new one
	var prevClose float64
	if r.count > 0 {
		prevIdx := (r.head - 1 + r.cap) % r.cap
		prevClose = r.closePrices[prevIdx]
	}

	r.closePrices[r.head] = close
	r.highPrices[r.head] = high
	r.lowPrices[r.head] = low
	r.prevCloseVals[r.head] = prevClose
	r.openTimes[r.head] = openTime
	r.head = (r.head + 1) % r.cap
	if r.count < r.cap {
		r.count++
	}
}

// lastClose 返回最后一根K线的收盘价，用于 ATR 的 prevClose 计算
func (r *klineRingBuffer) lastClose() float64 {
	if r.count == 0 {
		return 0
	}
	idx := (r.head - 1 + r.cap) % r.cap
	return r.closePrices[idx]
}

// prevCloseForLast 返回最后一根K线的 prevClose 值（即倒数第二根K线的收盘价）
// 用于 advanceATRState 中计算 TR（push 后，新K线已成为 history 最后一根）
func (r *klineRingBuffer) prevCloseForLast() float64 {
	if r.count == 0 {
		return 0
	}
	idx := (r.head - 1 + r.cap) % r.cap
	return r.prevCloseVals[idx]
}

// closes returns all close prices in chronological order.
func (r *klineRingBuffer) closes() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.closePrices[idx]
	}
	return result
}

// highs returns all high prices in chronological order.
func (r *klineRingBuffer) highs() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.highPrices[idx]
	}
	return result
}

// lows returns all low prices in chronological order.
func (r *klineRingBuffer) lows() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.lowPrices[idx]
	}
	return result
}

// prevCloses returns all prev close prices in chronological order.
func (r *klineRingBuffer) prevCloses() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.prevCloseVals[idx]
	}
	return result
}

// countBefore returns the number of elements with openTime <= cutoff.
func (r *klineRingBuffer) countBefore(cutoff int64) int {
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		if r.openTimes[idx] > cutoff {
			return i
		}
	}
	return r.count
}

// closesBefore returns close prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) closesBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.closePrices[idx]
	}
	return result
}

// highsBefore returns high prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) highsBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.highPrices[idx]
	}
	return result
}

// lowsBefore returns low prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) lowsBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.lowPrices[idx]
	}
	return result
}

// prevClosesBefore returns prev close prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) prevClosesBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.prevCloseVals[idx]
	}
	return result
}
