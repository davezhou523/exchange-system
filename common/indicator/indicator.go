package indicator

// ---------------------------------------------------------------------------
// 公共指标辅助函数
// 提供 TrueRange、prevClose 等通用辅助，被各子包（ema/atr/rsi）或业务层引用。
//
// 指标算法已按类型拆分到独立子包：
//   - ema:  EMA（指数移动平均）
//   - atr:  ATR（平均真实范围）+ TrueRange
//   - rsi:  RSI（相对强弱指数）
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

// PrevClosesFromCloses 从收盘价序列生成 prevClose 序列。
// prevCloses[0] = 0（第1根K线无前收盘价），prevCloses[i] = closes[i-1]
func PrevClosesFromCloses(closes []float64) []float64 {
	n := len(closes)
	prevCloses := make([]float64, n)
	for i := 1; i < n; i++ {
		prevCloses[i] = closes[i-1]
	}
	// prevCloses[0] = 0 (zero value)
	return prevCloses
}
