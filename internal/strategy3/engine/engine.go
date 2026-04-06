package engine

import (
	"fmt"
	"math"
	"time"

	"exchange-system/internal/strategy3/indicator"
	"exchange-system/internal/strategy3/model"
)

type Engine struct {
	params model.Params
}

type pullbackResult struct {
	Deep bool
}

func DefaultParams() model.Params {
	return model.DefaultParams()
}

func New(params model.Params) *Engine {
	return &Engine{params: params}
}

func (e *Engine) Evaluate(snapshot model.Snapshot, state model.State, account model.Account) (model.Decision, error) {
	if account.Equity <= 0 {
		return model.Decision{}, fmt.Errorf("account equity must be positive")
	}
	if account.AvailableCash <= 0 {
		account.AvailableCash = account.Equity
	}
	if snapshot.Timestamp.IsZero() {
		if len(snapshot.M15) == 0 {
			return model.Decision{}, fmt.Errorf("snapshot timestamp is zero")
		}
		snapshot.Timestamp = snapshot.M15[len(snapshot.M15)-1].CloseTime
	}
	nextState := state
	e.prepareRisk(&nextState, snapshot.Timestamp, account.Equity)
	if nextState.Position != nil {
		return e.manageOpenPosition(snapshot, nextState, account)
	}
	if err := e.validateSnapshot(snapshot); err != nil {
		return model.Decision{Action: model.ActionHold, Reason: err.Error(), UpdatedState: nextState}, nil
	}
	if nextState.Risk.TradingPaused {
		return model.Decision{Action: model.ActionHold, Reason: nextState.Risk.PauseReason, UpdatedState: nextState}, nil
	}
	trend := e.detectTrend(snapshot.H4)
	if trend == model.SideNone {
		return model.Decision{Action: model.ActionHold, Reason: "4H 趋势未形成", UpdatedState: nextState}, nil
	}
	pullback, ok := e.detectPullback(trend, snapshot.H1)
	if !ok {
		return model.Decision{Action: model.ActionHold, Trend: trend, Reason: "1H 回调条件未满足", UpdatedState: nextState}, nil
	}
	entryOK, entryReason := e.detectEntry(trend, snapshot.M15)
	if !entryOK {
		return model.Decision{Action: model.ActionHold, Trend: trend, IsDeepPullback: pullback.Deep, Reason: entryReason, UpdatedState: nextState}, nil
	}
	atrSeries, err := indicator.ATR(highs(snapshot.M15), lows(snapshot.M15), closes(snapshot.M15), e.params.M15AtrPeriod)
	if err != nil {
		return model.Decision{}, err
	}
	latestATR, ok := indicator.LastFinite(atrSeries)
	if !ok || latestATR <= 0 {
		return model.Decision{}, fmt.Errorf("m15 atr unavailable")
	}
	entryPrice := snapshot.M15[len(snapshot.M15)-1].Close
	stopDistance := latestATR * e.params.StopLossAtrMultiplier
	quantity := e.calculatePositionSize(account, nextState, entryPrice, stopDistance, pullback.Deep)
	if quantity <= 0 {
		return model.Decision{Action: model.ActionHold, Trend: trend, Reason: "仓位计算结果为零", UpdatedState: nextState}, nil
	}
	stopLoss := entryPrice - stopDistance
	takeProfits := []float64{entryPrice + stopDistance, entryPrice + 2*stopDistance}
	if trend == model.SideShort {
		stopLoss = entryPrice + stopDistance
		takeProfits = []float64{entryPrice - stopDistance, entryPrice - 2*stopDistance}
	}
	nextState.Position = &model.Position{
		Side:         trend,
		Quantity:     quantity,
		EntryPrice:   entryPrice,
		StopLoss:     stopLoss,
		StopDistance: stopDistance,
		TakeProfits:  append([]float64(nil), takeProfits...),
		OpenedAt:     snapshot.Timestamp,
		LastBarTime:  snapshot.Timestamp,
	}
	return model.Decision{
		Action:         model.ActionEnter,
		Side:           trend,
		Quantity:       quantity,
		EntryPrice:     entryPrice,
		ExecutionPrice: entryPrice,
		StopLoss:       stopLoss,
		TakeProfits:    takeProfits,
		Trend:          trend,
		IsDeepPullback: pullback.Deep,
		Reason:         "满足多周期趋势、回调和 15M 入场条件",
		UpdatedState:   nextState,
	}, nil
}

func (e *Engine) validateSnapshot(snapshot model.Snapshot) error {
	if len(snapshot.H4) < e.params.H4EmaSlow {
		return fmt.Errorf("4H 数据不足，至少需要 %d 根K线", e.params.H4EmaSlow)
	}
	if len(snapshot.H1) < e.params.H1EmaSlow+5 {
		return fmt.Errorf("1H 数据不足，至少需要 %d 根K线", e.params.H1EmaSlow+5)
	}
	minM15 := maxInt(8, e.params.M15BreakoutLookback+2)
	if len(snapshot.M15) < minM15 {
		return fmt.Errorf("15M 数据不足，至少需要 %d 根K线", minM15)
	}
	return nil
}

func (e *Engine) prepareRisk(state *model.State, now time.Time, equity float64) {
	day := now.Format("2006-01-02")
	if state.Risk.CurrentDay == "" {
		state.Risk.CurrentDay = day
	}
	if state.Risk.CurrentDay != day {
		state.Risk.CurrentDay = day
		state.Risk.DailyLossPct = 0
		if state.Risk.PauseReason == "达到最大日亏损限制" {
			state.Risk.TradingPaused = false
			state.Risk.PauseReason = ""
		}
	}
	if state.Risk.PeakEquity == 0 || equity > state.Risk.PeakEquity {
		state.Risk.PeakEquity = equity
	}
	if state.Risk.ConsecutiveLosses >= e.params.MaxConsecutiveLosses {
		state.Risk.TradingPaused = true
		state.Risk.PauseReason = "达到最大连续亏损限制"
	}
	if state.Risk.DailyLossPct >= e.params.MaxDailyLossPct {
		state.Risk.TradingPaused = true
		state.Risk.PauseReason = "达到最大日亏损限制"
	}
}

func (e *Engine) detectTrend(h4 []model.Candle) model.Side {
	closeSeries := closes(h4)
	fastSeries, err := indicator.EMA(closeSeries, e.params.H4EmaFast)
	if err != nil {
		return model.SideNone
	}
	slowSeries, err := indicator.EMA(closeSeries, e.params.H4EmaSlow)
	if err != nil {
		return model.SideNone
	}
	fast, okFast := indicator.LastFinite(fastSeries)
	slow, okSlow := indicator.LastFinite(slowSeries)
	if !okFast || !okSlow {
		return model.SideNone
	}
	price := h4[len(h4)-1].Close
	if price > fast && fast > slow {
		return model.SideLong
	}
	if price < fast && fast < slow {
		return model.SideShort
	}
	return model.SideNone
}

func (e *Engine) detectPullback(side model.Side, h1 []model.Candle) (pullbackResult, bool) {
	closeSeries := closes(h1)
	fastSeries, err := indicator.EMA(closeSeries, e.params.H1EmaFast)
	if err != nil {
		return pullbackResult{}, false
	}
	slowSeries, err := indicator.EMA(closeSeries, e.params.H1EmaSlow)
	if err != nil {
		return pullbackResult{}, false
	}
	rsiSeries, err := indicator.RSI(closeSeries, e.params.H1RsiPeriod)
	if err != nil {
		return pullbackResult{}, false
	}
	idx := len(h1) - 1
	if idx == 0 {
		return pullbackResult{}, false
	}
	price := h1[idx].Close
	fast := fastSeries[idx]
	slow := slowSeries[idx]
	rsi := rsiSeries[idx]
	slopeIndex := maxInt(0, idx-3)
	slopeFast := fastSeries[slopeIndex]
	slopeSlow := slowSeries[slopeIndex]
	if math.IsNaN(fast) || math.IsNaN(slow) || math.IsNaN(slopeFast) || math.IsNaN(slopeSlow) || math.IsNaN(rsi) {
		return pullbackResult{}, false
	}
	lookback := minInt(e.params.PullbackStructureLookback, len(h1)-1)
	segment := h1[len(h1)-1-lookback : len(h1)-1]
	deep := math.Abs(price-slow)/slow <= e.params.PullbackDeepBand
	switch side {
	case model.SideLong:
		structureLow := lowestLow(segment)
		ok := fast > price && price > slow &&
			price > structureLow &&
			fast > slow &&
			slopeFast > slopeSlow &&
			rsi >= e.params.H1RsiLongLow &&
			rsi <= e.params.H1RsiLongHigh
		return pullbackResult{Deep: deep}, ok
	case model.SideShort:
		structureHigh := highestHigh(segment)
		ok := fast < price && price < slow &&
			price < structureHigh &&
			fast < slow &&
			slopeFast < slopeSlow &&
			rsi >= e.params.H1RsiShortLow &&
			rsi <= e.params.H1RsiShortHigh
		return pullbackResult{Deep: deep}, ok
	default:
		return pullbackResult{}, false
	}
}

func (e *Engine) detectEntry(side model.Side, m15 []model.Candle) (bool, string) {
	closeSeries := closes(m15)
	rsiSeries, err := indicator.RSI(closeSeries, e.params.M15RsiPeriod)
	if err != nil {
		return false, "15M RSI 不可用"
	}
	idx := len(m15) - 1
	if idx == 0 {
		return false, "15M 数据不足"
	}
	rsi := rsiSeries[idx]
	prevRSI := rsiSeries[idx-1]
	if math.IsNaN(rsi) || math.IsNaN(prevRSI) {
		return false, "15M RSI 数据不足"
	}
	lookback := minInt(e.params.M15BreakoutLookback, len(m15)-1)
	window := m15[len(m15)-1-lookback : len(m15)-1]
	latestClose := m15[idx].Close
	var breakout bool
	var rsiSignal bool
	switch side {
	case model.SideLong:
		breakout = latestClose > highestHigh(window)
		rsiSignal = (rsi > 50 && prevRSI <= 50) || rsi >= e.params.M15RsiBiasLong
	case model.SideShort:
		breakout = latestClose < lowestLow(window)
		rsiSignal = (rsi < 50 && prevRSI >= 50) || rsi <= e.params.M15RsiBiasShort
	default:
		return false, "趋势方向无效"
	}
	if e.params.RequireBothEntrySignals {
		if breakout && rsiSignal {
			return true, "结构突破与 RSI 同时满足"
		}
		return false, "结构突破与 RSI 未同时满足"
	}
	if breakout || rsiSignal {
		return true, "结构突破或 RSI 条件满足"
	}
	return false, "15M 入场信号未触发"
}

func (e *Engine) calculatePositionSize(account model.Account, state model.State, price, stopDistance float64, deepPullback bool) float64 {
	if price <= 0 || stopDistance <= 0 {
		return 0
	}
	riskPosition := account.Equity * e.params.RiskPerTrade / stopDistance
	cashLimit := account.AvailableCash * e.params.MaxPositionSize / price
	leverageLimit := account.Equity * e.params.Leverage * e.params.MaxLeverageRatio / price
	base := minFloat(riskPosition, cashLimit, leverageLimit)
	if base <= 0 {
		return 0
	}
	scale := 1.0
	drawdown := currentDrawdown(state.Risk.PeakEquity, account.Equity, 0)
	if drawdown >= e.params.MaxDrawdownPct {
		scale *= e.params.DrawdownPositionScale
	}
	if deepPullback {
		scale *= e.params.DeepPullbackScale
	}
	return base * scale
}

func (e *Engine) manageOpenPosition(snapshot model.Snapshot, state model.State, account model.Account) (model.Decision, error) {
	if err := e.validateSnapshot(snapshot); err != nil {
		return model.Decision{Action: model.ActionHold, Reason: err.Error(), UpdatedState: state}, nil
	}
	position := *state.Position
	latest := snapshot.M15[len(snapshot.M15)-1]
	if latest.CloseTime.After(position.LastBarTime) {
		position.BarsSinceEntry++
		position.LastBarTime = latest.CloseTime
	}
	if exitPrice, hit := e.stopTriggered(position, latest); hit {
		nextState := state
		realized := realizedPnL(position, exitPrice)
		e.applyTradeResult(&nextState, account, realized)
		nextState.Position = nil
		return model.Decision{
			Action:         model.ActionExit,
			Side:           position.Side,
			EntryPrice:     position.EntryPrice,
			ExecutionPrice: exitPrice,
			Quantity:       position.Quantity,
			StopLoss:       position.StopLoss,
			Reason:         "触发止损",
			RealizedPnL:    realized,
			UpdatedState:   nextState,
		}, nil
	}
	if !position.FirstTargetHit && e.firstTargetReached(position, latest) {
		position.FirstTargetHit = true
		position.StopLoss = position.EntryPrice
	}
	if !position.SecondTargetHit && e.secondTargetReached(position, latest) {
		nextState := state
		closedQty := position.Quantity / 2
		position.Quantity -= closedQty
		position.SecondTargetHit = true
		nextState.Position = &position
		realized := realizedPnL(model.Position{
			Side:       position.Side,
			Quantity:   closedQty,
			EntryPrice: position.EntryPrice,
		}, position.TakeProfits[1])
		return model.Decision{
			Action:         model.ActionPartialExit,
			Side:           position.Side,
			Quantity:       closedQty,
			EntryPrice:     position.EntryPrice,
			ExecutionPrice: position.TakeProfits[1],
			Reason:         "达到第二止盈位，平仓一半",
			RealizedPnL:    realized,
			UpdatedState:   nextState,
		}, nil
	}
	if e.shouldExitByEMA(position, snapshot.M15) {
		nextState := state
		realized := realizedPnL(position, latest.Close)
		e.applyTradeResult(&nextState, account, realized)
		nextState.Position = nil
		return model.Decision{
			Action:         model.ActionExit,
			Side:           position.Side,
			Quantity:       position.Quantity,
			EntryPrice:     position.EntryPrice,
			ExecutionPrice: latest.Close,
			Reason:         "EMA 破位确认出场",
			RealizedPnL:    realized,
			UpdatedState:   nextState,
		}, nil
	}
	state.Position = &position
	return model.Decision{
		Action:       model.ActionHold,
		Side:         position.Side,
		Reason:       "持仓中",
		UpdatedState: state,
	}, nil
}

func (e *Engine) applyTradeResult(state *model.State, account model.Account, pnl float64) {
	if pnl < 0 {
		lossPct := math.Abs(pnl) / maxFloat(account.Equity, 1)
		state.Risk.DailyLossPct += lossPct
		state.Risk.ConsecutiveLosses++
	} else {
		state.Risk.ConsecutiveLosses = 0
	}
	if state.Risk.ConsecutiveLosses >= e.params.MaxConsecutiveLosses {
		state.Risk.TradingPaused = true
		state.Risk.PauseReason = "达到最大连续亏损限制"
	}
	if state.Risk.DailyLossPct >= e.params.MaxDailyLossPct {
		state.Risk.TradingPaused = true
		state.Risk.PauseReason = "达到最大日亏损限制"
	}
}

func (e *Engine) stopTriggered(position model.Position, candle model.Candle) (float64, bool) {
	switch position.Side {
	case model.SideLong:
		if candle.Low <= position.StopLoss {
			return position.StopLoss, true
		}
	case model.SideShort:
		if candle.High >= position.StopLoss {
			return position.StopLoss, true
		}
	}
	return 0, false
}

func (e *Engine) firstTargetReached(position model.Position, candle model.Candle) bool {
	if len(position.TakeProfits) == 0 {
		return false
	}
	switch position.Side {
	case model.SideLong:
		return candle.High >= position.TakeProfits[0]
	case model.SideShort:
		return candle.Low <= position.TakeProfits[0]
	default:
		return false
	}
}

func (e *Engine) secondTargetReached(position model.Position, candle model.Candle) bool {
	if len(position.TakeProfits) < 2 {
		return false
	}
	switch position.Side {
	case model.SideLong:
		return candle.High >= position.TakeProfits[1]
	case model.SideShort:
		return candle.Low <= position.TakeProfits[1]
	default:
		return false
	}
}

func (e *Engine) shouldExitByEMA(position model.Position, m15 []model.Candle) bool {
	if position.BarsSinceEntry < e.params.MinHoldingBars {
		return false
	}
	closeSeries := closes(m15)
	emaSeries, err := indicator.EMA(closeSeries, e.params.M15EmaPeriod)
	if err != nil {
		return false
	}
	atrSeries, err := indicator.ATR(highs(m15), lows(m15), closeSeries, e.params.M15AtrPeriod)
	if err != nil {
		return false
	}
	confirmBars := minInt(e.params.EmaExitConfirmBars, len(m15))
	for i := len(m15) - confirmBars; i < len(m15); i++ {
		if i < 0 || math.IsNaN(emaSeries[i]) || math.IsNaN(atrSeries[i]) {
			return false
		}
		buffer := atrSeries[i] * e.params.EmaExitBufferAtr
		switch position.Side {
		case model.SideLong:
			if m15[i].Close >= emaSeries[i]-buffer {
				return false
			}
		case model.SideShort:
			if m15[i].Close <= emaSeries[i]+buffer {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func currentDrawdown(peak, equity, fallback float64) float64 {
	if peak <= 0 {
		return fallback
	}
	if equity >= peak {
		return 0
	}
	return (peak - equity) / peak
}

func realizedPnL(position model.Position, exitPrice float64) float64 {
	switch position.Side {
	case model.SideLong:
		return (exitPrice - position.EntryPrice) * position.Quantity
	case model.SideShort:
		return (position.EntryPrice - exitPrice) * position.Quantity
	default:
		return 0
	}
}

func closes(candles []model.Candle) []float64 {
	result := make([]float64, len(candles))
	for i, candle := range candles {
		result[i] = candle.Close
	}
	return result
}

func highs(candles []model.Candle) []float64 {
	result := make([]float64, len(candles))
	for i, candle := range candles {
		result[i] = candle.High
	}
	return result
}

func lows(candles []model.Candle) []float64 {
	result := make([]float64, len(candles))
	for i, candle := range candles {
		result[i] = candle.Low
	}
	return result
}

func lowestLow(candles []model.Candle) float64 {
	value := candles[0].Low
	for _, candle := range candles[1:] {
		if candle.Low < value {
			value = candle.Low
		}
	}
	return value
}

func highestHigh(candles []model.Candle) float64 {
	value := candles[0].High
	for _, candle := range candles[1:] {
		if candle.High > value {
			value = candle.High
		}
	}
	return value
}

func minFloat(values ...float64) float64 {
	value := values[0]
	for _, v := range values[1:] {
		if v < value {
			value = v
		}
	}
	return value
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
