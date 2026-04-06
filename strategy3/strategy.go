package strategy3

import (
	"errors"
	"fmt"
	"math"
	"time"
)

type Side string

const (
	SideNone  Side = ""
	SideLong  Side = "long"
	SideShort Side = "short"
)

type Action string

const (
	ActionHold        Action = "hold"
	ActionEnter       Action = "enter"
	ActionExit        Action = "exit"
	ActionPartialExit Action = "partial_exit"
)

type Candle struct {
	OpenTime  time.Time
	CloseTime time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Closed    bool
}

type Params struct {
	H4EmaFast                 int
	H4EmaSlow                 int
	H1EmaFast                 int
	H1EmaSlow                 int
	H1RsiPeriod               int
	M15EmaPeriod              int
	M15AtrPeriod              int
	M15RsiPeriod              int
	RiskPerTrade              float64
	MaxPositionSize           float64
	DeepPullbackScale         float64
	PullbackDeepBand          float64
	StopLossAtrMultiplier     float64
	MinHoldingBars            int
	EmaExitConfirmBars        int
	EmaExitBufferAtr          float64
	Leverage                  float64
	MaxLeverageRatio          float64
	MaxPositions              int
	MaxConsecutiveLosses      int
	MaxDailyLossPct           float64
	MaxDrawdownPct            float64
	DrawdownPositionScale     float64
	RequireBothEntrySignals   bool
	H1RsiLongLow              float64
	H1RsiLongHigh             float64
	H1RsiShortLow             float64
	H1RsiShortHigh            float64
	M15BreakoutLookback       int
	M15RsiBiasLong            float64
	M15RsiBiasShort           float64
	PullbackStructureLookback int
}

func DefaultParams() Params {
	return Params{
		H4EmaFast:                 21,
		H4EmaSlow:                 55,
		H1EmaFast:                 21,
		H1EmaSlow:                 55,
		H1RsiPeriod:               14,
		M15EmaPeriod:              21,
		M15AtrPeriod:              14,
		M15RsiPeriod:              14,
		RiskPerTrade:              0.03,
		MaxPositionSize:           0.55,
		DeepPullbackScale:         0.9,
		PullbackDeepBand:          0.003,
		StopLossAtrMultiplier:     1.5,
		MinHoldingBars:            5,
		EmaExitConfirmBars:        2,
		EmaExitBufferAtr:          0.30,
		Leverage:                  7.0,
		MaxLeverageRatio:          0.92,
		MaxPositions:              1,
		MaxConsecutiveLosses:      3,
		MaxDailyLossPct:           0.07,
		MaxDrawdownPct:            0.15,
		DrawdownPositionScale:     0.5,
		RequireBothEntrySignals:   false,
		H1RsiLongLow:              42,
		H1RsiLongHigh:             60,
		H1RsiShortLow:             40,
		H1RsiShortHigh:            58,
		M15BreakoutLookback:       6,
		M15RsiBiasLong:            52,
		M15RsiBiasShort:           48,
		PullbackStructureLookback: 5,
	}
}

type Account struct {
	Equity        float64
	AvailableCash float64
}

type Position struct {
	Side            Side
	Quantity        float64
	EntryPrice      float64
	StopLoss        float64
	StopDistance    float64
	TakeProfits     []float64
	BarsSinceEntry  int
	FirstTargetHit  bool
	SecondTargetHit bool
	OpenedAt        time.Time
	LastBarTime     time.Time
}

type RiskState struct {
	ConsecutiveLosses int
	DailyLossPct      float64
	PeakEquity        float64
	CurrentDay        string
	TradingPaused     bool
	PauseReason       string
}

type State struct {
	Position *Position
	Risk     RiskState
}

type Snapshot struct {
	Symbol    string
	H4        []Candle
	H1        []Candle
	M15       []Candle
	Timestamp time.Time
}

type Decision struct {
	Action         Action
	Side           Side
	Quantity       float64
	EntryPrice     float64
	ExecutionPrice float64
	StopLoss       float64
	TakeProfits    []float64
	Reason         string
	Trend          Side
	IsDeepPullback bool
	RealizedPnL    float64
	UpdatedState   State
}

type Engine struct {
	params Params
}

func NewEngine(params Params) *Engine {
	return &Engine{params: params}
}

func (e *Engine) Evaluate(snapshot Snapshot, state State, account Account) (Decision, error) {
	if account.Equity <= 0 {
		return Decision{}, errors.New("account equity must be positive")
	}
	if account.AvailableCash <= 0 {
		account.AvailableCash = account.Equity
	}
	if snapshot.Timestamp.IsZero() {
		if len(snapshot.M15) == 0 {
			return Decision{}, errors.New("snapshot timestamp is zero")
		}
		snapshot.Timestamp = snapshot.M15[len(snapshot.M15)-1].CloseTime
	}

	nextState := state
	e.prepareRisk(&nextState, snapshot.Timestamp, account.Equity)

	if nextState.Position != nil {
		return e.manageOpenPosition(snapshot, nextState, account)
	}

	if err := e.validateSnapshot(snapshot); err != nil {
		return Decision{Action: ActionHold, Reason: err.Error(), UpdatedState: nextState}, nil
	}

	if nextState.Risk.TradingPaused {
		return Decision{Action: ActionHold, Reason: nextState.Risk.PauseReason, UpdatedState: nextState}, nil
	}

	trend := e.detectTrend(snapshot.H4)
	if trend == SideNone {
		return Decision{Action: ActionHold, Reason: "4H 趋势未形成", UpdatedState: nextState}, nil
	}

	pullback, ok := e.detectPullback(trend, snapshot.H1)
	if !ok {
		return Decision{Action: ActionHold, Trend: trend, Reason: "1H 回调条件未满足", UpdatedState: nextState}, nil
	}

	entryOK, entryReason := e.detectEntry(trend, snapshot.M15)
	if !entryOK {
		return Decision{Action: ActionHold, Trend: trend, IsDeepPullback: pullback.Deep, Reason: entryReason, UpdatedState: nextState}, nil
	}

	atrSeries, err := ATR(highs(snapshot.M15), lows(snapshot.M15), closes(snapshot.M15), e.params.M15AtrPeriod)
	if err != nil {
		return Decision{}, err
	}
	latestATR, ok := lastFinite(atrSeries)
	if !ok || latestATR <= 0 {
		return Decision{}, errors.New("m15 atr unavailable")
	}

	entryPrice := snapshot.M15[len(snapshot.M15)-1].Close
	stopDistance := latestATR * e.params.StopLossAtrMultiplier
	quantity := e.calculatePositionSize(account, nextState, entryPrice, stopDistance, pullback.Deep)
	if quantity <= 0 {
		return Decision{Action: ActionHold, Trend: trend, Reason: "仓位计算结果为零", UpdatedState: nextState}, nil
	}

	stopLoss := entryPrice - stopDistance
	takeProfits := []float64{entryPrice + stopDistance, entryPrice + 2*stopDistance}
	if trend == SideShort {
		stopLoss = entryPrice + stopDistance
		takeProfits = []float64{entryPrice - stopDistance, entryPrice - 2*stopDistance}
	}

	nextState.Position = &Position{
		Side:         trend,
		Quantity:     quantity,
		EntryPrice:   entryPrice,
		StopLoss:     stopLoss,
		StopDistance: stopDistance,
		TakeProfits:  append([]float64(nil), takeProfits...),
		OpenedAt:     snapshot.Timestamp,
		LastBarTime:  snapshot.Timestamp,
	}

	return Decision{
		Action:         ActionEnter,
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

func (e *Engine) validateSnapshot(snapshot Snapshot) error {
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

func (e *Engine) prepareRisk(state *State, now time.Time, equity float64) {
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

type pullbackResult struct {
	Deep bool
}

func (e *Engine) detectTrend(h4 []Candle) Side {
	closeSeries := closes(h4)
	fastSeries, err := EMA(closeSeries, e.params.H4EmaFast)
	if err != nil {
		return SideNone
	}
	slowSeries, err := EMA(closeSeries, e.params.H4EmaSlow)
	if err != nil {
		return SideNone
	}
	fast, okFast := lastFinite(fastSeries)
	slow, okSlow := lastFinite(slowSeries)
	if !okFast || !okSlow {
		return SideNone
	}
	price := h4[len(h4)-1].Close
	if price > fast && fast > slow {
		return SideLong
	}
	if price < fast && fast < slow {
		return SideShort
	}
	return SideNone
}

func (e *Engine) detectPullback(side Side, h1 []Candle) (pullbackResult, bool) {
	closeSeries := closes(h1)
	fastSeries, err := EMA(closeSeries, e.params.H1EmaFast)
	if err != nil {
		return pullbackResult{}, false
	}
	slowSeries, err := EMA(closeSeries, e.params.H1EmaSlow)
	if err != nil {
		return pullbackResult{}, false
	}
	rsiSeries, err := RSI(closeSeries, e.params.H1RsiPeriod)
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
	case SideLong:
		structureLow := lowestLow(segment)
		ok := fast > price && price > slow &&
			price > structureLow &&
			fast > slow &&
			slopeFast > slopeSlow &&
			rsi >= e.params.H1RsiLongLow &&
			rsi <= e.params.H1RsiLongHigh
		return pullbackResult{Deep: deep}, ok
	case SideShort:
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

func (e *Engine) detectEntry(side Side, m15 []Candle) (bool, string) {
	closeSeries := closes(m15)
	rsiSeries, err := RSI(closeSeries, e.params.M15RsiPeriod)
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
	case SideLong:
		breakout = latestClose > highestHigh(window)
		rsiSignal = (rsi > 50 && prevRSI <= 50) || rsi >= e.params.M15RsiBiasLong
	case SideShort:
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

func (e *Engine) calculatePositionSize(account Account, state State, price, stopDistance float64, deepPullback bool) float64 {
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

func currentDrawdown(peak, equity, fallback float64) float64 {
	if peak <= 0 {
		return fallback
	}
	if equity >= peak {
		return 0
	}
	return (peak - equity) / peak
}

func (e *Engine) manageOpenPosition(snapshot Snapshot, state State, account Account) (Decision, error) {
	if err := e.validateSnapshot(snapshot); err != nil {
		return Decision{Action: ActionHold, Reason: err.Error(), UpdatedState: state}, nil
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
		return Decision{
			Action:         ActionExit,
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
		realized := realizedPnL(Position{
			Side:       position.Side,
			Quantity:   closedQty,
			EntryPrice: position.EntryPrice,
		}, position.TakeProfits[1])
		return Decision{
			Action:         ActionPartialExit,
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
		return Decision{
			Action:         ActionExit,
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
	return Decision{
		Action:       ActionHold,
		Side:         position.Side,
		Reason:       "持仓中",
		UpdatedState: state,
	}, nil
}

func (e *Engine) applyTradeResult(state *State, account Account, pnl float64) {
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

func (e *Engine) stopTriggered(position Position, candle Candle) (float64, bool) {
	switch position.Side {
	case SideLong:
		if candle.Low <= position.StopLoss {
			return position.StopLoss, true
		}
	case SideShort:
		if candle.High >= position.StopLoss {
			return position.StopLoss, true
		}
	}
	return 0, false
}

func (e *Engine) firstTargetReached(position Position, candle Candle) bool {
	if len(position.TakeProfits) == 0 {
		return false
	}
	switch position.Side {
	case SideLong:
		return candle.High >= position.TakeProfits[0]
	case SideShort:
		return candle.Low <= position.TakeProfits[0]
	default:
		return false
	}
}

func (e *Engine) secondTargetReached(position Position, candle Candle) bool {
	if len(position.TakeProfits) < 2 {
		return false
	}
	switch position.Side {
	case SideLong:
		return candle.High >= position.TakeProfits[1]
	case SideShort:
		return candle.Low <= position.TakeProfits[1]
	default:
		return false
	}
}

func (e *Engine) shouldExitByEMA(position Position, m15 []Candle) bool {
	if position.BarsSinceEntry < e.params.MinHoldingBars {
		return false
	}
	closeSeries := closes(m15)
	emaSeries, err := EMA(closeSeries, e.params.M15EmaPeriod)
	if err != nil {
		return false
	}
	atrSeries, err := ATR(highs(m15), lows(m15), closeSeries, e.params.M15AtrPeriod)
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
		case SideLong:
			if m15[i].Close >= emaSeries[i]-buffer {
				return false
			}
		case SideShort:
			if m15[i].Close <= emaSeries[i]+buffer {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func realizedPnL(position Position, exitPrice float64) float64 {
	switch position.Side {
	case SideLong:
		return (exitPrice - position.EntryPrice) * position.Quantity
	case SideShort:
		return (position.EntryPrice - exitPrice) * position.Quantity
	default:
		return 0
	}
}

func EMA(values []float64, period int) ([]float64, error) {
	if period <= 0 {
		return nil, errors.New("ema period must be positive")
	}
	if len(values) < period {
		return nil, errors.New("insufficient data for ema")
	}
	result := make([]float64, len(values))
	for i := range result {
		result[i] = math.NaN()
	}
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += values[i]
	}
	result[period-1] = sum / float64(period)
	multiplier := 2.0 / float64(period+1)
	for i := period; i < len(values); i++ {
		result[i] = (values[i]-result[i-1])*multiplier + result[i-1]
	}
	return result, nil
}

func RSI(values []float64, period int) ([]float64, error) {
	if period <= 0 {
		return nil, errors.New("rsi period must be positive")
	}
	if len(values) <= period {
		return nil, errors.New("insufficient data for rsi")
	}
	result := make([]float64, len(values))
	for i := range result {
		result[i] = math.NaN()
	}

	gains := 0.0
	losses := 0.0
	for i := 1; i <= period; i++ {
		change := values[i] - values[i-1]
		if change > 0 {
			gains += change
		} else {
			losses -= change
		}
	}

	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)
	result[period] = rsiFromAverage(avgGain, avgLoss)
	for i := period + 1; i < len(values); i++ {
		change := values[i] - values[i-1]
		gain := 0.0
		loss := 0.0
		if change > 0 {
			gain = change
		} else {
			loss = -change
		}
		avgGain = ((avgGain * float64(period-1)) + gain) / float64(period)
		avgLoss = ((avgLoss * float64(period-1)) + loss) / float64(period)
		result[i] = rsiFromAverage(avgGain, avgLoss)
	}
	return result, nil
}

func ATR(highs, lows, closes []float64, period int) ([]float64, error) {
	if period <= 0 {
		return nil, errors.New("atr period must be positive")
	}
	if len(highs) != len(lows) || len(lows) != len(closes) {
		return nil, errors.New("atr series length mismatch")
	}
	if len(highs) <= period {
		return nil, errors.New("insufficient data for atr")
	}

	tr := make([]float64, len(highs))
	tr[0] = highs[0] - lows[0]
	for i := 1; i < len(highs); i++ {
		rangeHL := highs[i] - lows[i]
		rangeHC := math.Abs(highs[i] - closes[i-1])
		rangeLC := math.Abs(lows[i] - closes[i-1])
		tr[i] = maxFloat(rangeHL, maxFloat(rangeHC, rangeLC))
	}

	result := make([]float64, len(highs))
	for i := range result {
		result[i] = math.NaN()
	}
	sum := 0.0
	for i := 1; i <= period; i++ {
		sum += tr[i]
	}
	result[period] = sum / float64(period)
	for i := period + 1; i < len(tr); i++ {
		result[i] = ((result[i-1] * float64(period-1)) + tr[i]) / float64(period)
	}
	return result, nil
}

func rsiFromAverage(avgGain, avgLoss float64) float64 {
	if avgLoss == 0 {
		return 100
	}
	rs := avgGain / avgLoss
	return 100 - 100/(1+rs)
}

func lastFinite(values []float64) (float64, bool) {
	for i := len(values) - 1; i >= 0; i-- {
		if !math.IsNaN(values[i]) && !math.IsInf(values[i], 0) {
			return values[i], true
		}
	}
	return 0, false
}

func closes(candles []Candle) []float64 {
	result := make([]float64, len(candles))
	for i, candle := range candles {
		result[i] = candle.Close
	}
	return result
}

func highs(candles []Candle) []float64 {
	result := make([]float64, len(candles))
	for i, candle := range candles {
		result[i] = candle.High
	}
	return result
}

func lows(candles []Candle) []float64 {
	result := make([]float64, len(candles))
	for i, candle := range candles {
		result[i] = candle.Low
	}
	return result
}

func lowestLow(candles []Candle) float64 {
	value := candles[0].Low
	for _, candle := range candles[1:] {
		if candle.Low < value {
			value = candle.Low
		}
	}
	return value
}

func highestHigh(candles []Candle) float64 {
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
