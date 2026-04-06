package model

import "time"

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
