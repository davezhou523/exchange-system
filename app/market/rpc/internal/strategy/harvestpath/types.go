package harvestpath

type EntrySide string

const (
	EntrySideLong  EntrySide = "LONG"
	EntrySideShort EntrySide = "SHORT"
)

type TargetSide string

const (
	TargetSideUp   TargetSide = "UP"
	TargetSideDown TargetSide = "DOWN"
)

const (
	ActionWaitForReclaim  = "WAIT_FOR_RECLAIM"
	ActionReduceProbeSize = "REDUCE_PROBE_SIZE"
	ActionFollowPath      = "FOLLOW_PATH"
)

const (
	RiskLevelPathAlert    = "PATH_ALERT"
	RiskLevelPathPressure = "PATH_PRESSURE"
	RiskLevelPathClear    = "PATH_CLEAR"
)

type Candle struct {
	OpenTime       int64
	CloseTime      int64
	Open           float64
	High           float64
	Low            float64
	Close          float64
	Volume         float64
	QuoteVolume    float64
	TakerBuyVolume float64
	Atr            float64
}

type BookLevel struct {
	Price    float64
	Quantity float64
}

type OrderBookSnapshot struct {
	Timestamp int64
	Bids      []BookLevel
	Asks      []BookLevel
}

type BookFeatures struct {
	Probability         float64
	SpreadBps           float64
	TopLevelImbalance   float64
	DepthImbalance      float64
	DirectionalPressure float64
	Summary             string
}

type Context struct {
	Symbol    string
	EventTime int64
	LastPrice float64
	EntrySide EntrySide
	Candles1m []Candle
	OrderBook *OrderBookSnapshot
}

type StopZone struct {
	Side             TargetSide
	ZoneLow          float64
	ZoneHigh         float64
	ReferencePrice   float64
	Touches          int
	DistanceToMarket float64
	StopDensityScore float64
}

type TriggerAnalysis struct {
	VolumeBurstScore         float64
	TradeImbalanceScore      float64
	VolatilityExpansionScore float64
	TimeWindowScore          float64
	TriggerScore             float64
}

type Signal struct {
	Symbol                 string
	EventTime              int64
	TargetSide             TargetSide
	TargetZoneLow          float64
	TargetZoneHigh         float64
	ReferencePrice         float64
	MarketPrice            float64
	StopDensityScore       float64
	TriggerScore           float64
	RuleProbability        float64
	LSTMProbability        float64
	BookProbability        float64
	BookSpreadBps          float64
	BookImbalance          float64
	BookPressure           float64
	BookSummary            string
	HarvestPathProbability float64
	ExpectedPathDepth      float64
	ExpectedReversalSpeed  float64
	PathAction             string
	RiskLevel              string
}
