package harvestpath

import (
	"errors"
	"math"
	"time"
)

const (
	defaultLookback       = 20
	minRequiredCandles    = 8
	defaultBlockThreshold = 0.80
)

type Detector struct {
	Lookback       int
	BlockThreshold float64
}

func NewDetector(lookback int, blockThreshold float64) *Detector {
	if lookback <= 0 {
		lookback = defaultLookback
	}
	if blockThreshold <= 0 {
		blockThreshold = defaultBlockThreshold
	}
	return &Detector{
		Lookback:       lookback,
		BlockThreshold: blockThreshold,
	}
}

func (d *Detector) Evaluate(input Context) (*Signal, error) {
	if len(input.Candles1m) < minRequiredCandles {
		return nil, errors.New("not enough 1m candles")
	}
	if input.LastPrice <= 0 {
		return nil, errors.New("last price is required")
	}

	lookback := d.Lookback
	if lookback <= 0 {
		lookback = defaultLookback
	}
	if lookback > len(input.Candles1m) {
		lookback = len(input.Candles1m)
	}
	window := input.Candles1m[len(input.Candles1m)-lookback:]
	if len(window) < minRequiredCandles {
		return nil, errors.New("lookback window is too short")
	}

	stopZone := d.buildStopZone(window, input.LastPrice, input.EntrySide)
	trigger := d.analyzeTrigger(window, input.EntrySide, input.EventTime)
	avgRange := averageRange(window)
	if avgRange <= 0 {
		avgRange = input.LastPrice * 0.001
	}

	harvestPathProbability := clamp(stopZone.StopDensityScore*0.60+trigger.TriggerScore*0.40, 0, 1)
	expectedDepth := math.Max(avgRange, stopZone.DistanceToMarket+avgRange*0.35)
	expectedReversalSpeed := clamp(trigger.TriggerScore*0.55+stopZone.StopDensityScore*0.45, 0, 1)

	signal := &Signal{
		Symbol:                 input.Symbol,
		EventTime:              input.EventTime,
		TargetSide:             stopZone.Side,
		TargetZoneLow:          stopZone.ZoneLow,
		TargetZoneHigh:         stopZone.ZoneHigh,
		ReferencePrice:         stopZone.ReferencePrice,
		MarketPrice:            input.LastPrice,
		StopDensityScore:       stopZone.StopDensityScore,
		TriggerScore:           trigger.TriggerScore,
		RuleProbability:        harvestPathProbability,
		HarvestPathProbability: harvestPathProbability,
		ExpectedPathDepth:      expectedDepth,
		ExpectedReversalSpeed:  expectedReversalSpeed,
	}
	signal.PathAction, signal.RiskLevel = d.classify(harvestPathProbability)
	return signal, nil
}

func (d *Detector) BlendLSTMProbability(signal *Signal, lstmProbability, weight float64) {
	if d == nil || signal == nil {
		return
	}
	lstmProbability = clamp(lstmProbability, 0, 1)
	signal.LSTMProbability = lstmProbability
	d.BlendModelProbabilities(signal, weight, 0)
}

func (d *Detector) BlendBookProbability(signal *Signal, bookProbability, spreadBps, imbalance, pressure float64, summary string, weight float64) {
	if d == nil || signal == nil {
		return
	}
	signal.BookProbability = clamp(bookProbability, 0, 1)
	signal.BookSpreadBps = math.Max(spreadBps, 0)
	signal.BookImbalance = clamp(imbalance, -1, 1)
	signal.BookPressure = clamp(pressure, 0, 1)
	signal.BookSummary = summary
	d.BlendModelProbabilities(signal, 0, weight)
}

func (d *Detector) BlendModelProbabilities(signal *Signal, lstmWeight, bookWeight float64) {
	if d == nil || signal == nil {
		return
	}
	ruleWeight := 1.0 - clamp(lstmWeight, 0, 1) - clamp(bookWeight, 0, 1)
	if ruleWeight < 0 {
		ruleWeight = 0
	}
	totalWeight := ruleWeight
	finalProbability := signal.RuleProbability * ruleWeight
	if signal.LSTMProbability > 0 && lstmWeight > 0 {
		finalProbability += signal.LSTMProbability * lstmWeight
		totalWeight += lstmWeight
	}
	if signal.BookProbability > 0 && bookWeight > 0 {
		finalProbability += signal.BookProbability * bookWeight
		totalWeight += bookWeight
	}
	if totalWeight <= 0 {
		signal.HarvestPathProbability = clamp(signal.RuleProbability, 0, 1)
	} else {
		signal.HarvestPathProbability = clamp(finalProbability/totalWeight, 0, 1)
	}
	signal.PathAction, signal.RiskLevel = d.classify(signal.HarvestPathProbability)
}

func (d *Detector) ShouldBlock(signal *Signal) bool {
	if signal == nil {
		return false
	}
	return signal.HarvestPathProbability >= d.BlockThreshold
}

func (d *Detector) buildStopZone(window []Candle, lastPrice float64, entrySide EntrySide) StopZone {
	side := TargetSideUp
	ref := window[0].High
	if entrySide == EntrySideShort {
		side = TargetSideDown
		ref = window[0].Low
	}

	for _, candle := range window {
		if side == TargetSideUp && candle.High > ref {
			ref = candle.High
		}
		if side == TargetSideDown && candle.Low < ref {
			ref = candle.Low
		}
	}

	tolerance := averageRange(window) * 0.35
	if tolerance <= 0 {
		tolerance = math.Max(lastPrice*0.0008, 0.01)
	}

	touches := 0
	for _, candle := range window {
		price := candle.High
		if side == TargetSideDown {
			price = candle.Low
		}
		if math.Abs(price-ref) <= tolerance {
			touches++
		}
	}

	distance := 0.0
	if side == TargetSideUp && ref > lastPrice {
		distance = ref - lastPrice
	}
	if side == TargetSideDown && ref < lastPrice {
		distance = lastPrice - ref
	}

	maxDistance := math.Max(averageATR(window)*2.5, tolerance*3)
	proximityScore := 1.0
	if maxDistance > 0 && distance > 0 {
		proximityScore = 1 - math.Min(distance/maxDistance, 1)
	}
	touchScore := math.Min(float64(touches)/3.0, 1.0)
	structureScore := 1.0
	if distance == 0 {
		structureScore = 0.75
	}
	stopDensityScore := clamp(touchScore*0.45+proximityScore*0.35+structureScore*0.20, 0, 1)

	return StopZone{
		Side:             side,
		ZoneLow:          ref - tolerance,
		ZoneHigh:         ref + tolerance,
		ReferencePrice:   ref,
		Touches:          touches,
		DistanceToMarket: distance,
		StopDensityScore: stopDensityScore,
	}
}

func (d *Detector) analyzeTrigger(window []Candle, entrySide EntrySide, eventTime int64) TriggerAnalysis {
	last := window[len(window)-1]
	prev := window[:len(window)-1]

	avgVolume := averageVolume(prev)
	volumeBurst := 0.0
	if avgVolume > 0 {
		volumeBurst = clamp((last.Volume/avgVolume-1.0)/1.2, 0, 1)
	}

	buyRatio := 0.5
	if last.Volume > 0 {
		buyRatio = clamp(last.TakerBuyVolume/last.Volume, 0, 1)
	}
	tradeImbalance := clamp(math.Abs(buyRatio-0.5)*2, 0, 1)
	if entrySide == EntrySideLong {
		tradeImbalance = clamp((buyRatio-0.5)*2, 0, 1)
	} else if entrySide == EntrySideShort {
		tradeImbalance = clamp((0.5-buyRatio)*2, 0, 1)
	}

	avgPrevRange := averageRange(prev)
	volatilityExpansion := 0.0
	lastRange := last.High - last.Low
	if avgPrevRange > 0 {
		volatilityExpansion = clamp((lastRange/avgPrevRange-1.0)/1.0, 0, 1)
	}

	t := time.UnixMilli(eventTime).UTC()
	minute := t.Minute()
	timeWindow := 0.2
	switch minute % 30 {
	case 0:
		timeWindow = 1.0
	case 14, 15, 16, 29:
		timeWindow = 0.75
	case 1, 28:
		timeWindow = 0.55
	}

	triggerScore := clamp(
		volumeBurst*0.35+
			tradeImbalance*0.30+
			volatilityExpansion*0.20+
			timeWindow*0.15,
		0, 1,
	)

	return TriggerAnalysis{
		VolumeBurstScore:         volumeBurst,
		TradeImbalanceScore:      tradeImbalance,
		VolatilityExpansionScore: volatilityExpansion,
		TimeWindowScore:          timeWindow,
		TriggerScore:             triggerScore,
	}
}

func (d *Detector) classify(probability float64) (string, string) {
	switch {
	case probability >= d.BlockThreshold:
		return ActionWaitForReclaim, RiskLevelPathAlert
	case probability >= 0.60:
		return ActionReduceProbeSize, RiskLevelPathPressure
	default:
		return ActionFollowPath, RiskLevelPathClear
	}
}

func averageRange(candles []Candle) float64 {
	if len(candles) == 0 {
		return 0
	}
	sum := 0.0
	for _, candle := range candles {
		sum += math.Max(candle.High-candle.Low, 0)
	}
	return sum / float64(len(candles))
}

func averageATR(candles []Candle) float64 {
	if len(candles) == 0 {
		return 0
	}
	sum := 0.0
	count := 0
	for _, candle := range candles {
		if candle.Atr <= 0 {
			continue
		}
		sum += candle.Atr
		count++
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

func averageVolume(candles []Candle) float64 {
	if len(candles) == 0 {
		return 0
	}
	sum := 0.0
	for _, candle := range candles {
		sum += candle.Volume
	}
	return sum / float64(len(candles))
}

func clamp(v, minV, maxV float64) float64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}
