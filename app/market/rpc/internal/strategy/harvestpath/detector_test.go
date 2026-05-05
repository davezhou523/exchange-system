package harvestpath

import "testing"

func TestDetectorEvaluateLongRisk(t *testing.T) {
	detector := NewDetector(12, 0.80)
	candles := []Candle{
		{High: 100.2, Low: 99.8, Close: 100.0, Volume: 10, TakerBuyVolume: 5, Atr: 0.5},
		{High: 100.4, Low: 99.9, Close: 100.1, Volume: 11, TakerBuyVolume: 6, Atr: 0.5},
		{High: 100.3, Low: 99.7, Close: 100.0, Volume: 10, TakerBuyVolume: 5, Atr: 0.5},
		{High: 100.5, Low: 100.0, Close: 100.2, Volume: 12, TakerBuyVolume: 7, Atr: 0.5},
		{High: 100.6, Low: 100.1, Close: 100.3, Volume: 12, TakerBuyVolume: 7, Atr: 0.5},
		{High: 100.7, Low: 100.2, Close: 100.4, Volume: 13, TakerBuyVolume: 8, Atr: 0.5},
		{High: 100.8, Low: 100.3, Close: 100.5, Volume: 14, TakerBuyVolume: 8, Atr: 0.5},
		{High: 100.9, Low: 100.4, Close: 100.6, Volume: 15, TakerBuyVolume: 9, Atr: 0.5},
		{High: 101.0, Low: 100.5, Close: 100.7, Volume: 15, TakerBuyVolume: 9, Atr: 0.5},
		{High: 101.0, Low: 100.6, Close: 100.8, Volume: 16, TakerBuyVolume: 10, Atr: 0.5},
		{High: 101.0, Low: 100.7, Close: 100.9, Volume: 18, TakerBuyVolume: 13, Atr: 0.5},
		{High: 101.1, Low: 100.8, Close: 101.0, Volume: 30, TakerBuyVolume: 24, Atr: 0.5},
	}

	signal, err := detector.Evaluate(Context{
		Symbol:    "ETHUSDT",
		EventTime: 1710000000000,
		LastPrice: 100.95,
		EntrySide: EntrySideLong,
		Candles1m: candles,
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if signal == nil {
		t.Fatal("Evaluate() returned nil signal")
	}
	if signal.TargetSide != TargetSideUp {
		t.Fatalf("TargetSide = %s, want %s", signal.TargetSide, TargetSideUp)
	}
	if signal.StopDensityScore <= 0 {
		t.Fatalf("StopDensityScore = %f, want > 0", signal.StopDensityScore)
	}
}

func TestDetectorShouldBlock(t *testing.T) {
	detector := NewDetector(20, 0.75)
	if !detector.ShouldBlock(&Signal{HarvestPathProbability: 0.80}) {
		t.Fatal("ShouldBlock() = false, want true")
	}
	if detector.ShouldBlock(&Signal{HarvestPathProbability: 0.60}) {
		t.Fatal("ShouldBlock() = true, want false")
	}
}

func TestDetectorBlendLSTMProbability(t *testing.T) {
	detector := NewDetector(20, 0.75)
	signal := &Signal{
		RuleProbability:        0.50,
		HarvestPathProbability: 0.50,
		PathAction:             ActionFollowPath,
		RiskLevel:              RiskLevelPathClear,
	}

	detector.BlendLSTMProbability(signal, 0.90, 0.50)

	if signal.LSTMProbability != 0.90 {
		t.Fatalf("LSTMProbability = %f, want 0.90", signal.LSTMProbability)
	}
	if signal.HarvestPathProbability <= 0.69 || signal.HarvestPathProbability >= 0.71 {
		t.Fatalf("HarvestPathProbability = %f, want about 0.70", signal.HarvestPathProbability)
	}
	if signal.PathAction != ActionReduceProbeSize {
		t.Fatalf("PathAction = %s, want %s", signal.PathAction, ActionReduceProbeSize)
	}
	if signal.RiskLevel != RiskLevelPathPressure {
		t.Fatalf("RiskLevel = %s, want %s", signal.RiskLevel, RiskLevelPathPressure)
	}
}

func TestDetectorBlendModelProbabilities(t *testing.T) {
	detector := NewDetector(20, 0.75)
	signal := &Signal{
		RuleProbability:        0.50,
		LSTMProbability:        0.80,
		BookProbability:        0.20,
		HarvestPathProbability: 0.50,
	}

	detector.BlendModelProbabilities(signal, 0.30, 0.20)

	want := 0.50*0.50 + 0.80*0.30 + 0.20*0.20
	if signal.HarvestPathProbability < want-0.0001 || signal.HarvestPathProbability > want+0.0001 {
		t.Fatalf("HarvestPathProbability = %f, want %f", signal.HarvestPathProbability, want)
	}
}
