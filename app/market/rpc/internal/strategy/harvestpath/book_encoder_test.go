package harvestpath

import "testing"

func TestBookEncoderEncodeLongAdversePressure(t *testing.T) {
	encoder := NewBookEncoder(3, 12)
	features := encoder.Encode(&OrderBookSnapshot{
		Bids: []BookLevel{
			{Price: 100.0, Quantity: 1},
			{Price: 99.9, Quantity: 1},
			{Price: 99.8, Quantity: 1},
		},
		Asks: []BookLevel{
			{Price: 100.1, Quantity: 5},
			{Price: 100.2, Quantity: 4},
			{Price: 100.3, Quantity: 4},
		},
	}, 100.0, EntrySideLong)
	if features == nil {
		t.Fatal("Encode() returned nil")
	}
	if features.Probability <= 0.40 {
		t.Fatalf("Probability = %f, want > 0.40", features.Probability)
	}
	if features.DepthImbalance >= 0 {
		t.Fatalf("DepthImbalance = %f, want < 0", features.DepthImbalance)
	}
}
