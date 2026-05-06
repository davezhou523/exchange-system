package aggregator

import "testing"

func TestNewKlineAggregatorDisablesKafkaWhenProducerNil(t *testing.T) {
	t.Parallel()

	agg := NewKlineAggregator(StandardIntervals, nil, "", 0, IndicatorParams{})
	defer agg.Stop()

	if agg.kafkaSendEnabled.Load() {
		t.Fatal("kafkaSendEnabled = true, want false when producer is nil")
	}
}
