package marketstate

import (
	"testing"
	"time"
)

// TestEvaluateRangeGatePassed 验证 4H ADX、EMA 收敛和 ATR 回落同时满足时会放行震荡门禁。
func TestEvaluateRangeGatePassed(t *testing.T) {
	now := time.Date(2026, 5, 5, 8, 0, 0, 0, time.UTC)
	gate := EvaluateRangeGate(now, Features{
		Symbol:     "ETHUSDT",
		Timeframe:  "4h",
		Ema21:      100.2,
		Ema55:      100,
		Atr:        1.2,
		Adx:        18,
		UpdatedAt:  now.Add(-time.Minute),
		Healthy:    true,
		IsTradable: true,
		IsFinal:    true,
	}, Features{
		Symbol:     "ETHUSDT",
		Timeframe:  "4h",
		Atr:        1.5,
		UpdatedAt:  now.Add(-5 * time.Hour),
		Healthy:    true,
		IsTradable: true,
		IsFinal:    true,
	}, Config{})

	if !gate.Ready || !gate.Passed || gate.Score != 3 || gate.Reason != "range_gate_h4_passed" {
		t.Fatalf("gate = %+v, want passed score=3", gate)
	}
}

// TestEvaluateRangeGateFailed 验证 4H 条件不足时会拒绝震荡门禁并给出失败原因。
func TestEvaluateRangeGateFailed(t *testing.T) {
	now := time.Date(2026, 5, 5, 8, 0, 0, 0, time.UTC)
	gate := EvaluateRangeGate(now, Features{
		Symbol:     "ETHUSDT",
		Timeframe:  "4h",
		Ema21:      103,
		Ema55:      100,
		Atr:        1.8,
		Adx:        28,
		UpdatedAt:  now.Add(-time.Minute),
		Healthy:    true,
		IsTradable: true,
		IsFinal:    true,
	}, Features{
		Symbol:     "ETHUSDT",
		Timeframe:  "4h",
		Atr:        1.2,
		UpdatedAt:  now.Add(-5 * time.Hour),
		Healthy:    true,
		IsTradable: true,
		IsFinal:    true,
	}, Config{})

	if !gate.Ready || gate.Passed || gate.Score != 0 || gate.Reason != "range_gate_h4_failed" {
		t.Fatalf("gate = %+v, want failed score=0", gate)
	}
}
