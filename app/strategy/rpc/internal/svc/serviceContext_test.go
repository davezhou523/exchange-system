package svc

import (
	"strings"
	"testing"

	"exchange-system/app/strategy/rpc/internal/config"
)

func TestResolveStrategyParameters(t *testing.T) {
	sc := config.StrategyConfig{
		Name:     "trend-following",
		Symbol:   "BTCUSDT",
		Template: "btc-core",
		Parameters: map[string]float64{
			"leverage": 6,
		},
		Overrides: map[string]float64{
			"risk_per_trade": 0.02,
		},
	}
	templates := map[string]map[string]float64{
		"btc-core": {
			"risk_per_trade": 0.025,
			"max_positions":  1,
			"leverage":       5,
		},
	}

	got, err := resolveStrategyParameters(templates, sc)
	if err != nil {
		t.Fatalf("resolveStrategyParameters() error = %v", err)
	}
	if got["max_positions"] != 1 {
		t.Fatalf("max_positions = %v, want 1", got["max_positions"])
	}
	if got["leverage"] != 6 {
		t.Fatalf("leverage = %v, want 6", got["leverage"])
	}
	if got["risk_per_trade"] != 0.02 {
		t.Fatalf("risk_per_trade = %v, want 0.02", got["risk_per_trade"])
	}
}

func TestResolveStrategyParametersUnknownTemplate(t *testing.T) {
	sc := config.StrategyConfig{
		Name:     "trend-following",
		Symbol:   "SOLUSDT",
		Template: "missing-template",
	}

	_, err := resolveStrategyParameters(nil, sc)
	if err == nil {
		t.Fatal("resolveStrategyParameters() error = nil, want unknown template error")
	}
	if !strings.Contains(err.Error(), "unknown template") {
		t.Fatalf("resolveStrategyParameters() error = %v, want unknown template", err)
	}
}
