package svc

import (
	"testing"

	"exchange-system/app/market/rpc/internal/config"
)

func TestStartStrategyUsesInlineParameters(t *testing.T) {
	engine, err := NewStrategyEngine(&config.StrategyEngineConfig{
		SignalLogDir: t.TempDir(),
	}, nil)
	if err != nil {
		t.Fatalf("NewStrategyEngine() error = %v", err)
	}

	err = engine.StartStrategy(config.StrategyConfig{
		Symbol:  "ETHUSDT",
		Enabled: true,
		Parameters: map[string]float64{
			"ema21_period": 21,
			"ema55_period": 55,
			"rsi_period":   14,
			"atr_period":   14,
		},
	}, nil)
	if err != nil {
		t.Fatalf("StartStrategy() error = %v", err)
	}

	if _, err := engine.GetStrategyStatus("ETHUSDT"); err != nil {
		t.Fatalf("GetStrategyStatus() error = %v", err)
	}
}

func TestResolveStrategyParamsFallsBackToEngineTemplates(t *testing.T) {
	params, err := resolveStrategyParams(
		config.StrategyConfig{Symbol: "ETHUSDT"},
		"ethusdt",
		nil,
		&config.StrategyEngineConfig{
			Templates: map[string]map[string]float64{
				"ethusdt": {
					"ema21_period": 21,
					"ema55_period": 55,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("resolveStrategyParams() error = %v", err)
	}
	if params["ema21_period"] != 21 || params["ema55_period"] != 55 {
		t.Fatalf("resolveStrategyParams() = %#v, want template params", params)
	}
}
