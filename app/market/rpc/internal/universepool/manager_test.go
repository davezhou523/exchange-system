package universepool

import (
	"testing"
	"time"
)

type stubSubscriptionController struct {
	symbols []string
}

func (s *stubSubscriptionController) UpdateSymbols(symbols []string) error {
	s.symbols = append([]string(nil), symbols...)
	return nil
}

func (s *stubSubscriptionController) CurrentSymbols() []string {
	return append([]string(nil), s.symbols...)
}

func TestManagerTickSyncsCandidateObservationSubscriptions(t *testing.T) {
	subCtrl := &stubSubscriptionController{}
	mgr := NewManager(Config{
		Enabled:          true,
		CandidateSymbols: []string{"SOLUSDT", "XRPUSDT", "BNBUSDT"},
		AllowList:        []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval: 30 * time.Second,
	}, nil, nil, subCtrl, nil)

	mgr.Tick(time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC))

	want := []string{"BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"}
	got := subCtrl.CurrentSymbols()
	if len(got) != len(want) {
		t.Fatalf("symbols len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("symbols[%d] = %s, want %s (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestManagerWarmupValidationMode1mIgnoresHigherTimeframes(t *testing.T) {
	mgr := NewManager(Config{
		Enabled: true,
		Warmup: WarmupConfig{
			Enabled:                true,
			Min1mBars:              1,
			Require15mReady:        false,
			Require1hReady:         false,
			Require4hReady:         false,
			RequireIndicatorsReady: true,
		},
	}, nil, nil, nil, nil)

	got := mgr.isWarmupReadyStatus(WarmupStatus{
		Symbol:          "XRPUSDT",
		HasEnough1mBars: true,
		IndicatorsReady: true,
		Has15mReady:     false,
		Has1hReady:      false,
		Has4hReady:      false,
	})
	if !got {
		t.Fatal("isWarmupReadyStatus() = false, want true")
	}
}

func TestNewManagerValidationMode5mAppliesWarmupDefaults(t *testing.T) {
	mgr := NewManager(Config{
		Enabled:        true,
		ValidationMode: "5m",
	}, nil, nil, nil, nil)

	if mgr.cfg.EvaluateInterval != 30*time.Second {
		t.Fatalf("EvaluateInterval = %s, want 30s", mgr.cfg.EvaluateInterval)
	}
	if mgr.cfg.Warmup.Min1mBars < 5 {
		t.Fatalf("Min1mBars = %d, want >= 5", mgr.cfg.Warmup.Min1mBars)
	}
	if mgr.cfg.Warmup.Require15mReady || mgr.cfg.Warmup.Require1hReady || mgr.cfg.Warmup.Require4hReady {
		t.Fatalf("higher timeframe requirements = %+v, want all false", mgr.cfg.Warmup)
	}
}
