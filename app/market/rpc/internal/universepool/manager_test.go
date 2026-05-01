package universepool

import (
	"testing"
	"time"

	"exchange-system/common/pb/market"
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

func TestManagerUpdateSnapshotFromKlineUses5mInValidationMode5m(t *testing.T) {
	mgr := NewManager(Config{
		Enabled:        true,
		ValidationMode: "5m",
	}, nil, nil, nil, nil)

	mgr.UpdateSnapshotFromKline(&market.Kline{
		Symbol:    "XRPUSDT",
		Interval:  "1m",
		Close:     1.2,
		Ema21:     1.1,
		Ema55:     1.0,
		Atr:       0.01,
		IsClosed:  true,
		EventTime: time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC).UnixMilli(),
	})
	if len(mgr.snapshots) != 0 {
		t.Fatalf("snapshots len = %d, want 0", len(mgr.snapshots))
	}

	mgr.UpdateSnapshotFromKline(&market.Kline{
		Symbol:    "XRPUSDT",
		Interval:  "5m",
		Close:     1.2,
		Ema21:     1.1,
		Ema55:     1.0,
		Atr:       0.01,
		IsClosed:  true,
		EventTime: time.Date(2026, 4, 26, 12, 5, 0, 0, time.UTC).UnixMilli(),
	})
	got, ok := mgr.snapshots["XRPUSDT"]
	if !ok {
		t.Fatal("snapshot missing for XRPUSDT")
	}
	if got.LastReason != "fresh_5m" {
		t.Fatalf("LastReason = %s, want fresh_5m", got.LastReason)
	}
}

func TestManagerUpdateSnapshotFromKlineUses1mInValidationMode1m(t *testing.T) {
	mgr := NewManager(Config{
		Enabled:        true,
		ValidationMode: "1m",
	}, nil, nil, nil, nil)

	updatedAt := time.Date(2026, 5, 1, 14, 15, 0, 0, time.UTC)
	mgr.UpdateSnapshotFromKline(&market.Kline{
		Symbol:     "BNBUSDT",
		Interval:   "1m",
		Close:      610.5,
		Ema21:      608.2,
		Ema55:      603.8,
		Rsi:        58.4,
		Atr:        4.2,
		Volume:     12345,
		IsClosed:   true,
		IsDirty:    false,
		IsTradable: true,
		IsFinal:    true,
		EventTime:  updatedAt.UnixMilli(),
	})

	got, ok := mgr.snapshots["BNBUSDT"]
	if !ok {
		t.Fatal("snapshot missing for BNBUSDT")
	}
	if got.LastReason != "fresh_1m" {
		t.Fatalf("LastReason = %s, want fresh_1m", got.LastReason)
	}
	if !got.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("UpdatedAt = %s, want %s", got.UpdatedAt, updatedAt)
	}
	if got.LastPrice != 610.5 {
		t.Fatalf("LastPrice = %v, want 610.5", got.LastPrice)
	}
	if !got.Healthy {
		t.Fatal("Healthy = false, want true")
	}
}

func TestSummarizeStatesIncludesSnapshotMetadata(t *testing.T) {
	now := time.Date(2026, 4, 27, 0, 10, 0, 0, time.UTC)
	summary := summarizeStates(Config{ValidationMode: "5m"}, now, DesiredUniverse{}, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-2 * time.Minute), Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-30 * time.Second), Healthy: true},
	}, map[string]SymbolRuntimeState{})

	if summary.SnapshotInterval != "5m" {
		t.Fatalf("SnapshotInterval = %s, want 5m", summary.SnapshotInterval)
	}
	if !summary.LastSnapshotAt.Equal(now.Add(-30 * time.Second)) {
		t.Fatalf("LastSnapshotAt = %s, want %s", summary.LastSnapshotAt, now.Add(-30*time.Second))
	}
}
