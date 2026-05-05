package svc

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/universepool"
)

// TestBuildHydratedWarmupKlinesCalculatesIndicators 验证 shared warmup 原始 OHLCV 能重算出非零高周期指标。
func TestBuildHydratedWarmupKlinesCalculatesIndicators(t *testing.T) {
	rawKlines := make([]strategyWarmupParsedKline, 0, 80)
	baseTime := time.Date(2024, 4, 15, 8, 0, 0, 0, time.UTC)
	for i := 0; i < 80; i++ {
		openTime := baseTime.Add(time.Duration(i) * 4 * time.Hour)
		rawKlines = append(rawKlines, strategyWarmupParsedKline{
			OpenTime:  openTime,
			CloseTime: openTime.Add(4*time.Hour - time.Millisecond),
			Open:      3000 + float64(i),
			High:      3010 + float64(i),
			Low:       2990 + float64(i),
			Close:     3002 + float64(i),
			Volume:    100 + float64(i),
		})
	}

	klines := buildHydratedWarmupKlines("ETHUSDT", "4h", rawKlines, strategyIndicatorParams{
		Ema21Period: 21,
		Ema55Period: 55,
		RsiPeriod:   14,
		AtrPeriod:   14,
	})
	if len(klines) != 60 {
		t.Fatalf("len(klines) = %d, want 60", len(klines))
	}
	last := klines[len(klines)-1]
	if last.Ema55 == 0 {
		t.Fatalf("last.Ema55 = 0, want non-zero")
	}
	if last.Ema21 == 0 {
		t.Fatalf("last.Ema21 = 0, want non-zero")
	}
	if last.Atr == 0 {
		t.Fatalf("last.Atr = 0, want non-zero")
	}
}

// TestHydrateStrategyFromSharedWarmupDirHydratesEngine 验证 market 内嵌策略在启动阶段会吃到 shared warmup 的 4H 历史。
func TestHydrateStrategyFromSharedWarmupDirHydratesEngine(t *testing.T) {
	sharedWarmupDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sharedWarmupDir, "ETHUSDT", "4h"), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	baseTime := time.Date(2024, 4, 15, 8, 0, 0, 0, time.UTC)
	filePath := filepath.Join(sharedWarmupDir, "ETHUSDT", "4h", "2024-04-15.jsonl")
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	for i := 0; i < 80; i++ {
		openTime := baseTime.Add(time.Duration(i) * 4 * time.Hour)
		line := fmt.Sprintf(
			"{\"openTime\":\"%s\",\"open\":%.2f,\"high\":%.2f,\"low\":%.2f,\"close\":%.2f,\"volume\":%.2f,\"closeTime\":\"%s\"}\n",
			openTime.Format(time.RFC3339Nano),
			3000+float64(i),
			3010+float64(i),
			2990+float64(i),
			3002+float64(i),
			100+float64(i),
			openTime.Add(4*time.Hour-time.Millisecond).Format(time.RFC3339Nano),
		)
		if _, err := file.WriteString(line); err != nil {
			_ = file.Close()
			t.Fatalf("WriteString() error = %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	engine, err := NewStrategyEngine(&config.StrategyEngineConfig{
		SignalLogDir: t.TempDir(),
	}, nil)
	if err != nil {
		t.Fatalf("NewStrategyEngine() error = %v", err)
	}
	if err := engine.StartStrategy(config.StrategyConfig{
		Symbol:  "ETHUSDT",
		Enabled: true,
		Parameters: map[string]float64{
			"ema21_period": 21,
			"ema55_period": 55,
			"rsi_period":   14,
			"atr_period":   14,
		},
	}, nil); err != nil {
		t.Fatalf("StartStrategy() error = %v", err)
	}

	cfg := config.Config{
		SharedWarmupDir: sharedWarmupDir,
		Indicators: config.IndicatorConfig{
			Ema21Period: 21,
			Ema55Period: 55,
			RsiPeriod:   14,
			AtrPeriod:   14,
		},
	}
	if err := hydrateStrategyFromSharedWarmupDir(sharedWarmupDir, cfg, engine, "ETHUSDT"); err != nil {
		t.Fatalf("hydrateStrategyFromSharedWarmupDir() error = %v", err)
	}

	_, _, _, history4h := engine.KlineCount("ETHUSDT")
	if history4h != 60 {
		t.Fatalf("history4h = %d, want 60", history4h)
	}
}

// TestHydrateUniversePoolFromSharedWarmup 验证 shared warmup 的 4H 历史会在启动阶段回灌到动态币池的 range gate 缓存。
func TestHydrateUniversePoolFromSharedWarmup(t *testing.T) {
	sharedWarmupDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sharedWarmupDir, "ETHUSDT", "4h"), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	baseTime := time.Date(2024, 4, 15, 8, 0, 0, 0, time.UTC)
	filePath := filepath.Join(sharedWarmupDir, "ETHUSDT", "4h", "2024-04-15.jsonl")
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	for i := 0; i < 80; i++ {
		openTime := baseTime.Add(time.Duration(i) * 4 * time.Hour)
		line := fmt.Sprintf(
			"{\"openTime\":\"%s\",\"open\":%.2f,\"high\":%.2f,\"low\":%.2f,\"close\":%.2f,\"volume\":%.2f,\"closeTime\":\"%s\"}\n",
			openTime.Format(time.RFC3339Nano),
			3000+float64(i)*0.1,
			3004+float64(i%3),
			2996-float64(i%2),
			3000+float64(i)*0.1,
			100+float64(i),
			openTime.Add(4*time.Hour-time.Millisecond).Format(time.RFC3339Nano),
		)
		if _, err := file.WriteString(line); err != nil {
			_ = file.Close()
			t.Fatalf("WriteString() error = %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	manager := universepool.NewManager(universepool.Config{
		Enabled:        true,
		ValidationMode: "1m",
		CandidateSymbols: []string{
			"ETHUSDT",
		},
	}, nil, nil, nil, nil)
	cfg := config.Config{
		SharedWarmupDir: sharedWarmupDir,
		Indicators: config.IndicatorConfig{
			Ema21Period: 21,
			Ema55Period: 55,
			RsiPeriod:   14,
			AtrPeriod:   14,
		},
	}
	cfg.UniversePool.Enabled = true
	cfg.UniversePool.CandidateSymbols = []string{"ETHUSDT"}

	if err := hydrateUniversePoolFromSharedWarmup(cfg, manager); err != nil {
		t.Fatalf("hydrateUniversePoolFromSharedWarmup() error = %v", err)
	}
	snap, ok := manager.RangeGateSnapshot("ETHUSDT")
	if !ok {
		t.Fatal("RangeGateSnapshot() missing for ETHUSDT")
	}
	if !snap.RangeGate4H.Ready {
		t.Fatalf("range_gate = %+v, want ready after warmup hydrate", snap.RangeGate4H)
	}
	if snap.RangeGate4H.Reason == "" {
		t.Fatalf("range_gate = %+v, want non-empty reason after warmup hydrate", snap.RangeGate4H)
	}
}

// TestWaitForStrategyWarmupReady 等待 shared warmup 多周期文件落盘后再继续，避免启动时序导致回灌读到空目录。
func TestWaitForStrategyWarmupReady(t *testing.T) {
	sharedWarmupDir := t.TempDir()
	symbol := "ETHUSDT"

	go func() {
		time.Sleep(30 * time.Millisecond)
		for _, interval := range strategyWarmupIntervals() {
			dir := filepath.Join(sharedWarmupDir, symbol, interval)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return
			}
			_ = os.WriteFile(filepath.Join(dir, "ready.jsonl"), []byte("{\"ok\":true}\n"), 0o644)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	if err := waitForStrategyWarmupReady(sharedWarmupDir, symbol, 500*time.Millisecond, 10*time.Millisecond); err != nil {
		t.Fatalf("waitForStrategyWarmupReady() error = %v", err)
	}
}

// TestWaitForStrategyWarmupReadyTimeout 缺少任一周期文件时应按超时返回错误，避免误判 warmup 已完成。
func TestWaitForStrategyWarmupReadyTimeout(t *testing.T) {
	sharedWarmupDir := t.TempDir()
	symbol := "ETHUSDT"
	if err := os.MkdirAll(filepath.Join(sharedWarmupDir, symbol, "4h"), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(sharedWarmupDir, symbol, "4h", "ready.jsonl"), []byte("{\"ok\":true}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := waitForStrategyWarmupReady(sharedWarmupDir, symbol, 40*time.Millisecond, 10*time.Millisecond); err == nil {
		t.Fatalf("waitForStrategyWarmupReady() error = nil, want timeout")
	}
}
