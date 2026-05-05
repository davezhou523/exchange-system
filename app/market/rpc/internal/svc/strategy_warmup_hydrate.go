package svc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/common/indicator/atr"
	"exchange-system/common/indicator/ema"
	"exchange-system/common/indicator/rsi"
	marketpb "exchange-system/common/pb/market"
)

// strategyWarmupRawKline 对应 shared warmup 目录里的原始 OHLCV JSONL 结构。
type strategyWarmupRawKline struct {
	OpenTime  string  `json:"openTime"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	CloseTime string  `json:"closeTime"`
}

// strategyIndicatorParams 汇总单周期指标参数，便于统一计算 warmup 指标快照。
type strategyIndicatorParams struct {
	Ema21Period int
	Ema55Period int
	RsiPeriod   int
	AtrPeriod   int
}

// hydrateStrategyEngineFromSharedWarmup 从共享 warmup 目录恢复多周期历史，并回灌到 market 内嵌策略实例。
func hydrateStrategyEngineFromSharedWarmup(cfg config.Config, engine *StrategyEngine) error {
	if engine == nil || len(cfg.StrategyEngine.Strategies) == 0 {
		return nil
	}
	sharedWarmupDir := strings.TrimSpace(cfg.SharedWarmupDir)
	if sharedWarmupDir == "" {
		return nil
	}

	for _, strategyCfg := range cfg.StrategyEngine.Strategies {
		if !strategyCfg.Enabled {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(strategyCfg.Symbol))
		if symbol == "" {
			continue
		}
		waitTimeout := resolveStrategyWarmupWaitTimeout(cfg)
		if err := waitForStrategyWarmupReady(sharedWarmupDir, symbol, waitTimeout, 200*time.Millisecond); err != nil {
			return err
		}
		if err := hydrateStrategyFromSharedWarmupDir(sharedWarmupDir, cfg, engine, symbol); err != nil {
			return err
		}
	}
	return nil
}

// resolveStrategyWarmupWaitTimeout 统一解析 strategy 启动时等待 shared warmup 文件的超时时间。
func resolveStrategyWarmupWaitTimeout(cfg config.Config) time.Duration {
	timeout := cfg.ClickHouse.Recovery.WarmupWaitTimeout
	if timeout <= 0 {
		return 2 * time.Minute
	}
	return timeout
}

// waitForStrategyWarmupReady 轮询 shared warmup 目录，直到多周期文件全部就绪或超时。
func waitForStrategyWarmupReady(sharedWarmupDir, symbol string, timeout, pollInterval time.Duration) error {
	if strings.TrimSpace(sharedWarmupDir) == "" || strings.TrimSpace(symbol) == "" {
		return nil
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}

	deadline := time.Now().Add(timeout)
	for {
		readyIntervals, missingIntervals, err := strategyWarmupReadyIntervals(sharedWarmupDir, symbol)
		if err != nil {
			return err
		}
		if len(missingIntervals) == 0 {
			log.Printf("[策略引擎] warmup文件就绪 | symbol=%s intervals=%s", symbol, strings.Join(readyIntervals, ","))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wait shared warmup ready timeout for %s: ready=%s missing=%s timeout=%s",
				symbol,
				strings.Join(readyIntervals, ","),
				strings.Join(missingIntervals, ","),
				timeout,
			)
		}
		time.Sleep(pollInterval)
	}
}

// strategyWarmupReadyIntervals 检查指定交易对的多周期 warmup 文件是否已经落盘。
func strategyWarmupReadyIntervals(sharedWarmupDir, symbol string) ([]string, []string, error) {
	readyIntervals := make([]string, 0, len(strategyWarmupIntervals()))
	missingIntervals := make([]string, 0, len(strategyWarmupIntervals()))
	for _, interval := range strategyWarmupIntervals() {
		ready, err := strategyWarmupIntervalHasFiles(sharedWarmupDir, symbol, interval)
		if err != nil {
			return nil, nil, err
		}
		if ready {
			readyIntervals = append(readyIntervals, interval)
			continue
		}
		missingIntervals = append(missingIntervals, interval)
	}
	return readyIntervals, missingIntervals, nil
}

// strategyWarmupIntervalHasFiles 判断某个周期目录下是否已有至少一个 warmup JSONL 文件。
func strategyWarmupIntervalHasFiles(sharedWarmupDir, symbol, interval string) (bool, error) {
	dir := filepath.Join(sharedWarmupDir, strings.ToUpper(strings.TrimSpace(symbol)), interval)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".jsonl") {
			return true, nil
		}
	}
	return false, nil
}

// hydrateStrategyFromSharedWarmupDir 为单个交易对构造带指标的 warmup K 线，并回灌到策略实例。
func hydrateStrategyFromSharedWarmupDir(sharedWarmupDir string, cfg config.Config, engine *StrategyEngine, symbol string) error {
	if engine == nil || symbol == "" {
		return nil
	}

	totalHydrated := 0
	hydratedByInterval := make(map[string]int, len(strategyWarmupIntervals()))
	for _, interval := range strategyWarmupIntervals() {
		klines, err := loadHydratedWarmupKlines(sharedWarmupDir, symbol, interval, resolveStrategyIndicatorParams(cfg, interval))
		if err != nil {
			return fmt.Errorf("hydrate shared warmup %s %s: %w", symbol, interval, err)
		}
		hydratedCount := engine.HydrateStrategyWarmup(symbol, klines)
		hydratedByInterval[interval] = hydratedCount
		totalHydrated += hydratedCount
	}
	if diagnostics, ok := engine.StrategyWarmupDiagnostics(symbol); ok {
		log.Printf(
			"[策略引擎] warmup回灌结果 | symbol=%s hydrated_total=%d hydrated_4h=%d hydrated_1h=%d hydrated_15m=%d hydrated_1m=%d history_len_4h=%d history_len_1h=%d history_len_15m=%d history_len_1m=%d latest4h_ema21=%.6f latest4h_ema55=%.6f latest4h_close=%.6f latest4h_is_final=%t",
			symbol,
			totalHydrated,
			hydratedByInterval["4h"],
			hydratedByInterval["1h"],
			hydratedByInterval["15m"],
			hydratedByInterval["1m"],
			diagnostics.HistoryLen4h,
			diagnostics.HistoryLen1h,
			diagnostics.HistoryLen15m,
			diagnostics.HistoryLen1m,
			diagnostics.Latest4hEma21,
			diagnostics.Latest4hEma55,
			diagnostics.Latest4hClose,
			diagnostics.Latest4hFinal,
		)
	}
	return nil
}

// loadHydratedWarmupKlines 读取 shared warmup 原始 K 线，重算指标后生成可直接回灌策略的快照。
func loadHydratedWarmupKlines(sharedWarmupDir, symbol, interval string, params strategyIndicatorParams) ([]*marketpb.Kline, error) {
	rawKlines, err := readStrategyWarmupRawKlines(sharedWarmupDir, symbol, interval)
	if err != nil || len(rawKlines) == 0 {
		return nil, err
	}

	sort.Slice(rawKlines, func(i, j int) bool {
		return rawKlines[i].OpenTime.Before(rawKlines[j].OpenTime)
	})
	return buildHydratedWarmupKlines(symbol, interval, rawKlines, params), nil
}

// readStrategyWarmupRawKlines 读取某个交易对某个周期的 shared warmup JSONL 文件，并按时间汇总为原始序列。
func readStrategyWarmupRawKlines(sharedWarmupDir, symbol, interval string) ([]strategyWarmupParsedKline, error) {
	dir := filepath.Join(sharedWarmupDir, strings.ToUpper(strings.TrimSpace(symbol)), interval)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	fileNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		fileNames = append(fileNames, entry.Name())
	}
	sort.Strings(fileNames)

	result := make([]strategyWarmupParsedKline, 0, 256)
	for _, fileName := range fileNames {
		filePath := filepath.Join(dir, fileName)
		items, err := readStrategyWarmupRawKlinesFromFile(filePath)
		if err != nil {
			return nil, err
		}
		result = append(result, items...)
	}
	return dedupeStrategyWarmupParsedKlines(result), nil
}

// strategyWarmupParsedKline 保存已解析时间字段的 warmup K 线，便于后续排序和指标计算。
type strategyWarmupParsedKline struct {
	OpenTime  time.Time
	CloseTime time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

// readStrategyWarmupRawKlinesFromFile 按行解析单个 warmup 文件，忽略空行并保留合法 K 线。
func readStrategyWarmupRawKlinesFromFile(filePath string) ([]strategyWarmupParsedKline, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	items := make([]strategyWarmupParsedKline, 0, 128)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1024), 2*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var raw strategyWarmupRawKline
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, fmt.Errorf("decode warmup line %s: %w", filePath, err)
		}
		item, ok, err := parseStrategyWarmupRawKline(raw)
		if err != nil {
			return nil, fmt.Errorf("parse warmup line %s: %w", filePath, err)
		}
		if ok {
			items = append(items, item)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// parseStrategyWarmupRawKline 把原始 JSONL 记录解析成内部结构，过滤时间非法或价格异常的数据。
func parseStrategyWarmupRawKline(raw strategyWarmupRawKline) (strategyWarmupParsedKline, bool, error) {
	openTime, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw.OpenTime))
	if err != nil {
		return strategyWarmupParsedKline{}, false, err
	}
	closeTime, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw.CloseTime))
	if err != nil {
		return strategyWarmupParsedKline{}, false, err
	}
	if raw.Open <= 0 || raw.High <= 0 || raw.Low <= 0 || raw.Close <= 0 {
		return strategyWarmupParsedKline{}, false, nil
	}
	return strategyWarmupParsedKline{
		OpenTime:  openTime,
		CloseTime: closeTime,
		Open:      raw.Open,
		High:      raw.High,
		Low:       raw.Low,
		Close:     raw.Close,
		Volume:    raw.Volume,
	}, true, nil
}

// dedupeStrategyWarmupParsedKlines 按 openTime 去重，避免重复预热文件导致同一根 K 线被重复回灌。
func dedupeStrategyWarmupParsedKlines(items []strategyWarmupParsedKline) []strategyWarmupParsedKline {
	if len(items) == 0 {
		return nil
	}
	bestByOpenTime := make(map[int64]strategyWarmupParsedKline, len(items))
	for _, item := range items {
		key := item.OpenTime.UnixMilli()
		bestByOpenTime[key] = item
	}
	keys := make([]int64, 0, len(bestByOpenTime))
	for key := range bestByOpenTime {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	result := make([]strategyWarmupParsedKline, 0, len(keys))
	for _, key := range keys {
		result = append(result, bestByOpenTime[key])
	}
	return result
}

// buildHydratedWarmupKlines 对原始 OHLCV 序列顺序重算指标，并裁剪为策略实例实际需要的 warmup 窗口。
func buildHydratedWarmupKlines(symbol, interval string, rawKlines []strategyWarmupParsedKline, params strategyIndicatorParams) []*marketpb.Kline {
	if len(rawKlines) == 0 {
		return nil
	}

	closes := make([]float64, 0, len(rawKlines))
	highs := make([]float64, 0, len(rawKlines))
	lows := make([]float64, 0, len(rawKlines))
	prevCloses := make([]float64, 0, len(rawKlines))
	result := make([]*marketpb.Kline, 0, len(rawKlines))

	var ema21Value float64
	var ema55Value float64
	var rsiState rsi.State
	var rsiReady bool
	var atrValue float64
	var atrReady bool

	for i, item := range rawKlines {
		closes = append(closes, item.Close)
		highs = append(highs, item.High)
		lows = append(lows, item.Low)
		if i == 0 {
			prevCloses = append(prevCloses, 0)
		} else {
			prevCloses = append(prevCloses, rawKlines[i-1].Close)
		}

		if params.Ema21Period > 0 {
			switch {
			case len(closes) == params.Ema21Period:
				ema21Value = ema.Init(closes, params.Ema21Period)
			case len(closes) > params.Ema21Period:
				ema21Value = ema.Step(ema21Value, item.Close, params.Ema21Period)
			}
		}

		if params.Ema55Period > 0 {
			switch {
			case len(closes) == params.Ema55Period:
				ema55Value = ema.Init(closes, params.Ema55Period)
			case len(closes) > params.Ema55Period:
				ema55Value = ema.Step(ema55Value, item.Close, params.Ema55Period)
			}
		}

		rsiValue := 0.0
		if params.RsiPeriod > 0 && len(closes) > params.RsiPeriod {
			if !rsiReady {
				rsiState = rsi.InitWilder(closes, params.RsiPeriod)
				rsiReady = true
			} else {
				diff := closes[len(closes)-1] - closes[len(closes)-2]
				rsiState.AvgGain, rsiState.AvgLoss = rsi.WilderStep(rsiState.AvgGain, rsiState.AvgLoss, diff, params.RsiPeriod)
			}
			rsiValue = rsi.FromAvgGainLoss(rsiState.AvgGain, rsiState.AvgLoss)
		}

		atrComputed := 0.0
		if params.AtrPeriod > 0 {
			tr := atr.TrueRange(item.High, item.Low, prevCloses[len(prevCloses)-1])
			switch {
			case len(highs) == params.AtrPeriod:
				atrValue = atr.InitRma(highs, lows, prevCloses, params.AtrPeriod).ATR
				atrReady = true
			case len(highs) > params.AtrPeriod && atrReady:
				atrValue = atr.RmaStep(atrValue, tr, params.AtrPeriod)
			}
			if atrReady {
				atrComputed = atrValue
			}
		}

		result = append(result, &marketpb.Kline{
			Symbol:     symbol,
			Interval:   interval,
			OpenTime:   item.OpenTime.UnixMilli(),
			Open:       item.Open,
			High:       item.High,
			Low:        item.Low,
			Close:      item.Close,
			Volume:     item.Volume,
			CloseTime:  item.CloseTime.UnixMilli(),
			IsClosed:   true,
			EventTime:  item.CloseTime.UnixMilli(),
			IsTradable: true,
			IsFinal:    true,
			Ema21:      ema21Value,
			Ema55:      ema55Value,
			Rsi:        rsiValue,
			Atr:        atrComputed,
		})
	}

	limit := strategyWarmupLimit(interval)
	if limit <= 0 || len(result) <= limit {
		return result
	}
	return result[len(result)-limit:]
}

// resolveStrategyIndicatorParams 读取指定周期的指标参数，优先用周期级配置，缺失时回退到默认指标配置。
func resolveStrategyIndicatorParams(cfg config.Config, interval string) strategyIndicatorParams {
	params := strategyIndicatorParams{
		Ema21Period: cfg.Indicators.Ema21Period,
		Ema55Period: cfg.Indicators.Ema55Period,
		RsiPeriod:   cfg.Indicators.RsiPeriod,
		AtrPeriod:   cfg.Indicators.AtrPeriod,
	}
	if intervalCfg, ok := cfg.IntervalIndicators[interval]; ok {
		if intervalCfg.Ema21Period > 0 {
			params.Ema21Period = intervalCfg.Ema21Period
		}
		if intervalCfg.Ema55Period > 0 {
			params.Ema55Period = intervalCfg.Ema55Period
		}
		if intervalCfg.RsiPeriod > 0 {
			params.RsiPeriod = intervalCfg.RsiPeriod
		}
		if intervalCfg.AtrPeriod > 0 {
			params.AtrPeriod = intervalCfg.AtrPeriod
		}
	}
	return params
}

// strategyWarmupIntervals 返回策略启动恢复依赖的多周期顺序，优先回灌高周期以尽快恢复趋势判态。
func strategyWarmupIntervals() []string {
	return []string{"4h", "1h", "15m", "1m"}
}

// strategyWarmupLimit 返回策略实例各周期本地缓存窗口上限，保持与策略 runtime 一致。
func strategyWarmupLimit(interval string) int {
	switch interval {
	case "4h":
		return 60
	case "1h":
		return 70
	case "15m":
		return 80
	case "1m":
		return 120
	default:
		return 0
	}
}
