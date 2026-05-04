package aggregator

import (
	"context"
	"encoding/json"
	"exchange-system/common/indicator"
	"exchange-system/common/indicator/atr"
	"exchange-system/common/indicator/ema"
	"exchange-system/common/indicator/rsi"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// HistoryWarmupper 历史数据预热接口。
// 启动时从交易所 REST API 拉取历史K线，填充 ring buffer，
// 使 EMA/RSI/ATR 从第一根实时K线起就有正确值（而非冷启动的假值）。
//
// 为什么需要预热？
// EMA21 需要 ≥21 根K线才能初始化，EMA55 需要 ≥55 根。
// 如果不预热，从空 buffer 开始：
//   - 前 55 根15m K线（≈13.75小时）的 EMA55 值都是错的
//   - RSI/ATR 同理
//   - 导致策略在启动后数小时内产生错误信号
//
// 预热后，ring buffer 中已有足够历史数据，第一根实时K线的指标就是正确的。
type HistoryWarmupper interface {
	// FetchKlines 从交易所拉取最近的历史K线数据。
	// symbol: 交易对（如 "ETHUSDT"）
	// interval: K线周期（如 "15m", "1h", "4h"）
	// limit: 拉取数量（Binance 最多 1500）
	// 返回按时间升序排列的 K 线数据，每根包含 [openTime, open, high, low, close, volume, closeTime, ...]
	FetchKlines(ctx context.Context, symbol, interval string, limit int) ([][]interface{}, error)

	// FetchKlinesRange 从交易所拉取指定时间范围的历史K线数据。
	// symbol: 交易对（如 "ETHUSDT"）
	// interval: K线周期（如 "15m", "1h", "4h"）
	// startTime: 起始时间戳（毫秒），包含
	// endTime: 结束时间戳（毫秒），包含
	// limit: 单次拉取上限（Binance 最多 1500）
	// 返回按时间升序排列的 K 线数据，每根包含 [openTime, open, high, low, close, volume, closeTime, ...]
	FetchKlinesRange(ctx context.Context, symbol, interval string, startTime, endTime int64, limit int) ([][]interface{}, error)
}

// BinanceWarmupper 通过 Binance REST API 拉取历史K线数据。
type BinanceWarmupper struct {
	baseURL  string // Binance API 地址（如 "https://fapi.binance.com"）
	proxyURL string // 代理地址（可选）
	client   *http.Client
}

// NewBinanceWarmupper 创建 Binance 历史数据预热器。
// baseURL: Binance Futures API 地址
// proxyURL: 代理地址（空字符串表示不使用代理）
func NewBinanceWarmupper(baseURL, proxyURL string) *BinanceWarmupper {
	transport := &http.Transport{}
	if proxyURL != "" {
		if proxy, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxy)
		}
	}

	return &BinanceWarmupper{
		baseURL:  baseURL,
		proxyURL: proxyURL,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}
}

// FetchKlines 从 Binance REST API 拉取历史K线数据（最近N根）。
// API 文档: GET /fapi/v1/klines
// 返回格式: [[openTime, open, high, low, close, volume, closeTime, quoteVolume, trades, ...], ...]
func (w *BinanceWarmupper) FetchKlines(ctx context.Context, symbol, interval string, limit int) ([][]interface{}, error) {
	apiURL := fmt.Sprintf("%s/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		w.baseURL, symbol, interval, limit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("warmup: create request failed: %w", err)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("warmup: fetch klines failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("warmup: Binance API returned %d: %s", resp.StatusCode, string(body))
	}

	var klines [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&klines); err != nil {
		return nil, fmt.Errorf("warmup: decode response failed: %w", err)
	}

	return klines, nil
}

// FetchKlinesRange 从 Binance REST API 拉取指定时间范围的历史K线数据。
// API 文档: GET /fapi/v1/klines?symbol=X&interval=Y&startTime=A&endTime=B&limit=N
// 返回格式: [[openTime, open, high, low, close, volume, closeTime, quoteVolume, trades, ...], ...]
// Binance API 的 endTime 是包含的（K线的 openTime <= endTime）。
func (w *BinanceWarmupper) FetchKlinesRange(ctx context.Context, symbol, interval string, startTime, endTime int64, limit int) ([][]interface{}, error) {
	apiURL := fmt.Sprintf("%s/fapi/v1/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=%d",
		w.baseURL, symbol, interval, startTime, endTime, limit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("warmup: create request failed: %w", err)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("warmup: fetch klines range failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("warmup: Binance API returned %d: %s", resp.StatusCode, string(body))
	}

	var klines [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&klines); err != nil {
		return nil, fmt.Errorf("warmup: decode response failed: %w", err)
	}

	return klines, nil
}

// WarmupHistory 预热指定 symbol 的历史 ring buffer。
// 从交易所拉取历史K线，按时间顺序推入 ring buffer 并初始化递推指标状态。
// 必须在 OnKline 之前调用（通常是启动时）。
//
// 预热数量计算：
//   - 需要 maxPeriod * 3 根K线（EMA 初始化 + 充足历史）
//   - 例如 EMA55 需要 buffer ≥ 165 根K线
//   - Binance 限制单次最多 1500 根，分页拉取可突破此限制
//
// ⚠️ 预热数据不触发 emit（不推送 Kafka），只填充 ring buffer + 初始化指标状态
func (a *KlineAggregator) WarmupHistory(ctx context.Context, symbol string) error {
	if a.warmupper == nil {
		log.Printf("[warmup] no warmupper configured, skipping warmup for %s", symbol)
		return nil
	}

	// 启动时清理旧的预热数据和聚合K线数据，确保指标从干净状态重新计算
	// 1. warmup 目录：旧预热原始数据（可能因预热页数/参数变化而失效）
	// 2. 聚合K线目录（15m/1h/4h等）：旧指标值（基于旧预热数据计算）
	// 3. 保留 1m 原始K线数据（WebSocket 直传，不经过指标计算）
	a.cleanupHistoricalData(symbol)

	// 为每个周期预热
	for _, iv := range a.intervals {
		if err := a.warmupInterval(ctx, symbol, iv); err != nil {
			log.Printf("[warmup] WARN: %s %s warmup failed: %v (will start with cold indicators)", symbol, iv.Name, err)
			// 预热失败不阻塞启动，只是指标从冷启动开始
			continue
		}
	}

	return nil
}

// cleanupHistoricalData 启动时清理旧的历史数据文件，确保指标从干净状态重新计算。
// 删除内容：
//   - warmup 目录：旧预热原始数据（sharedWarmupDir/SYMBOL/ 或 klineLogDir/warmup/SYMBOL/）
//   - 所有周期K线目录（含1m）：避免1m数据无限积累占用磁盘
//
// 保留内容：
//   - 根级别 jsonl 文件（klineLogDir/SYMBOL/YYYY-MM-DD.jsonl），1m 实时数据
func (a *KlineAggregator) cleanupHistoricalData(symbol string) {
	if a.klineLogDir == "" {
		return
	}

	// 清理 warmup 目录
	warmupBaseDir := a.warmupRootDir()
	if warmupBaseDir != "" {
		warmupDir := filepath.Join(warmupBaseDir, symbol)
		if err := os.RemoveAll(warmupDir); err != nil {
			log.Printf("[warmup] WARN: shared_dir=%s failed to remove warmup dir %s: %v", formatWarmupSharedDir(warmupBaseDir), warmupDir, err)
		} else {
			log.Printf("[warmup] shared_dir=%s cleaned up warmup dir: %s", formatWarmupSharedDir(warmupBaseDir), warmupDir)
		}
	}

	// 清理所有周期K线目录（含1m），避免1m数据无限积累占用磁盘
	// 每次启动都会重新预热聚合K线，1m数据只是WebSocket直传记录无回放价值
	for _, iv := range a.intervals {
		intervalDir := filepath.Join(a.klineLogDir, symbol, iv.Name)
		if err := os.RemoveAll(intervalDir); err != nil {
			log.Printf("[warmup] WARN: failed to remove interval dir %s: %v", intervalDir, err)
		} else {
			log.Printf("[warmup] cleaned up interval dir: %s", intervalDir)
		}
	}
}

// warmupInterval 预热单个周期的 ring buffer
// 通过分页拉取更多历史数据（默认5页 × 1500 = 7500根），使递推指标充分收敛，
// 接近 Binance 官网用全量历史数据计算的指标值。
//
// 分页策略：
//  1. 先拉取最近 limit 根K线（第1页），确定最新时间范围
//  2. 以第1页最早K线的 openTime 之前1ms 为 endTime，继续往前拉取（第2页）
//  3. 重复直到拉满 pages 页或数据不足一页时停止
//  4. 合并所有页数据，按时间升序排列
func (a *KlineAggregator) warmupInterval(ctx context.Context, symbol string, iv IntervalDef) error {
	// 每页拉取上限（Binance 单次最多 1500 根）
	const pageLimit = 1500
	// 预热页数：更多页 = 更多历史数据 = 指标更接近 Binance 真实值
	// 5 页 × 1500 = 7500 根K线：
	//   - 15m: 7500 × 15min ≈ 78.1 天
	//   - 1h:  7500 × 1h    ≈ 312.5 天
	pages := a.warmupPages
	if pages <= 0 {
		pages = 5
	}

	log.Printf("[warmup] fetching up to %d pages × %d %s klines for %s (max ~%d klines) ...",
		pages, pageLimit, iv.Name, symbol, pages*pageLimit)

	intervalMs := iv.Duration.Milliseconds()
	if intervalMs <= 0 {
		return fmt.Errorf("invalid interval duration for %s", iv.Name)
	}

	// 第1页：拉取最近 pageLimit 根K线
	firstPage, err := a.warmupper.FetchKlines(ctx, symbol, iv.Name, pageLimit)
	if err != nil {
		return fmt.Errorf("fetch %s klines page 1: %w", iv.Name, err)
	}
	if len(firstPage) == 0 {
		return fmt.Errorf("no %s klines returned", iv.Name)
	}

	// 收集所有页数据（按时间升序）
	var allRawKlines [][]interface{}
	allRawKlines = append(allRawKlines, firstPage...)

	// 第2页及之后：用显式 startTime/endTime 向前翻页，避免 startTime=0 时跳回到交易对最早历史。
	for p := 2; p <= pages; p++ {
		// 当前最早K线的 openTime
		earliestOpenTime := int64(allRawKlines[0][0].(float64))
		// endTime 取“当前最早K线的前一根”的 openTime，避免和上一页重复。
		endTime := earliestOpenTime - intervalMs
		if endTime < 0 {
			log.Printf("[warmup] %s %s page %d: reached history floor, stopping pagination", symbol, iv.Name, p)
			break
		}
		// startTime 反推完整一页窗口，保证每一页都紧邻上一页。
		startTime := endTime - int64(pageLimit-1)*intervalMs
		if startTime < 0 {
			startTime = 0
		}

		pageKlines, err := a.warmupper.FetchKlinesRange(ctx, symbol, iv.Name, startTime, endTime, pageLimit)
		if err != nil {
			log.Printf("[warmup] WARN: %s %s page %d fetch failed: %v (using %d klines so far)",
				symbol, iv.Name, p, err, len(allRawKlines))
			break
		}

		if len(pageKlines) == 0 {
			log.Printf("[warmup] %s %s page %d: no more data, stopping pagination", symbol, iv.Name, p)
			break
		}

		// 将新页数据插入到前面（按时间升序合并）
		allRawKlines = append(pageKlines, allRawKlines...)

		log.Printf("[warmup] %s %s page %d: got %d klines (total: %d), range: %s ~ %s",
			symbol, iv.Name, p, len(pageKlines), len(allRawKlines),
			time.UnixMilli(int64(pageKlines[0][0].(float64))).UTC().Format("2006-01-02T15:04:05"),
			time.UnixMilli(int64(pageKlines[len(pageKlines)-1][0].(float64))).UTC().Format("2006-01-02T15:04:05"))

		// 如果本页数据不足 pageLimit，说明已无更多历史数据
		if len(pageKlines) < pageLimit {
			log.Printf("[warmup] %s %s page %d: partial page (%d < %d), no more history",
				symbol, iv.Name, p, len(pageKlines), pageLimit)
			break
		}
	}

	allRawKlines, continuity := normalizeWarmupPages(allRawKlines, intervalMs)
	log.Printf("[warmup] %s %s: total raw klines = %d", symbol, iv.Name, len(allRawKlines))
	log.Printf(
		"[warmup] %s %s continuity range=%s ~ %s step_ms=%d total=%d duplicates=%d gaps=%d continuous=%v",
		symbol,
		iv.Name,
		formatWarmupOpenTime(continuity.FirstOpenTime),
		formatWarmupOpenTime(continuity.LastOpenTime),
		intervalMs,
		continuity.Total,
		continuity.DuplicateCount,
		continuity.GapCount,
		continuity.GapCount == 0,
	)

	// 解析全部K线数据到临时数组（用于指标初始化）
	// 指标初始化需要尽可能多的历史数据来确保递推收敛
	allKlines := make([]warmupKline, 0, len(allRawKlines))
	for _, k := range allRawKlines {
		allKlines = append(allKlines, warmupKline{
			openTime: int64(k[0].(float64)),
			open:     parseFloat(k[1]),
			high:     parseFloat(k[2]),
			low:      parseFloat(k[3]),
			close:    parseFloat(k[4]),
		})
	}

	// 保存预热原始数据到文件，方便核对与调试
	// 格式：sharedWarmupDir/SYMBOL/INTERVAL/YYYY-MM-DD.jsonl
	// 每次启动会覆盖当天同路径文件（因为预热数据可能不同）
	a.saveWarmupData(symbol, iv.Name, allRawKlines)

	// 获取或创建 worker
	a.mu.Lock()
	w, exists := a.workers[symbol]
	a.mu.Unlock()

	if !exists {
		w = a.getOrCreateWorker(symbol)
	}

	history := w.intervalHistories[iv.Name]
	if history == nil {
		return fmt.Errorf("no history buffer for %s", iv.Name)
	}

	// 只将最后 bufferSize 根K线推入 ring buffer
	// ring buffer 容量有限，放不下全部数据，但指标状态已用全部数据初始化
	bufferSize := a.calcHistoryBufferSize(iv.Name)
	pushStart := 0
	if len(allKlines) > bufferSize {
		pushStart = len(allKlines) - bufferSize
	}
	for i := pushStart; i < len(allKlines); i++ {
		k := allKlines[i]
		history.push(k.open, k.high, k.low, k.close, k.openTime)
	}

	// 用全部K线数据初始化递推指标状态（而非只用 ring buffer 中的数据）
	// 更多历史数据 → 递推轮次更多 → 指标值更接近 Binance 真实值
	lastKline := allKlines[len(allKlines)-1]
	b := &bucket{
		Open:  lastKline.open,
		High:  lastKline.high,
		Low:   lastKline.low,
		Close: lastKline.close,
	}

	a.initIndicatorStateFromRawKlines(allKlines, history, b, iv.Name)

	log.Printf("[warmup] %s %s: loaded %d klines (pushed %d to buffer) | EMA21=%.2f EMA55=%.2f RSI=%.2f ATR=%.2f",
		symbol, iv.Name, len(allKlines), len(allKlines)-pushStart, b.Ema21, b.Ema55, b.Rsi, b.Atr)

	return nil
}

// initIndicatorStateFromRawKlines 用原始K线数据（全量，不受 ring buffer 容量限制）初始化递推指标状态。
// 预热场景：拉取了 1500 根K线，但 ring buffer 只能存 200 根。
// 指标初始化用全部 1500 根递推，确保收敛到 Binance 真实值。
// ring buffer 只存最后 200 根用于实时计算时的 O(1) 递推。
type warmupKline struct {
	openTime               int64
	open, high, low, close float64
}

func (a *KlineAggregator) initIndicatorStateFromRawKlines(klines []warmupKline, history *klineRingBuffer, b *bucket, intervalName string) {
	// 获取该周期的指标参数
	params, ok := a.intervalIndicators[intervalName]
	if !ok {
		params = IntervalIndicatorConfig{
			Ema21Period: a.indicatorParams.Ema21Period,
			Ema55Period: a.indicatorParams.Ema55Period,
			RsiPeriod:   a.indicatorParams.RsiPeriod,
			AtrPeriod:   a.indicatorParams.AtrPeriod,
		}
	}

	state := &history.state
	n := len(klines)

	// 提取收盘价/最高价/最低价序列
	closes := make([]float64, n)
	highs := make([]float64, n)
	lows := make([]float64, n)
	for i, k := range klines {
		closes[i] = k.close
		highs[i] = k.high
		lows[i] = k.low
	}

	// --- EMA 初始化 ---
	// 标准 EMA：SMA(period) 初始化，然后逐根递推
	// 数据越多，递推越久，EMA 值越接近 Binance 的真实值
	if params.Ema21Period > 0 && n >= params.Ema21Period {
		emaVal := roundIndicatorValue(ema.Init(closes, params.Ema21Period))
		state.ema21 = emaVal
		state.ema21Init = true
		b.Ema21 = emaVal
	}

	if params.Ema55Period > 0 && n >= params.Ema55Period {
		emaVal := roundIndicatorValue(ema.Init(closes, params.Ema55Period))
		state.ema55 = emaVal
		state.ema55Init = true
		b.Ema55 = emaVal
	}

	// --- RSI 初始化（Wilder/RMA 平滑）---
	// 标准 Wilder RSI：用前 period 个变化做简单平均，然后逐根递推
	if params.RsiPeriod > 0 && n > params.RsiPeriod {
		rsiState := rsi.InitWilder(closes, params.RsiPeriod)
		state.avgGain = rsiState.AvgGain
		state.avgLoss = rsiState.AvgLoss
		state.rsiPeriod = params.RsiPeriod
		state.rsiInit = true
		b.Rsi = roundIndicatorValue(rsi.FromAvgGainLoss(rsiState.AvgGain, rsiState.AvgLoss))
	}

	// --- ATR 初始化（RMA/Wilder 平滑，与 Binance/TradingView 对齐）---
	// 标准 Wilder ATR：用前 period 个 TR 的简单平均，然后逐根 RMA 递推
	// TR = max(H-L, |H-prevC|, |L-prevC|)，第1根K线无 prevC 时 TR = H-L
	if params.AtrPeriod > 0 && n >= params.AtrPeriod {
		prevCloses := indicator.PrevClosesFromCloses(closes)
		atrState := atr.InitRma(highs, lows, prevCloses, params.AtrPeriod)
		state.atr = roundIndicatorValue(atrState.ATR)
		state.atrPeriod = params.AtrPeriod
		state.atrInit = true
		b.Atr = state.atr
	}
}

// parseFloat 辅助函数：从 Binance JSON 响应中解析浮点数
func parseFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

// warmupContinuityStats 汇总 warmup 原始K线在时间轴上的连续性，便于启动日志快速定位跳页、重复和缺口。
type warmupContinuityStats struct {
	FirstOpenTime  int64
	LastOpenTime   int64
	Total          int
	DuplicateCount int
	GapCount       int
}

// normalizeWarmupPages 对分页结果按 openTime 排序并去重，同时统计重复和缺口数量。
func normalizeWarmupPages(klines [][]interface{}, intervalMs int64) ([][]interface{}, warmupContinuityStats) {
	stats := warmupContinuityStats{}
	if len(klines) == 0 || intervalMs <= 0 {
		return klines, stats
	}

	sort.Slice(klines, func(i, j int) bool {
		return warmupOpenTime(klines[i]) < warmupOpenTime(klines[j])
	})

	normalized := make([][]interface{}, 0, len(klines))
	var prevOpenTime int64
	for _, k := range klines {
		openTime := warmupOpenTime(k)
		if openTime <= 0 {
			continue
		}
		if len(normalized) == 0 {
			normalized = append(normalized, k)
			prevOpenTime = openTime
			continue
		}
		if openTime == prevOpenTime {
			stats.DuplicateCount++
			continue
		}
		if openTime > prevOpenTime+intervalMs {
			stats.GapCount += int((openTime-prevOpenTime)/intervalMs) - 1
		}
		normalized = append(normalized, k)
		prevOpenTime = openTime
	}

	stats.Total = len(normalized)
	if len(normalized) > 0 {
		stats.FirstOpenTime = warmupOpenTime(normalized[0])
		stats.LastOpenTime = warmupOpenTime(normalized[len(normalized)-1])
	}
	return normalized, stats
}

// warmupOpenTime 提取 Binance 原始K线数组中的 openTime，避免多处手写断言转换。
func warmupOpenTime(k []interface{}) int64 {
	if len(k) == 0 {
		return 0
	}
	switch v := k[0].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

// formatWarmupOpenTime 把 warmup continuity 日志中的毫秒时间戳转成可读 UTC 时间。
func formatWarmupOpenTime(ts int64) string {
	if ts <= 0 {
		return "n/a"
	}
	return time.UnixMilli(ts).UTC().Format("2006-01-02T15:04:05")
}

// SetWarmupper 设置历史数据预热器。
// 必须在 OnKline 之前调用。
// 设置后，每个新 symbol 的 worker 创建时会自动预热历史数据。
func (a *KlineAggregator) SetWarmupper(w HistoryWarmupper) {
	a.warmupper = w
}

// SetWarmupPages 设置预热分页数量。
// 每页最多拉取 1500 根K线（Binance API 限制）。
// pages=5 表示最多拉取 5×1500=7500 根历史K线用于指标初始化。
// 更多历史数据 → 递推轮次更多 → 指标值更接近 Binance 官网真实值。
func (a *KlineAggregator) SetWarmupPages(pages int) {
	a.warmupPages = pages
}

// warmupLogEntry 预热数据日志条目，与 Binance API 返回字段对齐
type warmupLogEntry struct {
	OpenTime  string  `json:"openTime"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	CloseTime string  `json:"closeTime"`
}

// saveWarmupData 保存预热拉取的原始K线数据到 jsonl 文件。
// 路径格式：sharedWarmupDir/SYMBOL/INTERVAL/YYYY-MM-DD.jsonl
// 每次启动覆盖当天文件（因为预热数据可能因启动时间不同而变化）。
// 保存失败仅打日志，不影响预热流程。
func (a *KlineAggregator) saveWarmupData(symbol, interval string, klines [][]interface{}) {
	if len(klines) == 0 {
		return
	}

	warmupBaseDir := a.warmupRootDir()
	if warmupBaseDir == "" {
		return
	}
	dir := filepath.Join(warmupBaseDir, symbol, interval)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf(
			"[warmup] symbol=%s interval=%s shared_dir=%s failed_to=create_dir path=%s err=%v",
			symbol,
			interval,
			formatWarmupSharedDir(warmupBaseDir),
			dir,
			err,
		)
		return
	}

	// 用第一根K线的 openTime 所在日期作为文件名
	firstOpenTime := int64(klines[0][0].(float64))
	dateStr := time.UnixMilli(firstOpenTime).UTC().Format("2006-01-02")
	path := filepath.Join(dir, dateStr+".jsonl")

	// 覆盖写入（每次启动预热数据可能不同）
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf(
			"[warmup] symbol=%s interval=%s shared_dir=%s failed_to=open_file path=%s err=%v",
			symbol,
			interval,
			formatWarmupSharedDir(warmupBaseDir),
			path,
			err,
		)
		return
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	for _, k := range klines {
		entry := warmupLogEntry{
			OpenTime:  time.UnixMilli(int64(k[0].(float64))).UTC().Format("2006-01-02T15:04:05.000Z"),
			Open:      parseFloat(k[1]),
			High:      parseFloat(k[2]),
			Low:       parseFloat(k[3]),
			Close:     parseFloat(k[4]),
			Volume:    parseFloat(k[5]),
			CloseTime: time.UnixMilli(int64(k[6].(float64))).UTC().Format("2006-01-02T15:04:05.000Z"),
		}
		if err := encoder.Encode(entry); err != nil {
			log.Printf(
				"[warmup] symbol=%s interval=%s shared_dir=%s failed_to=encode_file path=%s err=%v",
				symbol,
				interval,
				formatWarmupSharedDir(warmupBaseDir),
				path,
				err,
			)
			return
		}
	}

	log.Printf(
		"[warmup] symbol=%s interval=%s shared_dir=%s saved_raw_klines=%d path=%s",
		symbol,
		interval,
		formatWarmupSharedDir(warmupBaseDir),
		len(klines),
		path,
	)
}

// warmupRootDir 返回 warmup 持久化根目录，优先使用共享目录，未配置时回退旧目录结构。
func (a *KlineAggregator) warmupRootDir() string {
	if a == nil {
		return ""
	}
	if dir := strings.TrimSpace(a.sharedWarmupDir); dir != "" {
		return dir
	}
	if dir := strings.TrimSpace(a.klineLogDir); dir != "" {
		return filepath.Join(dir, "warmup")
	}
	return ""
}

// formatWarmupSharedDir 统一输出 warmup 共享目录，未配置时提供占位值，方便日志 grep 和采集。
func formatWarmupSharedDir(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return "n/a"
	}
	return dir
}
