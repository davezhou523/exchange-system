package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"exchange-system/app/market/rpc/internal/aggregator"
	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/kafka"
	"exchange-system/app/market/rpc/internal/universepool"
	"exchange-system/app/market/rpc/internal/websocket"
	"exchange-system/common/pb/market"
)

// binanceAPIURL Binance Futures REST API 地址
const binanceAPIURL = "https://fapi.binance.com"

type ServiceContext struct {
	Config config.Config

	marketProducer *kafka.Producer
	depthProducer  *kafka.Producer
	wsClient       *websocket.BinanceWebSocketClient
	agg            *aggregator.KlineAggregator
	universeMgr    *universepool.Manager
	chWriter       *clickHouseWriter
	cancel         context.CancelFunc
}

// snapshotUpdater 定义 UniversePool 快照更新能力，便于 dispatcher 解耦与测试。
type snapshotUpdater interface {
	// UpdateSnapshotFromKline 使用闭合 K 线同步刷新 UniversePool 评估快照。
	UpdateSnapshotFromKline(k *market.Kline)
}

// klineDispatcher 把 websocket 收到的 1m 闭合 K 线同时分发给 UniversePool 和聚合器。
type klineDispatcher struct {
	agg         *aggregator.KlineAggregator
	universeMgr snapshotUpdater
}

// OnKline 在不破坏现有聚合链路的前提下，同步更新动态币池快照缓存。
func (d *klineDispatcher) OnKline(ctx context.Context, k *market.Kline) {
	if d == nil || k == nil {
		return
	}
	// UniversePool 快照属于“观察输入”，优先于聚合发射链路同步更新，避免 warmup 期间出现 snapshot 空窗。
	if d.universeMgr != nil {
		d.universeMgr.UpdateSnapshotFromKline(k)
	}
	if d.agg != nil {
		d.agg.OnKline(ctx, k)
	}
}

// NewServiceContext 初始化 market 服务依赖，并在启动 WebSocket 前完成预热与补数。
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c.SharedWarmupDir = resolveSharedWarmupDir(c.SharedWarmupDir)
	if c.WarmupCleanupOnStartup {
		if err := cleanupSharedWarmupRoot(c.SharedWarmupDir); err != nil {
			cancel()
			return nil, err
		}
	}

	producer, err := kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.Kline)
	if err != nil {
		cancel()
		return nil, err
	}
	var depthProducer *kafka.Producer
	if c.Kafka.Topics.Depth != "" {
		depthProducer, err = kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.Depth)
		if err != nil {
			_ = producer.Close()
			cancel()
			return nil, err
		}
	}

	agg := aggregator.NewKlineAggregator(aggregator.StandardIntervals, producer, c.KlineLogDir, c.WatermarkDelay, aggregator.IndicatorParams{
		Ema21Period: c.Indicators.Ema21Period,
		Ema55Period: c.Indicators.Ema55Period,
		RsiPeriod:   c.Indicators.RsiPeriod,
		AtrPeriod:   c.Indicators.AtrPeriod,
	})
	// SharedWarmupDir 供 strategy 等下游服务读取预热快照，避免复用消费验证目录。
	agg.SetSharedWarmupDir(c.SharedWarmupDir)

	// 设置每个周期的独立指标参数（指标粒度与K线周期对齐）
	intervalConfigs := make(map[string]aggregator.IntervalIndicatorConfig)
	for name, cfg := range c.IntervalIndicators {
		intervalConfigs[name] = aggregator.IntervalIndicatorConfig{
			Ema21Period: cfg.Ema21Period,
			Ema55Period: cfg.Ema55Period,
			RsiPeriod:   cfg.RsiPeriod,
			AtrPeriod:   cfg.AtrPeriod,
		}
	}
	agg.SetIntervalIndicators(intervalConfigs)

	// 设置指标计算模式
	// "closed"：当前K线参与计算（默认，适合回测和收盘后下单）
	// "previous"：只用历史K线（适合实盘未收盘提前信号）
	switch c.IndicatorMode {
	case "previous":
		agg.SetIndicatorMode(aggregator.IndicatorPrevious)
	default:
		agg.SetIndicatorMode(aggregator.IndicatorClosed)
	}

	// 设置 K 线发射模式
	// "watermark"：watermark 确认后才发射（默认，保证数据完整性）
	// "immediate"：bucket 完成后立即发射（低延迟，适合交易信号）
	switch c.EmitMode {
	case "immediate":
		agg.SetEmitMode(aggregator.EmitImmediate)
	default:
		agg.SetEmitMode(aggregator.EmitWatermark)
	}

	// 设置延迟容忍窗口（Flink watermark + lateness 模型）
	// 超过 watermark + allowedLateness 的迟到数据将被丢弃
	agg.SetAllowedLateness(c.AllowedLateness)

	// 设置 watermark buffer 最大存活时间
	agg.SetMaxWatermarkBufferAge(c.MaxWatermarkBufferAge)

	// 设置 worker 空闲 GC（清理长时间无数据的 symbol worker，防内存泄漏）
	agg.SetWorkerGC(c.WorkerGC.GCInterval, c.WorkerGC.MaxIdleTime)

	// 设置历史数据预热：启动时从 Binance REST API 拉取历史K线填充 ring buffer
	// 使 EMA/RSI/ATR 从第一根实时K线起就有正确值（而非冷启动的假值）
	warmupper := aggregator.NewBinanceWarmupper(binanceAPIURL, c.Binance.Proxy)
	agg.SetWarmupper(warmupper)
	agg.SetWarmupPages(c.WarmupPages)

	chWriter, err := newClickHouseWriter(ctx, c.ClickHouse)
	if err != nil {
		if depthProducer != nil {
			_ = depthProducer.Close()
		}
		_ = producer.Close()
		cancel()
		return nil, err
	}

	// 校验配置组合安全性：EmitImmediate + IndicatorClosed = 实盘未来函数风险
	// 必须在 SetEmitMode + SetIndicatorMode 之后调用
	agg.ValidateConfig()

	dispatcher := &klineDispatcher{
		agg: agg,
	}

	wsClient := websocket.NewBinanceWebSocketClient(
		c.Binance.WebSocketURL,
		c.Binance.Proxy,
		c.Binance.Symbols,
		c.Binance.Intervals,
		producer,
		depthProducer,
		dispatcher,
	)

	var universeMgr *universepool.Manager
	if c.UniversePool.Enabled {
		universeCfg := universepool.Config{
			Enabled:                  c.UniversePool.Enabled,
			CandidateSymbols:         append([]string(nil), c.UniversePool.CandidateSymbols...),
			AllowList:                append([]string(nil), c.UniversePool.AllowList...),
			BlockList:                append([]string(nil), c.UniversePool.BlockList...),
			ValidationMode:           c.UniversePool.ValidationMode,
			TrendPreferredSymbols:    append([]string(nil), c.UniversePool.TrendPreferredSymbols...),
			RangePreferredSymbols:    append([]string(nil), c.UniversePool.RangePreferredSymbols...),
			BreakoutPreferredSymbols: append([]string(nil), c.UniversePool.BreakoutPreferredSymbols...),
			BreakoutAtrPctMin:        c.UniversePool.BreakoutAtrPctMin,
			BreakoutAtrPctExitMin:    c.UniversePool.BreakoutAtrPctExitMin,
			EvaluateInterval:         c.UniversePool.EvaluateInterval,
			MinActiveDuration:        c.UniversePool.MinActiveDuration,
			MinInactiveDuration:      c.UniversePool.MinInactiveDuration,
			CooldownDuration:         c.UniversePool.CooldownDuration,
			AddScoreThreshold:        c.UniversePool.AddScoreThreshold,
			RemoveScoreThreshold:     c.UniversePool.RemoveScoreThreshold,
			Warmup:                   c.UniversePool.Warmup,
		}
		universeLogger := universepool.NewJSONLLogger(c.UniversePool.LogDir)
		universeMgr = universepool.NewManager(universeCfg, nil, agg, wsClient, universeLogger)
		agg.SetEmitObserver(func(k *market.Kline) {
			if universeMgr != nil {
				universeMgr.UpdateSnapshotFromKline(k)
			}
		})
		dispatcher.universeMgr = universeMgr
	}

	if chWriter != nil {
		prevObserver := dispatcher.universeMgr
		agg.SetEmitObserver(func(k *market.Kline) {
			if prevObserver != nil {
				prevObserver.UpdateSnapshotFromKline(k)
			}
			chWriter.Enqueue(k)
		})
		if universeMgr != nil {
			dispatcher.universeMgr = universeMgr
		}
	}

	// 启动阶段主动为已配置交易对创建 worker，并提前拉取 warmup 数据到共享目录。
	// 这样 strategy 在 market 刚启动时就能读到预热快照，而不是等首条实时 K 线触发懒加载。
	agg.EnsureWarmupForSymbols(buildBootstrapWarmupSymbols(c))

	if err := recoverKlineFactGap(ctx, c, agg, chWriter, warmupper); err != nil {
		if chWriter != nil {
			_ = chWriter.Close()
		}
		if depthProducer != nil {
			_ = depthProducer.Close()
		}
		_ = producer.Close()
		agg.Stop()
		cancel()
		return nil, err
	}

	wsClient.StartInBackground(ctx)
	if universeMgr != nil {
		go universeMgr.Start(ctx)
	}

	return &ServiceContext{
		Config:         c,
		marketProducer: producer,
		depthProducer:  depthProducer,
		wsClient:       wsClient,
		agg:            agg,
		universeMgr:    universeMgr,
		chWriter:       chWriter,
		cancel:         cancel,
	}, nil
}

// buildBootstrapWarmupSymbols 合并 websocket 订阅列表和 Universe 候选集，得到启动阶段需要提前预热的交易对集合。
func buildBootstrapWarmupSymbols(c config.Config) []string {
	symbols := make([]string, 0, len(c.Binance.Symbols)+len(c.UniversePool.CandidateSymbols))
	symbols = append(symbols, c.Binance.Symbols...)
	symbols = append(symbols, c.UniversePool.CandidateSymbols...)
	return symbols
}

// resolveSharedWarmupDir 把相对 SharedWarmupDir 解析到仓库根目录，避免从不同服务目录启动时写到错误路径。
func resolveSharedWarmupDir(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return ""
	}
	if filepath.IsAbs(dir) {
		return filepath.Clean(dir)
	}
	if repoRoot, ok := findExchangeSystemRepoRoot(); ok {
		return filepath.Join(repoRoot, filepath.Clean(dir))
	}
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return filepath.Clean(dir)
}

// cleanupSharedWarmupRoot 在服务启动时清空共享 warmup 根目录，避免 strategy 读到上一轮启动残留的旧快照。
func cleanupSharedWarmupRoot(dir string) error {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil
	}
	cleanDir := filepath.Clean(dir)
	if !isSafeWarmupRoot(cleanDir) {
		return fmt.Errorf("refuse to cleanup non-warmup shared dir: %s", cleanDir)
	}
	if err := os.RemoveAll(cleanDir); err != nil {
		return fmt.Errorf("cleanup shared warmup dir %s: %w", cleanDir, err)
	}
	if err := os.MkdirAll(cleanDir, 0o755); err != nil {
		return fmt.Errorf("recreate shared warmup dir %s: %w", cleanDir, err)
	}
	log.Printf("[warmup] shared_dir=%s cleaned up warmup root", cleanDir)
	return nil
}

// isSafeWarmupRoot 用固定目录后缀校验待清理路径，降低误删其他运行时目录的风险。
func isSafeWarmupRoot(dir string) bool {
	dir = filepath.ToSlash(strings.TrimSpace(dir))
	if dir == "" {
		return false
	}
	return strings.HasSuffix(dir, "/runtime/shared/kline/warmup") || dir == "runtime/shared/kline/warmup"
}

// findExchangeSystemRepoRoot 从当前工作目录向上查找 go.mod，定位 exchange-system 仓库根目录。
func findExchangeSystemRepoRoot() (string, bool) {
	wd, err := os.Getwd()
	if err != nil {
		return "", false
	}
	dir := wd
	for {
		goModPath := filepath.Join(dir, "go.mod")
		content, readErr := os.ReadFile(goModPath)
		if readErr == nil && strings.Contains(string(content), "module exchange-system") {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

// Close 关闭 ServiceContext 持有的后台资源，尽量按依赖顺序释放。
func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.agg != nil {
		s.agg.Stop()
	}
	var firstErr error
	if s.wsClient != nil {
		if err := s.wsClient.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.marketProducer != nil {
		if err := s.marketProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.depthProducer != nil {
		if err := s.depthProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.chWriter != nil {
		if err := s.chWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// clickHouseWriter 负责把聚合后的 K 线异步写入 ClickHouse。
type clickHouseWriter struct {
	endpoint      string
	database      string
	username      string
	password      string
	source        string
	client        *http.Client
	queue         chan *market.Kline
	flushInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// clickHouseKlineRow 定义写入 ClickHouse 的 JSONEachRow 结构。
type clickHouseKlineRow struct {
	EventTime      string  `json:"event_time"`
	Symbol         string  `json:"symbol"`
	Interval       string  `json:"interval"`
	OpenTime       string  `json:"open_time"`
	CloseTime      string  `json:"close_time"`
	Open           float64 `json:"open"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	Close          float64 `json:"close"`
	Volume         float64 `json:"volume"`
	QuoteVolume    float64 `json:"quote_volume"`
	TakerBuyVolume float64 `json:"taker_buy_volume"`
	IsClosed       uint8   `json:"is_closed"`
	IsDirty        uint8   `json:"is_dirty"`
	DirtyReason    string  `json:"dirty_reason"`
	IsTradable     uint8   `json:"is_tradable"`
	IsFinal        uint8   `json:"is_final"`
	Ema21          float64 `json:"ema21"`
	Ema55          float64 `json:"ema55"`
	Rsi            float64 `json:"rsi"`
	Atr            float64 `json:"atr"`
	Source         string  `json:"source"`
}

// newClickHouseWriter 创建 ClickHouse 异步写入器。
func newClickHouseWriter(parent context.Context, cfg config.ClickHouseConfig) (*clickHouseWriter, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("clickhouse endpoint is required when enabled")
	}
	database := strings.TrimSpace(cfg.Database)
	if database == "" {
		database = "exchange_analytics"
	}
	source := strings.TrimSpace(cfg.Source)
	if source == "" {
		source = "market-rpc"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 2048
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	writer := &clickHouseWriter{
		endpoint:      endpoint,
		database:      database,
		username:      strings.TrimSpace(cfg.Username),
		password:      cfg.Password,
		source:        source,
		client:        &http.Client{Timeout: timeout},
		queue:         make(chan *market.Kline, queueSize),
		flushInterval: flushInterval,
		ctx:           ctx,
		cancel:        cancel,
	}
	writer.wg.Add(1)
	go writer.run()
	return writer, nil
}

// Enqueue 把 K 线投递到异步写入队列，满队列时直接丢弃避免阻塞行情链路。
func (w *clickHouseWriter) Enqueue(k *market.Kline) {
	if w == nil || k == nil {
		return
	}
	row := cloneKline(k)
	select {
	case w.queue <- row:
	default:
		log.Printf("[clickhouse] queue full, drop kline symbol=%s interval=%s open_time=%d", k.Symbol, k.Interval, k.OpenTime)
	}
}

// Close 停止后台协程并等待剩余批次完成。
func (w *clickHouseWriter) Close() error {
	if w == nil {
		return nil
	}
	w.cancel()
	w.wg.Wait()
	return nil
}

// run 后台批量消费 K 线队列，按时间或批大小刷入 ClickHouse。
func (w *clickHouseWriter) run() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batch := make([]*market.Kline, 0, 128)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.insertBatch(batch); err != nil {
			log.Printf("[clickhouse] insert kline batch failed: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-w.ctx.Done():
			for {
				select {
				case k := <-w.queue:
					if k != nil {
						batch = append(batch, k)
					}
				default:
					flush()
					return
				}
			}
		case k := <-w.queue:
			if k == nil {
				continue
			}
			batch = append(batch, k)
			if len(batch) >= 128 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// insertBatch 把一批 K 线编码为 JSONEachRow 并写入 ClickHouse。
func (w *clickHouseWriter) insertBatch(batch []*market.Kline) error {
	if w == nil || len(batch) == 0 {
		return nil
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	for _, item := range batch {
		if item == nil {
			continue
		}
		if err := encoder.Encode(w.buildRow(item)); err != nil {
			return fmt.Errorf("encode kline row: %w", err)
		}
	}
	if body.Len() == 0 {
		return nil
	}
	query := fmt.Sprintf("INSERT INTO %s.kline_fact FORMAT JSONEachRow", w.database)
	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, w.endpoint+"/?query="+url.QueryEscape(query), &body)
	if err != nil {
		return fmt.Errorf("build clickhouse request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if w.username != "" {
		req.SetBasicAuth(w.username, w.password)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("post clickhouse request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// buildRow 把 protobuf K 线转换为 ClickHouse 行结构。
func (w *clickHouseWriter) buildRow(k *market.Kline) clickHouseKlineRow {
	return clickHouseKlineRow{
		EventTime:      formatClickHouseDateTime64(k.GetEventTime()),
		Symbol:         strings.ToUpper(strings.TrimSpace(k.GetSymbol())),
		Interval:       strings.TrimSpace(k.GetInterval()),
		OpenTime:       formatClickHouseDateTime64(k.GetOpenTime()),
		CloseTime:      formatClickHouseDateTime64(k.GetCloseTime()),
		Open:           k.GetOpen(),
		High:           k.GetHigh(),
		Low:            k.GetLow(),
		Close:          k.GetClose(),
		Volume:         k.GetVolume(),
		QuoteVolume:    k.GetQuoteVolume(),
		TakerBuyVolume: k.GetTakerBuyVolume(),
		IsClosed:       boolToUInt8(k.GetIsClosed()),
		IsDirty:        boolToUInt8(k.GetIsDirty()),
		DirtyReason:    strings.TrimSpace(k.GetDirtyReason()),
		IsTradable:     boolToUInt8(k.GetIsTradable()),
		IsFinal:        boolToUInt8(k.GetIsFinal()),
		Ema21:          k.GetEma21(),
		Ema55:          k.GetEma55(),
		Rsi:            k.GetRsi(),
		Atr:            k.GetAtr(),
		Source:         w.source,
	}
}

// cloneKline 复制一份 K 线，避免异步写入读取到被后续修改的对象。
func cloneKline(k *market.Kline) *market.Kline {
	if k == nil {
		return nil
	}
	out := *k
	return &out
}

// formatClickHouseDateTime64 把毫秒时间戳转换为固定 3 位小数的 ClickHouse 时间字符串。
func formatClickHouseDateTime64(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.000")
}

// boolToUInt8 把布尔值转换为 ClickHouse 常用的 UInt8 标记。
func boolToUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
