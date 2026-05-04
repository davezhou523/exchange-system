package aggregator

import (
	"container/heap"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"exchange-system/app/market/rpc/internal/universepool"
	"exchange-system/common/pb/market"
)

// IntervalDef defines a target aggregation interval.
type IntervalDef struct {
	Name     string
	Duration time.Duration
}

// Standard intervals aggregated from 1m klines.
var StandardIntervals = []IntervalDef{
	{Name: "1m", Duration: 1 * time.Minute},
	{Name: "15m", Duration: 15 * time.Minute},
	{Name: "1h", Duration: 1 * time.Hour},
	{Name: "4h", Duration: 4 * time.Hour},
}

// IndicatorParams holds the parameters for technical indicator calculation.
type IndicatorParams struct {
	Ema21Period int
	Ema55Period int
	RsiPeriod   int
	AtrPeriod   int
}

// IndicatorMode 指标计算模式，控制当前K线是否参与指标计算
type IndicatorMode int

const (
	// IndicatorClosed 当前K线参与计算（默认模式）
	// 适用于：回测（用收盘价交易）、实盘收盘后下单
	// 指标描述的是"截至当前K线的状态"
	IndicatorClosed IndicatorMode = iota

	// IndicatorPrevious 只用历史K线计算（严格模式）
	// 适用于：实盘未收盘提前信号、避免未来函数风险
	// 指标描述的是"截至上一根K线的状态"，更保守
	IndicatorPrevious
)

// EmitMode 控制 K 线的发射时机
type EmitMode int

const (
	// EmitWatermark watermark 确认后才发射（默认，保证数据完整性）
	// 适用场景：Kafka 存储、数据归档、回测数据源
	// 延迟 = watermarkDelay，但数据完整性有保障
	EmitWatermark EmitMode = iota

	// EmitImmediate bucket 完成后立即发射（低延迟，适合交易信号）
	// 适用场景：实盘交易信号、短周期策略
	// 延迟 = 0，但可能因迟到数据导致 dirty 重发
	EmitImmediate
)

// IntervalIndicatorConfig 每个周期的指标配置（指标粒度与K线周期对齐）
type IntervalIndicatorConfig struct {
	Ema21Period int
	Ema55Period int
	RsiPeriod   int
	AtrPeriod   int
}

// Metrics tracks aggregator statistics.
type Metrics struct {
	Received1m       atomic.Int64
	Emitted1m        atomic.Int64
	Emitted15m       atomic.Int64
	Emitted1h        atomic.Int64
	Emitted4h        atomic.Int64
	GapsDetected     atomic.Int64
	KafkaSendErrors  atomic.Int64
	WatermarkWaits   atomic.Int64
	WatermarkFlushes atomic.Int64
}

// KlineAggregator aggregates 1m klines into larger intervals.
// Each symbol runs in its own goroutine with its own lock-free state,
// so Kafka IO for one symbol never blocks another.
type KlineAggregator struct {
	intervals       []IntervalDef
	producer        KafkaProducer
	metrics         Metrics
	klineLogDir     string
	sharedWarmupDir string
	watermarkDelay  time.Duration
	indicatorParams IndicatorParams
	// intervalIndicators 为每个周期独立配置指标参数，实现"指标按周期计算"
	// 例如：15m K线用 15m 收盘价序列算 EMA，而非用 1m 数据
	// 如果某周期未配置，则回退到 indicatorParams（兼容旧配置）
	intervalIndicators map[string]IntervalIndicatorConfig

	// indicatorMode 指标计算模式，控制当前K线是否参与指标计算
	indicatorMode IndicatorMode

	// emitMode 控制 K 线的发射时机
	emitMode EmitMode

	// maxWatermarkBufferAge：watermark buffer 中 bucket 的最大存活时间
	maxWatermarkBufferAge time.Duration

	// allowedLateness 延迟容忍窗口（Flink watermark + lateness 模型）
	allowedLateness time.Duration

	// timeSource 时间源，支持实盘和回测
	timeSource TimeSource

	// workerGCInterval worker 空闲 GC 的检查间隔
	workerGCInterval time.Duration

	// maxWorkerIdleTime worker 最大空闲时间
	maxWorkerIdleTime time.Duration

	// gcStarted 确保 worker GC 协程只启动一次
	gcStarted bool

	// warmupper 历史数据预热器（可选）
	warmupper HistoryWarmupper

	// warmupPages 预热分页数量，每页最多1500根K线。
	// 更多页 = 更多历史数据 = 递推指标更接近交易所真实值。
	// 默认 3 页（3×1500=4500根），0 或负数也使用默认值3。
	// 例如：15m 周期 4500根 ≈ 46.9天数据，足以让 EMA55/ATR14 充分收敛。
	warmupPages int

	// asyncSendQueue 异步 Kafka 发送队列
	asyncSendQueue chan *market.Kline

	// asyncSenderWg 异步发送协程的 WaitGroup
	asyncSenderWg sync.WaitGroup

	// emitObserver 在聚合后 K 线真正发射时收到回调，可用于同步更新轻量观察缓存。
	emitObserver func(*market.Kline)

	// kafkaSendEnabled 控制 emit 后是否继续把 K 线异步发送到 Kafka。
	// 断点补齐期间可临时关闭，避免把历史补数再次广播给下游实时链路。
	kafkaSendEnabled atomic.Bool

	mu      sync.Mutex
	workers map[string]*symbolWorker // symbol -> worker
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	logMu   sync.Mutex
	logFile map[string]*os.File // "SYMBOL/INTERVAL/DATE" -> file handle
}

// KafkaProducer sends aggregated kline data.
type KafkaProducer interface {
	SendMarketData(ctx context.Context, data interface{}) error
}

// TimeSource 时间源接口，支持实盘和回测两种模式。
type TimeSource interface {
	Now() time.Time
}

// SystemTimeSource 实盘时间源，使用系统时钟
type SystemTimeSource struct{}

func (s *SystemTimeSource) Now() time.Time { return time.Now().UTC() }

// SetEmitObserver 设置聚合后 K 线发射回调。
func (a *KlineAggregator) SetEmitObserver(fn func(*market.Kline)) {
	if a == nil {
		return
	}
	a.emitObserver = fn
}

// NewKlineAggregator creates a new aggregator for the given target intervals.
func NewKlineAggregator(intervals []IntervalDef, producer KafkaProducer, klineLogDir string, watermarkDelay time.Duration, indicatorParams IndicatorParams) *KlineAggregator {
	if watermarkDelay < 0 {
		watermarkDelay = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &KlineAggregator{
		intervals:             intervals,
		producer:              producer,
		workers:               make(map[string]*symbolWorker),
		logFile:               make(map[string]*os.File),
		ctx:                   ctx,
		cancel:                cancel,
		klineLogDir:           klineLogDir,
		watermarkDelay:        watermarkDelay,
		indicatorParams:       indicatorParams,
		intervalIndicators:    make(map[string]IntervalIndicatorConfig),
		maxWatermarkBufferAge: 5 * time.Minute,
		timeSource:            &SystemTimeSource{},
		workerGCInterval:      5 * time.Minute,
		maxWorkerIdleTime:     30 * time.Minute,
		asyncSendQueue:        make(chan *market.Kline, 4096),
	}
	a.kafkaSendEnabled.Store(true)

	// 启动异步 Kafka 发送协程
	a.asyncSenderWg.Add(1)
	go a.asyncKafkaSender()
	return a
}

// SetSharedWarmupDir 设置 warmup 共享目录，供其他服务读取启动恢复所需的预热快照。
func (a *KlineAggregator) SetSharedWarmupDir(dir string) {
	if a == nil {
		return
	}
	a.sharedWarmupDir = dir
}

// EnsureWarmupForSymbols 在服务启动阶段主动创建指定交易对的 worker，并立即触发历史预热。
// 这样无需等待首条 websocket K 线到来，就能先把 warmup 快照落到共享目录。
func (a *KlineAggregator) EnsureWarmupForSymbols(symbols []string) {
	if a == nil {
		return
	}
	for _, symbol := range normalizeWarmupSymbols(symbols) {
		a.getOrCreateWorker(symbol)
	}
}

// normalizeWarmupSymbols 统一清洗交易对列表，避免启动预热时因空值、大小写或重复导致重复创建 worker。
func normalizeWarmupSymbols(symbols []string) []string {
	if len(symbols) == 0 {
		return nil
	}
	result := make([]string, 0, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		normalized := strings.ToUpper(strings.TrimSpace(symbol))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

// SetIntervalIndicators 设置每个周期的独立指标参数。
func (a *KlineAggregator) SetIntervalIndicators(configs map[string]IntervalIndicatorConfig) {
	a.intervalIndicators = configs
}

// SetIndicatorMode 设置指标计算模式。
func (a *KlineAggregator) SetIndicatorMode(mode IndicatorMode) {
	a.indicatorMode = mode
}

// SetEmitMode 设置 K 线发射模式。
func (a *KlineAggregator) SetEmitMode(mode EmitMode) {
	a.emitMode = mode
}

// ValidateConfig 校验配置组合的安全性。
func (a *KlineAggregator) ValidateConfig() {
	if a.emitMode == EmitImmediate && a.indicatorMode == IndicatorClosed {
		panic("Invalid config: EmitImmediate + IndicatorClosed = future function risk. " +
			"Immediate mode must use IndicatorPrevious to avoid look-ahead bias in live trading. " +
			"Use IndicatorClosed only with EmitWatermark (or in backtest).")
	}
}

// SetMaxWatermarkBufferAge 设置 watermark buffer 的最大存活时间。
func (a *KlineAggregator) SetMaxWatermarkBufferAge(d time.Duration) {
	if d > 0 {
		a.maxWatermarkBufferAge = d
	}
}

// SetAllowedLateness 设置延迟容忍窗口。
func (a *KlineAggregator) SetAllowedLateness(d time.Duration) {
	if d > 0 {
		a.allowedLateness = d
	}
}

// SetTimeSource 设置时间源。
func (a *KlineAggregator) SetTimeSource(ts TimeSource) {
	if ts != nil {
		a.timeSource = ts
	}
}

// SetWorkerGC 设置 worker 空闲 GC 参数。
func (a *KlineAggregator) SetWorkerGC(gcInterval, maxIdleTime time.Duration) {
	if gcInterval > 0 {
		a.workerGCInterval = gcInterval
	}
	if maxIdleTime > 0 {
		a.maxWorkerIdleTime = maxIdleTime
	}
}

// SetKafkaSendEnabled 设置 emit 后是否继续异步发送 Kafka。
func (a *KlineAggregator) SetKafkaSendEnabled(enabled bool) {
	if a == nil {
		return
	}
	a.kafkaSendEnabled.Store(enabled)
}

// calcHistoryBufferSize 根据指标参数动态计算 ring buffer 容量。
func (a *KlineAggregator) calcHistoryBufferSize(intervalName string) int {
	const minBufferSize = 200

	params, ok := a.intervalIndicators[intervalName]
	if !ok {
		params = IntervalIndicatorConfig{
			Ema21Period: a.indicatorParams.Ema21Period,
			Ema55Period: a.indicatorParams.Ema55Period,
			RsiPeriod:   a.indicatorParams.RsiPeriod,
			AtrPeriod:   a.indicatorParams.AtrPeriod,
		}
	}

	maxPeriod := params.Ema55Period
	if params.RsiPeriod > maxPeriod {
		maxPeriod = params.RsiPeriod
	}
	if params.AtrPeriod > maxPeriod {
		maxPeriod = params.AtrPeriod
	}

	required := maxPeriod * 3
	if required < minBufferSize {
		return minBufferSize
	}
	return required
}

// OnKline dispatches a closed 1m kline to the per-symbol worker goroutine.
func (a *KlineAggregator) OnKline(ctx context.Context, k *market.Kline) {
	a.feedKline(ctx, k, true)
}

// ReplayKline 以阻塞方式投递闭合 1m K 线，适用于启动补数等不能丢数据的场景。
func (a *KlineAggregator) ReplayKline(ctx context.Context, k *market.Kline) {
	a.feedKline(ctx, k, false)
}

// feedKline 按指定投递策略把闭合 1m K 线送入 symbol worker。
func (a *KlineAggregator) feedKline(ctx context.Context, k *market.Kline, dropIfFull bool) {
	if a == nil || k == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if k.Interval != "1m" || !k.IsClosed {
		return
	}

	select {
	case <-a.ctx.Done():
		return
	default:
	}

	a.metrics.Received1m.Add(1)

	w := a.getOrCreateWorker(k.Symbol)

	a.startWorkerGCOnce()

	if dropIfFull {
		select {
		case w.ch <- k:
		default:
			log.Printf("[aggregator] WARN: %s channel full, dropping 1m kline openTime=%d", k.Symbol, k.OpenTime)
		}
		return
	}

	select {
	case <-a.ctx.Done():
		return
	case <-ctx.Done():
		return
	case w.ch <- k:
	}
}

// startWorkerGCOnce 确保 worker GC 协程只启动一次。
func (a *KlineAggregator) startWorkerGCOnce() {
	a.mu.Lock()
	if a.gcStarted || a.workerGCInterval <= 0 {
		a.mu.Unlock()
		return
	}
	a.gcStarted = true
	a.mu.Unlock()

	a.wg.Add(1)
	go a.gcWorkers()
}

// gcWorkers 定期扫描所有 worker，清理空闲超过 maxWorkerIdleTime 的 worker。
func (a *KlineAggregator) gcWorkers() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.workerGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.cleanIdleWorkers()
		}
	}
}

// cleanIdleWorkers 执行一轮 worker 空闲清理。
func (a *KlineAggregator) cleanIdleWorkers() {
	now := a.timeSource.Now()

	a.mu.Lock()
	defer a.mu.Unlock()

	for symbol, w := range a.workers {
		if now.Sub(w.lastActiveTime) > a.maxWorkerIdleTime {
			log.Printf("[aggregator] GC: cleaning idle worker %s | idle=%v | lastActive=%s",
				symbol, now.Sub(w.lastActiveTime).Round(time.Second),
				w.lastActiveTime.Format("15:04:05"))

			close(w.ch)

			for w.watermarkHeap.Len() > 0 {
				bk := heap.Pop(&w.watermarkHeap).(*emitBucket)
				bk.bucket.markDirty("worker_gc")
				a.emitKline(context.Background(), w.symbol, bk.interval, bk.bucket)
			}

			for _, iv := range a.intervals {
				if b, ok := w.buckets[iv.Name]; ok && b.initialized {
					b.markDirty("worker_gc")
					a.emitKline(context.Background(), w.symbol, iv.Name, b)
				}
			}

			delete(a.workers, symbol)
		}
	}
}

// Stop gracefully shuts down all workers and emits incomplete buckets.
func (a *KlineAggregator) Stop() {
	a.cancel()
	a.wg.Wait()

	a.mu.Lock()
	for _, w := range a.workers {
		for w.watermarkHeap.Len() > 0 {
			bk := heap.Pop(&w.watermarkHeap).(*emitBucket)
			bk.bucket.markDirty("shutdown_flush")
			a.emitKline(context.Background(), w.symbol, bk.interval, bk.bucket)
		}

		for _, iv := range a.intervals {
			if b, ok := w.buckets[iv.Name]; ok && b.initialized {
				a.emitKline(context.Background(), w.symbol, iv.Name, b)
			}
		}
	}
	a.mu.Unlock()

	close(a.asyncSendQueue)
	a.asyncSenderWg.Wait()

	a.logMu.Lock()
	for key, f := range a.logFile {
		_ = f.Close()
		delete(a.logFile, key)
	}
	a.logMu.Unlock()
}

// GetMetrics returns a snapshot of the current metrics.
func (a *KlineAggregator) GetMetrics() Metrics {
	m := Metrics{}
	m.Received1m.Store(a.metrics.Received1m.Load())
	m.Emitted1m.Store(a.metrics.Emitted1m.Load())
	m.Emitted15m.Store(a.metrics.Emitted15m.Load())
	m.Emitted1h.Store(a.metrics.Emitted1h.Load())
	m.Emitted4h.Store(a.metrics.Emitted4h.Load())
	m.GapsDetected.Store(a.metrics.GapsDetected.Load())
	m.KafkaSendErrors.Store(a.metrics.KafkaSendErrors.Load())
	m.WatermarkWaits.Store(a.metrics.WatermarkWaits.Load())
	m.WatermarkFlushes.Store(a.metrics.WatermarkFlushes.Load())
	return m
}

// GetWarmupStatus 返回指定交易对当前的 warmup 状态快照，供动态币池状态机判断是否可进入 active。
func (a *KlineAggregator) GetWarmupStatus(symbol string) universepool.WarmupStatus {
	status := universepool.WarmupStatus{
		Symbol: symbol,
	}
	if a == nil || symbol == "" {
		status.LastIncompleteReason = "invalid_symbol"
		return status
	}

	a.mu.Lock()
	w, ok := a.workers[symbol]
	a.mu.Unlock()
	if !ok || w == nil {
		status.LastIncompleteReason = "no_worker"
		return status
	}

	status.LastUpdatedAt = w.lastActiveTime
	status.HasEnough1mBars = w.hasHistoryReady("1m")
	status.Has15mReady = w.hasHistoryReady("15m")
	status.Has1hReady = w.hasHistoryReady("1h")
	status.Has4hReady = w.hasHistoryReady("4h")
	status.IndicatorsReady = w.hasIndicatorsReady("1m")

	warmupReady := w.warmupReady.Load()
	switch {
	case !warmupReady:
		status.LastIncompleteReason = "warmup_pending"
	case !status.HasEnough1mBars:
		status.LastIncompleteReason = "missing_1m_history"
	case !status.Has15mReady:
		status.LastIncompleteReason = "missing_15m_history"
	case !status.Has1hReady:
		status.LastIncompleteReason = "missing_1h_history"
	case !status.Has4hReady:
		status.LastIncompleteReason = "missing_4h_history"
	case !status.IndicatorsReady:
		status.LastIncompleteReason = "indicators_not_ready"
	default:
		status.Ready = true
	}

	return status
}

// --- per-symbol worker ---

type symbolWorker struct {
	symbol  string
	ch      chan *market.Kline
	buckets map[string]*bucket // interval name -> bucket (incremental aggregation)
	agg     *KlineAggregator

	// Watermark: 用小顶堆管理已完成但等待 watermark 的 bucket
	watermarkHeap watermarkBucketHeap

	// intervalHistories 为每个周期维护独立的收盘价/最高价/最低价历史
	intervalHistories map[string]*klineRingBuffer

	// lastActiveTime 最后一次收到 K 线数据的时间
	lastActiveTime time.Time

	// warmupReady 预热完成标志（原子操作，无锁）
	warmupReady   atomic.Bool
	pendingMu     sync.Mutex      // 保护 pendingKlines 的并发访问
	pendingKlines []*market.Kline // 预热期间缓存的K线（预热完成后清空）
}

// hasHistoryReady 判断某个周期是否已经至少积累到可观测的历史数据。
func (w *symbolWorker) hasHistoryReady(interval string) bool {
	if w == nil {
		return false
	}
	history := w.intervalHistories[interval]
	return history != nil && history.count > 0
}

// hasIndicatorsReady 判断某个周期的递推指标状态是否已经初始化完成。
func (w *symbolWorker) hasIndicatorsReady(interval string) bool {
	if w == nil {
		return false
	}
	history := w.intervalHistories[interval]
	if history == nil {
		return false
	}
	return history.state.ema21Init &&
		history.state.ema55Init &&
		history.state.rsiInit &&
		history.state.atrInit
}

// emitBucket wraps a completed bucket ready for watermark-delayed emit.
type emitBucket struct {
	bucket   *bucket
	interval string
	dirty    bool      // mark dirty if flushing on shutdown
	enqueued time.Time // 入队时间，用于 maxWatermarkBufferAge 超时强制 flush
}

// watermarkBucketHeap 实现小顶堆（按 bucket.CloseTime 升序排列）
type watermarkBucketHeap []*emitBucket

func (h watermarkBucketHeap) Len() int { return len(h) }

func (h watermarkBucketHeap) Less(i, j int) bool {
	return h[i].bucket.CloseTime < h[j].bucket.CloseTime
}

func (h watermarkBucketHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *watermarkBucketHeap) Push(x interface{}) {
	*h = append(*h, x.(*emitBucket))
}

func (h *watermarkBucketHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[:n-1]
	return item
}

func (a *KlineAggregator) getOrCreateWorker(symbol string) *symbolWorker {
	a.mu.Lock()
	defer a.mu.Unlock()

	if w, ok := a.workers[symbol]; ok {
		return w
	}

	intervalHistories := make(map[string]*klineRingBuffer)
	for _, iv := range a.intervals {
		intervalHistories[iv.Name] = newKlineRingBuffer(a.calcHistoryBufferSize(iv.Name))
	}

	w := &symbolWorker{
		symbol:            symbol,
		ch:                make(chan *market.Kline, 256),
		buckets:           make(map[string]*bucket),
		agg:               a,
		watermarkHeap:     make(watermarkBucketHeap, 0),
		intervalHistories: intervalHistories,
		lastActiveTime:    a.timeSource.Now(),
	}
	a.workers[symbol] = w

	a.wg.Add(1)
	go w.run()

	// 预热：从交易所拉取历史K线填充 ring buffer
	// 预热期间，收到的K线会缓存在 pendingKlines 中，预热完成后批量处理
	if a.warmupper != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			if err := a.WarmupHistory(context.Background(), symbol); err != nil {
				log.Printf("[aggregator] warmup failed for %s: %v (will start with cold indicators)", symbol, err)
			}
			// 预热完成（无论成功失败），通知 worker 批量处理缓存K线
			w.onWarmupComplete()
		}()
	} else {
		w.warmupReady.Store(true)
	}

	return w
}

func (w *symbolWorker) run() {
	defer w.agg.wg.Done()

	for {
		select {
		case k, ok := <-w.ch:
			if !ok {
				return
			}
			w.handleKline(k)
		case <-w.agg.ctx.Done():
			for {
				select {
				case k, ok := <-w.ch:
					if !ok {
						return
					}
					w.handleKline(k)
				default:
					return
				}
			}
		}
	}
}

// handleKline 处理收到的K线数据。
// 预热期间缓存K线，预热完成后直接处理。
func (w *symbolWorker) handleKline(k *market.Kline) {
	if w.warmupReady.Load() {
		w.processKline(k)
		return
	}

	const maxPendingKlines = 500
	w.pendingMu.Lock()
	if len(w.pendingKlines) >= maxPendingKlines {
		w.pendingKlines = w.pendingKlines[1:]
	}
	w.pendingKlines = append(w.pendingKlines, k)
	w.pendingMu.Unlock()
}

// onWarmupComplete 预热完成后的回调，批量处理缓存K线并标记就绪。
func (w *symbolWorker) onWarmupComplete() {
	w.pendingMu.Lock()
	pending := w.pendingKlines
	w.pendingKlines = nil
	w.pendingMu.Unlock()

	for _, k := range pending {
		w.processKline(k)
	}

	w.warmupReady.Store(true)

	log.Printf("[aggregator] %s warmup complete, processed %d pending klines", w.symbol, len(pending))
}

func (w *symbolWorker) processKline(k *market.Kline) {
	w.lastActiveTime = w.agg.timeSource.Now()

	// --- Watermark: check if any buffered buckets can now be emitted ---
	watermark := k.CloseTime - w.agg.watermarkDelay.Milliseconds()

	// --- Flink lateness 模型：丢弃超过容忍窗口的迟到数据 ---
	if w.agg.allowedLateness > 0 && k.OpenTime < watermark-w.agg.allowedLateness.Milliseconds() {
		log.Printf("[aggregator] LATE DATA DROPPED: %s | openTime=%d watermark=%d lateness=%v | openTime < %d",
			k.Symbol, k.OpenTime, watermark, w.agg.allowedLateness, watermark-w.agg.allowedLateness.Milliseconds())
		return
	}

	now := w.agg.timeSource.Now()

	// 检查 watermark 堆
	for w.watermarkHeap.Len() > 0 {
		bk := w.watermarkHeap[0]

		if bk.bucket.CloseTime <= watermark {
			heap.Pop(&w.watermarkHeap)
			w.agg.emitKline(context.Background(), w.symbol, bk.interval, bk.bucket)
			continue
		}

		if w.agg.maxWatermarkBufferAge > 0 && now.Sub(bk.enqueued) > w.agg.maxWatermarkBufferAge {
			heap.Pop(&w.watermarkHeap)
			bk.bucket.markDirty("watermark_timeout")
			log.Printf("[aggregator] WATERMARK TIMEOUT: %s %s | enqueued=%s | age=%v | forcing dirty flush",
				w.symbol, bk.interval, bk.enqueued.Format("15:04:05.000"), now.Sub(bk.enqueued))
			w.agg.emitKline(context.Background(), w.symbol, bk.interval, bk.bucket)
			w.agg.metrics.WatermarkFlushes.Add(1)
			continue
		}

		break
	}

	// Process each target interval
	for _, iv := range w.agg.intervals {
		requiredMinutes := int(iv.Duration / time.Minute)

		// 1m K线：直接推入 history ring buffer 并计算指标
		if requiredMinutes == 1 {
			w.process1mIndicators(k)
			continue
		}

		latestPeriodOpenTime := alignToInterval(k.OpenTime, iv.Duration)
		b, exists := w.buckets[iv.Name]

		// --- Time-driven: flush bucket when kline crosses period boundary ---
		if exists && b.initialized && latestPeriodOpenTime != b.OpenTime {
			if b.count < requiredMinutes {
				b.markDirty("incomplete_bucket")
				log.Printf("[aggregator] INCOMPLETE: %s %s | period=%s | collected=%d/%d | missing=%d | dirty=true",
					w.symbol, iv.Name,
					time.UnixMilli(b.OpenTime).UTC().Format("2006-01-02T15:04:05"),
					b.count, requiredMinutes, requiredMinutes-b.count)
			}

			history := w.intervalHistories[iv.Name]
			w.calcBucketIndicators(b, iv.Name)

			if history != nil {
				history.push(b.Open, b.High, b.Low, b.Close, b.OpenTime)
				w.updateIndicatorState(history, b, iv.Name)
			}

			if w.agg.emitMode == EmitImmediate || b.dirty {
				w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
			} else if w.agg.watermarkDelay > 0 {
				// 优化：如果当前 watermark 已经过了这个 bucket 的 closeTime，
				// 直接 emit 而不推入 watermark 堆，避免多等一轮 1m K线。
				// 典型场景：15m bucket 在 07:45 这一桶完成后，
				// 等到 08:00 的 1m 到来时触发 flush，
				// closeTime=07:59:59.999，watermark=08:00:57.999，
				// 已过 closeTime，无需等待。
				if b.CloseTime <= watermark {
					w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
				} else {
					heap.Push(&w.watermarkHeap, &emitBucket{
						bucket:   b,
						interval: iv.Name,
						enqueued: now,
					})
					w.agg.metrics.WatermarkWaits.Add(1)
				}
			} else {
				w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
			}
			delete(w.buckets, iv.Name)
			b = nil
			exists = false
		}

		// --- Gap detection ---
		if exists && b.initialized {
			expectedNext := b.CloseTime + 1
			if k.OpenTime > expectedNext {
				w.agg.metrics.GapsDetected.Add(1)
				gapDuration := time.Duration(k.OpenTime-expectedNext) * time.Millisecond
				log.Printf("[aggregator] GAP: %s %s — expected openTime=%d got=%d gap=%v",
					w.symbol, iv.Name, expectedNext, k.OpenTime, gapDuration)
				if b.count > 0 {
					w.calcBucketIndicators(b, iv.Name)
					history := w.intervalHistories[iv.Name]
					if history != nil {
						history.push(b.Open, b.High, b.Low, b.Close, b.OpenTime)
						w.updateIndicatorState(history, b, iv.Name)
					}
					b.markDirty("gap_detected")
					w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
				}
				delete(w.buckets, iv.Name)
				b = nil
				exists = false
			}
		}

		// --- Continuity check ---
		if exists && b.initialized && b.count > 0 {
			expectedOpenTime := b.prevOpenTime + 60000
			gap := k.OpenTime - expectedOpenTime
			if gap > 60000 {
				w.agg.metrics.GapsDetected.Add(1)
				log.Printf("[aggregator] CONTINUITY GAP: %s %s — expected openTime=%d got=%d gap=%dms | marking dirty",
					w.symbol, iv.Name, expectedOpenTime, k.OpenTime, gap)
				b.markDirty("continuity_gap")
			} else if gap > 0 && gap <= 60000 {
				log.Printf("[aggregator] MINOR JITTER: %s %s — expected openTime=%d got=%d gap=%dms | tolerated",
					w.symbol, iv.Name, expectedOpenTime, k.OpenTime, gap)
			}
		}

		// --- Create or incrementally update bucket ---
		if !exists || b == nil {
			b = &bucket{
				Open:           k.Open,
				High:           k.High,
				Low:            k.Low,
				Close:          k.Close,
				Volume:         k.Volume,
				QuoteVolume:    k.QuoteVolume,
				TakerBuyVolume: k.TakerBuyVolume,
				TakerBuyQuote:  k.TakerBuyQuote,
				NumTrades:      k.NumTrades,
				FirstTradeID:   k.FirstTradeId,
				LastTradeID:    k.LastTradeId,
				OpenTime:       latestPeriodOpenTime,
				CloseTime:      latestPeriodOpenTime + iv.Duration.Milliseconds() - 1,
				prevOpenTime:   k.OpenTime,
				count:          1,
				initialized:    true,
			}
			w.buckets[iv.Name] = b
		} else {
			if k.High > b.High {
				b.High = k.High
			}
			if k.Low < b.Low {
				b.Low = k.Low
			}
			b.Close = k.Close
			b.Volume += k.Volume
			b.QuoteVolume += k.QuoteVolume
			b.TakerBuyVolume += k.TakerBuyVolume
			b.TakerBuyQuote += k.TakerBuyQuote
			b.NumTrades += k.NumTrades
			if k.FirstTradeId < b.FirstTradeID {
				b.FirstTradeID = k.FirstTradeId
			}
			if k.LastTradeId > b.LastTradeID {
				b.LastTradeID = k.LastTradeId
			}
			b.prevOpenTime = k.OpenTime
			b.count++
		}
	}
}

// process1mIndicators 处理 1m K线的指标计算与发射。
// 1m K线本身是完整的（无需聚合），直接推入 1m history ring buffer 并计算指标，
// 然后通过 emitKline 发射（Kafka + jsonl 日志），与其他周期统一处理。
func (w *symbolWorker) process1mIndicators(k *market.Kline) {
	history := w.intervalHistories["1m"]
	if history == nil {
		return
	}

	b := &bucket{
		Open:        k.Open,
		High:        k.High,
		Low:         k.Low,
		Close:       k.Close,
		Volume:      k.Volume,
		QuoteVolume: k.QuoteVolume,
		OpenTime:    k.OpenTime,
		CloseTime:   k.CloseTime,
		NumTrades:   k.NumTrades,
		count:       1,
		initialized: true,
	}

	w.calcBucketIndicators(b, "1m")

	history.push(b.Open, b.High, b.Low, b.Close, b.OpenTime)
	w.updateIndicatorState(history, b, "1m")

	// 通过 emitKline 统一发射（Kafka + jsonl 日志）
	w.agg.emitKline(context.Background(), w.symbol, "1m", b)
}

// alignToInterval truncates a millisecond timestamp to the start of the containing interval period.
func alignToInterval(tsMs int64, d time.Duration) int64 {
	dMs := d.Milliseconds()
	return tsMs - (tsMs % dMs)
}
