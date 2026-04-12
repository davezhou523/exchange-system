package aggregator

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"exchange-system/common/pb/market"
)

// IntervalDef defines a target aggregation interval.
type IntervalDef struct {
	Name     string
	Duration time.Duration
}

// Standard intervals aggregated from 1m klines.
var StandardIntervals = []IntervalDef{
	{Name: "3m", Duration: 3 * time.Minute},
	{Name: "15m", Duration: 15 * time.Minute},
	{Name: "1h", Duration: 1 * time.Hour},
	{Name: "4h", Duration: 4 * time.Hour},
}

// IndicatorParams holds the parameters for technical indicator calculation.
type IndicatorParams struct {
	EmaFastPeriod int
	EmaSlowPeriod int
	RsiPeriod     int
	AtrPeriod     int
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
	EmaFastPeriod int
	EmaSlowPeriod int
	RsiPeriod     int
	AtrPeriod     int
}

// Metrics tracks aggregator statistics.
type Metrics struct {
	Received1m       atomic.Int64
	Emitted3m        atomic.Int64
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
	watermarkDelay  time.Duration
	indicatorParams IndicatorParams
	// intervalIndicators 为每个周期独立配置指标参数，实现"指标按周期计算"
	// 例如：15m K线用 15m 收盘价序列算 EMA，而非用 1m 数据
	// 如果某周期未配置，则回退到 indicatorParams（兼容旧配置）
	intervalIndicators map[string]IntervalIndicatorConfig

	// indicatorMode 指标计算模式，控制当前K线是否参与指标计算
	// IndicatorClosed（默认）：当前K线参与计算，适合回测和收盘后下单
	// IndicatorPrevious：只用历史K线，适合实盘未收盘提前信号
	indicatorMode IndicatorMode

	// emitMode 控制 K 线的发射时机
	// EmitWatermark（默认）：watermark 确认后才发射，保证数据完整性
	// EmitImmediate：bucket 完成后立即发射，低延迟适合交易信号
	emitMode EmitMode

	// maxWatermarkBufferAge：watermark buffer 中 bucket 的最大存活时间
	// 超过此时间仍未被 watermark 确认的 bucket 将被强制 flush（标记 dirty）
	// 解决：长时间无新数据 / symbol 停止推送 → buffer 内存泄漏
	maxWatermarkBufferAge time.Duration

	// allowedLateness 延迟容忍窗口（Flink watermark + lateness 模型）
	// 超过 watermark + allowedLateness 的迟到数据将被丢弃
	// 例如：watermarkDelay=2s, allowedLateness=2m → 数据超过当前 watermark 2 分钟后丢弃
	// 默认 0 表示不丢弃任何迟到数据（兼容现有行为）
	allowedLateness time.Duration

	// timeSource 时间源，支持实盘（SystemTimeSource）和回测（自定义实现）
	// 回测时使用基于 K 线时间的实现，保证完全 deterministic
	timeSource TimeSource

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
// 实盘使用 SystemTimeSource（time.Now()），回测使用自定义实现（基于 K 线时间），
// 保证回测完全 deterministic，可 replay。
type TimeSource interface {
	// Now 返回当前时间（用于 maxWatermarkBufferAge 超时检测、EventTime 等系统时间逻辑）
	Now() time.Time
}

// SystemTimeSource 实盘时间源，使用系统时钟
type SystemTimeSource struct{}

func (s *SystemTimeSource) Now() time.Time { return time.Now() }

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
		maxWatermarkBufferAge: 5 * time.Minute,     // 默认5分钟，超过则强制flush
		timeSource:            &SystemTimeSource{}, // 默认使用系统时钟
	}
	return a
}

// SetIntervalIndicators 设置每个周期的独立指标参数。
// 必须在 OnKline 之前调用。
func (a *KlineAggregator) SetIntervalIndicators(configs map[string]IntervalIndicatorConfig) {
	a.intervalIndicators = configs
}

// SetIndicatorMode 设置指标计算模式。
// 必须在 OnKline 之前调用。
// IndicatorClosed（默认）：当前K线参与计算，适合回测和收盘后下单
// IndicatorPrevious：只用历史K线，适合实盘未收盘提前信号
func (a *KlineAggregator) SetIndicatorMode(mode IndicatorMode) {
	a.indicatorMode = mode
}

// SetEmitMode 设置 K 线发射模式。
// 必须在 OnKline 之前调用。
// EmitWatermark（默认）：watermark 确认后才发射，保证数据完整性
// EmitImmediate：bucket 完成后立即发射，低延迟适合交易信号
func (a *KlineAggregator) SetEmitMode(mode EmitMode) {
	a.emitMode = mode
}

// SetMaxWatermarkBufferAge 设置 watermark buffer 的最大存活时间。
// 必须在 OnKline 之前调用。
func (a *KlineAggregator) SetMaxWatermarkBufferAge(d time.Duration) {
	if d > 0 {
		a.maxWatermarkBufferAge = d
	}
}

// SetAllowedLateness 设置延迟容忍窗口（Flink watermark + lateness 模型）。
// 必须在 OnKline 之前调用。
// 超过 watermark + allowedLateness 的迟到数据将被丢弃，不参与聚合。
// 例如：watermarkDelay=2s, allowedLateness=2m → 数据超过当前 watermark 2 分钟后丢弃
// 默认 0 表示不丢弃任何迟到数据（兼容现有行为）
func (a *KlineAggregator) SetAllowedLateness(d time.Duration) {
	if d > 0 {
		a.allowedLateness = d
	}
}

// SetTimeSource 设置时间源，支持实盘和回测两种模式。
// 必须在 OnKline 之前调用。
// 实盘：使用默认的 SystemTimeSource（time.Now()）
// 回测：使用自定义实现（基于 K 线时间），保证完全 deterministic，可 replay
func (a *KlineAggregator) SetTimeSource(ts TimeSource) {
	if ts != nil {
		a.timeSource = ts
	}
}

// calcHistoryBufferSize 根据指标参数动态计算 ring buffer 容量。
// 确保足够计算所有指标：max(200, maxPeriod*3)
// 其中 maxPeriod = max(EmaSlowPeriod, RsiPeriod, AtrPeriod)
// 例如：EMA200 需要 buffer ≥ 600，而默认 EMA26 只需 200
func (a *KlineAggregator) calcHistoryBufferSize(intervalName string) int {
	const minBufferSize = 200

	// 获取该周期的指标参数
	params, ok := a.intervalIndicators[intervalName]
	if !ok {
		params = IntervalIndicatorConfig{
			EmaFastPeriod: a.indicatorParams.EmaFastPeriod,
			EmaSlowPeriod: a.indicatorParams.EmaSlowPeriod,
			RsiPeriod:     a.indicatorParams.RsiPeriod,
			AtrPeriod:     a.indicatorParams.AtrPeriod,
		}
	}

	// 找出最大周期
	maxPeriod := params.EmaSlowPeriod
	if params.RsiPeriod > maxPeriod {
		maxPeriod = params.RsiPeriod
	}
	if params.AtrPeriod > maxPeriod {
		maxPeriod = params.AtrPeriod
	}

	// 需要 3 倍最大周期的数据（EMA 初始化 + 充足历史）
	required := maxPeriod * 3
	if required < minBufferSize {
		return minBufferSize
	}
	return required
}

// OnKline dispatches a closed 1m kline to the per-symbol worker goroutine.
func (a *KlineAggregator) OnKline(ctx context.Context, k *market.Kline) {
	if k.Interval != "1m" || !k.IsClosed {
		return
	}

	// Reject new klines after Stop() has been called
	select {
	case <-a.ctx.Done():
		return
	default:
	}

	a.metrics.Received1m.Add(1)

	w := a.getOrCreateWorker(k.Symbol)
	select {
	case w.ch <- k:
	default:
		log.Printf("[aggregator] WARN: %s channel full, dropping 1m kline openTime=%d", k.Symbol, k.OpenTime)
	}
}

// Stop gracefully shuts down all workers and emits incomplete buckets.
// After Stop, no more klines will be accepted via OnKline.
func (a *KlineAggregator) Stop() {
	// Signal all workers to stop via context cancellation
	a.cancel()

	// Wait for all workers to drain their channels and finish
	a.wg.Wait()

	// Emit any remaining incomplete buckets (including watermark-buffered)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, w := range a.workers {
		// Emit watermark-buffered klines first
		for w.watermarkHeap.Len() > 0 {
			bk := heap.Pop(&w.watermarkHeap).(*emitBucket)
			bk.bucket.dirty = true
			a.emitKline(context.Background(), w.symbol, bk.interval, bk.bucket)
		}

		// Emit incomplete aggregation buckets
		for _, iv := range a.intervals {
			if b, ok := w.buckets[iv.Name]; ok && b.initialized {
				a.emitKline(context.Background(), w.symbol, iv.Name, b)
			}
		}
	}

	// Close all log files
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
	m.Emitted3m.Store(a.metrics.Emitted3m.Load())
	m.Emitted15m.Store(a.metrics.Emitted15m.Load())
	m.Emitted1h.Store(a.metrics.Emitted1h.Load())
	m.Emitted4h.Store(a.metrics.Emitted4h.Load())
	m.GapsDetected.Store(a.metrics.GapsDetected.Load())
	m.KafkaSendErrors.Store(a.metrics.KafkaSendErrors.Load())
	m.WatermarkWaits.Store(a.metrics.WatermarkWaits.Load())
	m.WatermarkFlushes.Store(a.metrics.WatermarkFlushes.Load())
	return m
}

// --- per-symbol worker ---

type symbolWorker struct {
	symbol  string
	ch      chan *market.Kline
	buckets map[string]*bucket // interval name -> bucket (incremental aggregation)
	agg     *KlineAggregator

	// Watermark: 用小顶堆管理已完成但等待 watermark 的 bucket
	// 按 CloseTime 排序，保证最早到期的 bucket 在堆顶
	// 每次只需检查堆顶，O(log n) 入队/出队，保证顺序不漏 flush
	watermarkHeap watermarkBucketHeap

	// intervalHistories 为每个周期维护独立的收盘价/最高价/最低价历史
	// 用于按周期计算指标，避免"指标粒度错位"
	// 例如：15m K线用 15m 收盘价序列算 EMA，而非用 1m 数据
	intervalHistories map[string]*klineRingBuffer
}

// emitBucket wraps a completed bucket ready for watermark-delayed emit.
type emitBucket struct {
	bucket   *bucket
	interval string
	dirty    bool      // mark dirty if flushing on shutdown
	enqueued time.Time // 入队时间，用于 maxWatermarkBufferAge 超时强制 flush
}

// watermarkBucketHeap 实现小顶堆（按 bucket.CloseTime 升序排列）
// 保证最早到期的 bucket 在堆顶，每次只需检查堆顶即可
type watermarkBucketHeap []*emitBucket

func (h watermarkBucketHeap) Len() int { return len(h) }

func (h watermarkBucketHeap) Less(i, j int) bool {
	// 按 CloseTime 升序：最早结束的 bucket 在堆顶
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

	// 为每个周期创建独立的指标历史 ring buffer
	// 容量根据指标参数动态计算，确保足够计算所有指标
	// 公式：max(200, maxPeriod*3)，其中 maxPeriod = max(EmaSlow, Rsi, Atr)
	// 例如：EMA200 需要 buffer ≥ 600（200*3），而默认 EMA26 只需 200
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
	}
	a.workers[symbol] = w

	a.wg.Add(1)
	go w.run()

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
			w.processKline(k)
		case <-w.agg.ctx.Done():
			// Drain remaining items in channel before exiting
			for {
				select {
				case k, ok := <-w.ch:
					if !ok {
						return
					}
					w.processKline(k)
				default:
					return
				}
			}
		}
	}
}

func (w *symbolWorker) processKline(k *market.Kline) {
	// --- Watermark: check if any buffered buckets can now be emitted ---
	// 使用交易所时间(event time)而非系统时间(processing time)，保证：
	// 1. 服务器时钟快慢不影响 watermark 判断
	// 2. 网络抖动不会错误标记 dirty
	// 3. 回测与实盘行为一致
	//
	// 用 CloseTime（K线结束时间）而非 OpenTime（K线开始时间）：
	// 1m K线 OpenTime=10:00, CloseTime=10:01
	// watermark 表示"数据已完整到达的时刻"，应该用 CloseTime
	// 否则 watermark 会提前 1 分钟，导致提前 flush 或错误 dirty 标记
	watermark := k.CloseTime - w.agg.watermarkDelay.Milliseconds()

	// --- Flink lateness 模型：丢弃超过容忍窗口的迟到数据 ---
	// 迟到数据：OpenTime < watermark - allowedLateness
	// 例如：watermark=10:05, allowedLateness=2m → OpenTime < 10:03 的数据丢弃
	// 默认 allowedLateness=0，不丢弃任何数据
	if w.agg.allowedLateness > 0 && k.OpenTime < watermark-w.agg.allowedLateness.Milliseconds() {
		log.Printf("[aggregator] LATE DATA DROPPED: %s | openTime=%d watermark=%d lateness=%v | openTime < %d",
			k.Symbol, k.OpenTime, watermark, w.agg.allowedLateness, watermark-w.agg.allowedLateness.Milliseconds())
		return
	}

	// 同时检查 maxWatermarkBufferAge：超时的 bucket 强制 flush（防内存泄漏）
	// 使用 timeSource 而非 time.Now()，回测时可注入基于 K 线时间的时间源
	now := w.agg.timeSource.Now()

	// 使用小顶堆，按 CloseTime 排序，只检查堆顶
	// 最早到期的 bucket 在堆顶，如果堆顶不满足条件则后续也不满足
	for w.watermarkHeap.Len() > 0 {
		bk := w.watermarkHeap[0] // 堆顶（CloseTime 最小）

		// 正常 watermark 确认：bucket.CloseTime <= watermark
		if bk.bucket.CloseTime <= watermark {
			heap.Pop(&w.watermarkHeap)
			w.agg.emitKline(context.Background(), w.symbol, bk.interval, bk.bucket)
			continue
		}

		// 超时强制 flush：bucket 在 buffer 中超过 maxWatermarkBufferAge
		// 场景：symbol 停止推送数据 / 长时间无新K线 → buffer 不会清
		if w.agg.maxWatermarkBufferAge > 0 && now.Sub(bk.enqueued) > w.agg.maxWatermarkBufferAge {
			heap.Pop(&w.watermarkHeap)
			bk.bucket.dirty = true
			log.Printf("[aggregator] WATERMARK TIMEOUT: %s %s | enqueued=%s | age=%v | forcing dirty flush",
				w.symbol, bk.interval, bk.enqueued.Format("15:04:05.000"), now.Sub(bk.enqueued))
			w.agg.emitKline(context.Background(), w.symbol, bk.interval, bk.bucket)
			w.agg.metrics.WatermarkFlushes.Add(1)
			continue
		}

		// 堆顶不满足条件，后续也不会满足（堆按 CloseTime 升序）
		break
	}

	// Process each target interval with O(1) incremental aggregation
	for _, iv := range w.agg.intervals {
		requiredMinutes := int(iv.Duration / time.Minute)

		if requiredMinutes == 1 {
			continue
		}

		// Calculate current period based on the latest kline's openTime
		latestPeriodOpenTime := alignToInterval(k.OpenTime, iv.Duration)

		// Check if we already have a bucket for this period
		b, exists := w.buckets[iv.Name]

		// --- Time-driven: flush bucket when kline crosses period boundary ---
		if exists && b.initialized && latestPeriodOpenTime != b.OpenTime {
			// Previous period is complete (time-wise)
			if b.count < requiredMinutes {
				b.dirty = true
				log.Printf("[aggregator] INCOMPLETE: %s %s | period=%s | collected=%d/%d | missing=%d | dirty=true",
					w.symbol, iv.Name,
					time.UnixMilli(b.OpenTime).UTC().Format("2006-01-02T15:04:05"),
					b.count, requiredMinutes, requiredMinutes-b.count)
			}

			// 将完成的 bucket 的 OHLCV 推入该周期的历史 ring buffer
			// 这样指标计算使用的是**同周期K线的收盘序列**，而非1m数据
			history := w.intervalHistories[iv.Name]

			// ✅ 关键：先计算指标，再 push 到 history
			// 这样 IndicatorPrevious 模式下，指标真正不包含当前K线数据
			// 如果先 push 再计算，即使 closes[:len-1] 排除了最后一根，
			// ATR 的 prevClose 仍会包含当前K线，EMA 初始化也可能被污染
			w.calcBucketIndicators(b, iv.Name)

			// 计算完指标后再 push，确保当前K线不参与计算
			if history != nil {
				history.push(b.Open, b.High, b.Low, b.Close, b.OpenTime)
				// push 后更新递推指标状态，使状态与数据保持一致
				w.updateIndicatorState(history, b, iv.Name)
			}

			if w.agg.emitMode == EmitImmediate || b.dirty {
				// EmitImmediate 模式或 dirty 数据：立即发射
				// Immediate 模式下，bucket 完成后立即推送，不等待 watermark
				// 适合实盘交易信号（低延迟），但可能因迟到数据导致 dirty 重发
				w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
			} else if w.agg.watermarkDelay > 0 {
				// Watermark 模式：缓冲等待 watermark 确认，保证数据完整性
				heap.Push(&w.watermarkHeap, &emitBucket{
					bucket:   b,
					interval: iv.Name,
					enqueued: now,
				})
				w.agg.metrics.WatermarkWaits.Add(1)
			} else {
				// 无 watermark delay，直接发射
				w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
			}
			delete(w.buckets, iv.Name)
			b = nil
			exists = false
		}

		// --- Gap detection: kline jumps far ahead (more than one period) ---
		if exists && b.initialized {
			expectedNext := b.CloseTime + 1
			if k.OpenTime > expectedNext {
				w.agg.metrics.GapsDetected.Add(1)
				gapDuration := time.Duration(k.OpenTime-expectedNext) * time.Millisecond
				log.Printf("[aggregator] GAP: %s %s — expected openTime=%d got=%d gap=%v",
					w.symbol, iv.Name, expectedNext, k.OpenTime, gapDuration)
				if b.count > 0 {
					// 先计算指标（不包含当前K线），再推入历史
					w.calcBucketIndicators(b, iv.Name)
					history := w.intervalHistories[iv.Name]
					if history != nil {
						history.push(b.Open, b.High, b.Low, b.Close, b.OpenTime)
						w.updateIndicatorState(history, b, iv.Name)
					}
					b.dirty = true
					w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
				}
				delete(w.buckets, iv.Name)
				b = nil
				exists = false
			}
		}

		// --- Continuity check within current period: verify consecutive 1m klines ---
		// 允许 ≤1 分钟的抖动（Binance WebSocket 可能存在延迟、重发、乱序）
		// 只有 gap > 1 分钟（60000ms）才认为是真正的数据缺失
		if exists && b.initialized && b.count > 0 {
			expectedOpenTime := b.prevOpenTime + 60000
			gap := k.OpenTime - expectedOpenTime
			if gap > 60000 {
				// 真 gap：缺失超过 1 根 1m K线
				w.agg.metrics.GapsDetected.Add(1)
				log.Printf("[aggregator] CONTINUITY GAP: %s %s — expected openTime=%d got=%d gap=%dms | marking dirty",
					w.symbol, iv.Name, expectedOpenTime, k.OpenTime, gap)
				b.dirty = true
			} else if gap > 0 && gap <= 60000 {
				// 小抖动：可能是网络延迟或乱序，不标记 dirty
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
			// Incremental update: O(1) per kline per interval
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

// --- Indicator calculation (按周期计算，避免指标粒度错位) ---

// calcBucketIndicators 计算技术指标，使用**递推式**算法（O(1) 计算）。
// 核心原则：15m K线的 EMA/RSI/ATR 用 15m 收盘价序列计算，
// 而非用 1m 数据（否则指标含义错误）。
// 严禁使用 bucket 关闭之后的数据，避免未来函数（look-ahead bias）。
//
// ✅ 调用顺序保证：先调用本函数计算指标，再 push 当前 bucket 到 history。
// 因此 history 中只包含历史K线，不含当前 bucket：
// - IndicatorClosed：将当前K线数据临时用于计算（当前K线参与计算）
// - IndicatorPrevious：只用 history（当前K线不参与计算）
//
// ✅ 使用递推式指标（与交易所/TradingView 对齐）：
// - EMA: ema = prevEma + α * (price - prevEma), α = 2/(period+1)
// - RSI (Wilder): avgGain = (prevAvgGain*(p-1) + gain) / p
// - ATR (Wilder): atr = (prevAtr*(p-1) + TR) / p
//
// ✅ 递推状态管理：
//   - 递推状态（lastEma, avgGain, atr）在 push 时更新，保证与 history 数据一致
//   - IndicatorClosed 模式下，本函数会临时用当前K线做一次额外递推来计算结果，
//     但不修改 history 中的递推状态（避免状态超前于数据）
//   - push 时会再次递推更新状态，此时数据与状态完全一致
func (w *symbolWorker) calcBucketIndicators(b *bucket, intervalName string) {
	// 获取该周期的指标参数（优先用周期专属配置，回退到默认配置）
	params, ok := w.agg.intervalIndicators[intervalName]
	if !ok {
		params = IntervalIndicatorConfig{
			EmaFastPeriod: w.agg.indicatorParams.EmaFastPeriod,
			EmaSlowPeriod: w.agg.indicatorParams.EmaSlowPeriod,
			RsiPeriod:     w.agg.indicatorParams.RsiPeriod,
			AtrPeriod:     w.agg.indicatorParams.AtrPeriod,
		}
	}

	// 使用该周期的历史 ring buffer（此时不包含当前 bucket，因为先算再 push）
	history := w.intervalHistories[intervalName]
	if history == nil {
		return
	}

	// IndicatorClosed 模式：当前K线参与计算（临时递推一次，不修改状态）
	// IndicatorPrevious 模式：只用 history（当前K线不参与计算）
	includeCurrent := w.agg.indicatorMode == IndicatorClosed

	// --- EMA 递推计算 ---
	// ema = prevEma + α * (price - prevEma), α = 2/(period+1)
	if params.EmaFastPeriod > 0 {
		b.EmaFast = w.computeEMAValue(history, b.Close, params.EmaFastPeriod,
			&history.state.emaFast, &history.state.emaFastInit, includeCurrent)
	}
	if params.EmaSlowPeriod > 0 {
		b.EmaSlow = w.computeEMAValue(history, b.Close, params.EmaSlowPeriod,
			&history.state.emaSlow, &history.state.emaSlowInit, includeCurrent)
	}

	// --- RSI 递推计算（Wilder 平滑）---
	// Wilder RSI: avgGain = (prevAvgGain*(period-1) + currentGain) / period
	if params.RsiPeriod > 0 {
		b.Rsi = w.computeRSIValue(history, b.Close, params.RsiPeriod, includeCurrent)
	}

	// --- ATR 递推计算（Wilder 平滑）---
	// ATR = (prevATR*(period-1) + currentTR) / period
	if params.AtrPeriod > 0 {
		b.Atr = w.computeATRValue(history, b.High, b.Low, params.AtrPeriod, includeCurrent)
	}
}

// updateIndicatorState 在 push 后更新递推指标状态。
// 此时 history 已包含新K线数据，递推状态与数据保持一致。
func (w *symbolWorker) updateIndicatorState(history *klineRingBuffer, b *bucket, intervalName string) {
	params, ok := w.agg.intervalIndicators[intervalName]
	if !ok {
		params = IntervalIndicatorConfig{
			EmaFastPeriod: w.agg.indicatorParams.EmaFastPeriod,
			EmaSlowPeriod: w.agg.indicatorParams.EmaSlowPeriod,
			RsiPeriod:     w.agg.indicatorParams.RsiPeriod,
			AtrPeriod:     w.agg.indicatorParams.AtrPeriod,
		}
	}

	state := &history.state

	// EMA 递推：用当前K线的收盘价更新状态
	if params.EmaFastPeriod > 0 && history.count >= params.EmaFastPeriod {
		w.advanceEMAState(history, b.Close, params.EmaFastPeriod,
			&state.emaFast, &state.emaFastInit)
	}
	if params.EmaSlowPeriod > 0 && history.count >= params.EmaSlowPeriod {
		w.advanceEMAState(history, b.Close, params.EmaSlowPeriod,
			&state.emaSlow, &state.emaSlowInit)
	}

	// RSI 递推：用当前K线的收盘价变化更新状态
	if params.RsiPeriod > 0 && history.count >= params.RsiPeriod+1 {
		w.advanceRSIState(history, b.Close, params.RsiPeriod)
	}

	// ATR 递推：用当前K线的 TR 更新状态
	if params.AtrPeriod > 0 && history.count >= params.AtrPeriod {
		w.advanceATRState(history, b.High, b.Low, params.AtrPeriod)
	}
}

// --- 递推式指标计算（O(1)，与交易所/TradingView 对齐）---
// 设计原则：
// 1. computeXxxValue: 读取当前状态，计算指标值（不修改状态）
//    IndicatorClosed 模式下会临时递推一次来包含当前K线
// 2. advanceXxxState: 在 push 后更新递推状态（修改状态）
//    保证递推状态始终与 history 中的数据一致

// computeEMAValue 计算 EMA 值（不修改递推状态）。
// IndicatorClosed 模式下，临时用当前价格做一次额外递推。
func (w *symbolWorker) computeEMAValue(history *klineRingBuffer, currentPrice float64, period int,
	lastEma *float64, initialized *bool, includeCurrent bool) float64 {

	if period <= 0 {
		return 0
	}

	closes := history.closes()
	alpha := 2.0 / float64(period+1)

	// 数据不足以初始化
	if len(closes) < period {
		if includeCurrent && len(closes)+1 >= period {
			// 加上当前K线后刚好够初始化
			allCloses := make([]float64, len(closes), len(closes)+1)
			copy(allCloses, closes)
			allCloses = append(allCloses, currentPrice)
			sma := 0.0
			for i := 0; i < period; i++ {
				sma += allCloses[i]
			}
			sma /= float64(period)
			ema := sma
			for i := period; i < len(allCloses); i++ {
				ema = ema + alpha*(allCloses[i]-ema)
			}
			return ema
		}
		if len(closes) > 0 {
			return closes[len(closes)-1]
		}
		if includeCurrent {
			return currentPrice
		}
		return 0
	}

	// 未初始化：需要从头计算（仅首次或重启后）
	if !*initialized {
		sma := 0.0
		for i := 0; i < period; i++ {
			sma += closes[i]
		}
		sma /= float64(period)
		ema := sma
		for i := period; i < len(closes); i++ {
			ema = ema + alpha*(closes[i]-ema)
		}
		// 不更新 *lastEma 和 *initialized，状态更新由 advanceEMAState 在 push 时完成
		if includeCurrent {
			ema = ema + alpha*(currentPrice-ema)
		}
		return ema
	}

	// 已初始化：直接用缓存的 lastEma 递推
	ema := *lastEma
	if includeCurrent {
		ema = ema + alpha*(currentPrice-ema)
	}
	return ema
}

// advanceEMAState 在 push 后更新 EMA 递推状态。
func (w *symbolWorker) advanceEMAState(history *klineRingBuffer, currentPrice float64, period int,
	lastEma *float64, initialized *bool) {

	alpha := 2.0 / float64(period+1)
	closes := history.closes()

	if !*initialized {
		if len(closes) < period {
			return // 数据不足，等待更多数据
		}
		// 初始化：用 SMA + 递推
		sma := 0.0
		for i := 0; i < period; i++ {
			sma += closes[i]
		}
		sma /= float64(period)
		ema := sma
		for i := period; i < len(closes); i++ {
			ema = ema + alpha*(closes[i]-ema)
		}
		*lastEma = ema
		*initialized = true
		return
	}

	// 已初始化：递推一次
	*lastEma = *lastEma + alpha*(currentPrice-*lastEma)
}

// computeRSIValue 计算 RSI 值（不修改递推状态）。
// Wilder 平滑：avgGain = (prevAvgGain*(period-1) + currentGain) / period
func (w *symbolWorker) computeRSIValue(history *klineRingBuffer, currentPrice float64, period int, includeCurrent bool) float64 {
	if period <= 0 {
		return 50.0
	}

	closes := history.closes()
	state := &history.state

	// 数据不足以计算 RSI（需要至少 period+1 根K线才有 period 个价格变化）
	totalCloses := len(closes)
	if includeCurrent {
		totalCloses++
	}
	if totalCloses <= period {
		return 50.0
	}

	// 未初始化：需要从头计算
	if !state.rsiInit || state.rsiPeriod != period {
		// 构建完整收盘价序列
		allCloses := closes
		if includeCurrent {
			allCloses = make([]float64, len(closes), len(closes)+1)
			copy(allCloses, closes)
			allCloses = append(allCloses, currentPrice)
		}
		if len(allCloses) <= period {
			return 50.0
		}
		// 初始 avgGain/avgLoss（简单平均）
		gains, losses := 0.0, 0.0
		for i := len(allCloses) - period; i < len(allCloses); i++ {
			diff := allCloses[i] - allCloses[i-1]
			if diff > 0 {
				gains += diff
			} else {
				losses -= diff
			}
		}
		avgGain := gains / float64(period)
		avgLoss := losses / float64(period)
		if avgLoss == 0 {
			return 100.0
		}
		rs := avgGain / avgLoss
		return 100.0 - (100.0 / (1.0 + rs))
	}

	// 已初始化：用缓存的状态递推
	avgGain := state.avgGain
	avgLoss := state.avgLoss

	if includeCurrent && len(closes) > 0 {
		prevPrice := closes[len(closes)-1]
		diff := currentPrice - prevPrice
		gain, loss := 0.0, 0.0
		if diff > 0 {
			gain = diff
		} else {
			loss = -diff
		}
		avgGain = (avgGain*float64(period-1) + gain) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)
	}

	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

// advanceRSIState 在 push 后更新 RSI 递推状态。
func (w *symbolWorker) advanceRSIState(history *klineRingBuffer, currentPrice float64, period int) {
	closes := history.closes()
	state := &history.state

	if len(closes) < 2 {
		return
	}

	// 未初始化：用简单平均初始化
	if !state.rsiInit || state.rsiPeriod != period {
		if len(closes) <= period {
			return
		}
		gains, losses := 0.0, 0.0
		for i := len(closes) - period; i < len(closes); i++ {
			diff := closes[i] - closes[i-1]
			if diff > 0 {
				gains += diff
			} else {
				losses -= diff
			}
		}
		state.avgGain = gains / float64(period)
		state.avgLoss = losses / float64(period)
		state.rsiPeriod = period
		state.rsiInit = true
		return
	}

	// 已初始化：递推
	prevPrice := closes[len(closes)-2]
	diff := currentPrice - prevPrice
	gain, loss := 0.0, 0.0
	if diff > 0 {
		gain = diff
	} else {
		loss = -diff
	}
	state.avgGain = (state.avgGain*float64(period-1) + gain) / float64(period)
	state.avgLoss = (state.avgLoss*float64(period-1) + loss) / float64(period)
}

// computeATRValue 计算 ATR 值（不修改递推状态）。
// Wilder 平滑：ATR = (prevATR*(period-1) + currentTR) / period
func (w *symbolWorker) computeATRValue(history *klineRingBuffer, currentHigh, currentLow float64, period int, includeCurrent bool) float64 {
	if period <= 0 || history.count == 0 {
		return 0
	}

	state := &history.state
	tr := computeTrueRange(currentHigh, currentLow, history.lastClose())

	// 未初始化：需要从头计算
	if !state.atrInit || state.atrPeriod != period {
		highs := history.highs()
		lows := history.lows()
		prevCloses := history.prevCloses()

		n := len(highs)
		if n < period {
			return tr // 数据不足，返回当前 TR
		}
		// 简单平均
		totalTR := 0.0
		for i := n - period; i < n; i++ {
			totalTR += computeTrueRange(highs[i], lows[i], prevCloses[i])
		}
		atr := totalTR / float64(period)
		if includeCurrent {
			atr = (atr*float64(period-1) + tr) / float64(period)
		}
		return atr
	}

	// 已初始化：用缓存的状态递推
	atr := state.atr
	if includeCurrent {
		atr = (atr*float64(period-1) + tr) / float64(period)
	}
	return atr
}

// advanceATRState 在 push 后更新 ATR 递推状态。
func (w *symbolWorker) advanceATRState(history *klineRingBuffer, currentHigh, currentLow float64, period int) {
	state := &history.state
	tr := computeTrueRange(currentHigh, currentLow, history.prevCloseForLast())

	// 未初始化
	if !state.atrInit || state.atrPeriod != period {
		highs := history.highs()
		lows := history.lows()
		prevCloses := history.prevCloses()
		n := len(highs)
		if n < period {
			return
		}
		totalTR := 0.0
		for i := n - period; i < n; i++ {
			totalTR += computeTrueRange(highs[i], lows[i], prevCloses[i])
		}
		state.atr = totalTR / float64(period)
		state.atrPeriod = period
		state.atrInit = true
		return
	}

	// 已初始化：递推
	state.atr = (state.atr*float64(period-1) + tr) / float64(period)
}

// computeTrueRange 计算 True Range = max(H-L, |H-prevClose|, |L-prevClose|)
func computeTrueRange(high, low, prevClose float64) float64 {
	tr := high - low
	if prevClose > 0 {
		diff1 := high - prevClose
		if diff1 < 0 {
			diff1 = -diff1
		}
		diff2 := low - prevClose
		if diff2 < 0 {
			diff2 = -diff2
		}
		if diff1 > tr {
			tr = diff1
		}
		if diff2 > tr {
			tr = diff2
		}
	}
	return tr
}

// --- klineRingBuffer: ring buffer for kline history ---

// indicatorState 缓存递推指标的中间状态，避免每次从头重算
// 使用 Wilder 平滑（指数递推），与交易所/TradingView 指标对齐
type indicatorState struct {
	// EMA 递推状态：ema = prevEma + α * (price - prevEma), α = 2/(period+1)
	emaFast     float64 // 上一次 EMA 快线值
	emaSlow     float64 // 上一次 EMA 慢线值
	emaFastInit bool    // EMA 快线是否已初始化（至少有 period 根K线）
	emaSlowInit bool    // EMA 慢线是否已初始化

	// RSI 递推状态（Wilder 平滑）：
	// avgGain = (prevAvgGain*(period-1) + currentGain) / period
	// avgLoss = (prevAvgLoss*(period-1) + currentLoss) / period
	avgGain   float64 // 上一次平均涨幅
	avgLoss   float64 // 上一次平均跌幅
	rsiInit   bool    // RSI 是否已初始化（至少有 period 根K线完成首轮计算）
	rsiPeriod int     // RSI 周期（用于递推）

	// ATR 递推状态（Wilder 平滑）：
	// atr = (prevAtr*(period-1) + currentTR) / period
	atr       float64 // 上一次 ATR 值
	atrInit   bool    // ATR 是否已初始化
	atrPeriod int     // ATR 周期（用于递推）
}

type klineRingBuffer struct {
	closePrices   []float64
	highPrices    []float64
	lowPrices     []float64
	prevCloseVals []float64 // close of previous kline (for ATR)
	openTimes     []int64
	size          int
	cap           int
	head          int // next write position
	count         int

	// state 缓存递推指标的中间状态，O(1) 计算而非 O(n) 重算
	state indicatorState
}

func newKlineRingBuffer(cap int) *klineRingBuffer {
	return &klineRingBuffer{
		closePrices:   make([]float64, cap),
		highPrices:    make([]float64, cap),
		lowPrices:     make([]float64, cap),
		prevCloseVals: make([]float64, cap),
		openTimes:     make([]int64, cap),
		cap:           cap,
	}
}

func (r *klineRingBuffer) push(open, high, low, close float64, openTime int64) {
	// prevClose is the close of the element just before the new one
	var prevClose float64
	if r.count > 0 {
		prevIdx := (r.head - 1 + r.cap) % r.cap
		prevClose = r.closePrices[prevIdx]
	}

	r.closePrices[r.head] = close
	r.highPrices[r.head] = high
	r.lowPrices[r.head] = low
	r.prevCloseVals[r.head] = prevClose
	r.openTimes[r.head] = openTime
	r.head = (r.head + 1) % r.cap
	if r.count < r.cap {
		r.count++
	}
}

// lastClose 返回最后一根K线的收盘价，用于 ATR 的 prevClose 计算
func (r *klineRingBuffer) lastClose() float64 {
	if r.count == 0 {
		return 0
	}
	idx := (r.head - 1 + r.cap) % r.cap
	return r.closePrices[idx]
}

// prevCloseForLast 返回最后一根K线的 prevClose 值（即倒数第二根K线的收盘价）
// 用于 advanceATRState 中计算 TR（push 后，新K线已成为 history 最后一根）
func (r *klineRingBuffer) prevCloseForLast() float64 {
	if r.count == 0 {
		return 0
	}
	idx := (r.head - 1 + r.cap) % r.cap
	return r.prevCloseVals[idx]
}

// closes returns all close prices in chronological order.
func (r *klineRingBuffer) closes() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.closePrices[idx]
	}
	return result
}

// highs returns all high prices in chronological order.
func (r *klineRingBuffer) highs() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.highPrices[idx]
	}
	return result
}

// lows returns all low prices in chronological order.
func (r *klineRingBuffer) lows() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.lowPrices[idx]
	}
	return result
}

// prevCloses returns all prev close prices in chronological order.
func (r *klineRingBuffer) prevCloses() []float64 {
	result := make([]float64, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.prevCloseVals[idx]
	}
	return result
}

// countBefore returns the number of elements with openTime <= cutoff.
func (r *klineRingBuffer) countBefore(cutoff int64) int {
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		if r.openTimes[idx] > cutoff {
			return i
		}
	}
	return r.count
}

// closesBefore returns close prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) closesBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.closePrices[idx]
	}
	return result
}

// highsBefore returns high prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) highsBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.highPrices[idx]
	}
	return result
}

// lowsBefore returns low prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) lowsBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.lowPrices[idx]
	}
	return result
}

// prevClosesBefore returns prev close prices with openTime <= cutoff, in chronological order.
func (r *klineRingBuffer) prevClosesBefore(cutoff int64) []float64 {
	n := r.countBefore(cutoff)
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := (r.head - r.count + i + r.cap) % r.cap
		result[i] = r.prevCloseVals[idx]
	}
	return result
}

// klineLogEntry mirrors the protobuf field order for deterministic JSON output.
type klineLogEntry struct {
	Symbol         string  `json:"symbol"`
	Interval       string  `json:"interval"`
	OpenTime       string  `json:"openTime"`
	Open           float64 `json:"open"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	Close          float64 `json:"close"`
	Volume         float64 `json:"volume"`
	CloseTime      string  `json:"closeTime"`
	IsClosed       bool    `json:"isClosed"`
	EventTime      string  `json:"eventTime"`
	FirstTradeId   int64   `json:"firstTradeId"`
	LastTradeId    int64   `json:"lastTradeId"`
	NumTrades      int32   `json:"numTrades"`
	QuoteVolume    float64 `json:"quoteVolume"`
	TakerBuyVolume float64 `json:"takerBuyVolume"`
	TakerBuyQuote  float64 `json:"takerBuyQuote"`
	IsDirty        bool    `json:"isDirty"`
	IsTradable     bool    `json:"isTradable"`
	EmaFast        float64 `json:"emaFast"`
	EmaSlow        float64 `json:"emaSlow"`
	Rsi            float64 `json:"rsi"`
	Atr            float64 `json:"atr"`
}

type bucket struct {
	Open           float64
	High           float64
	Low            float64
	Close          float64
	Volume         float64
	QuoteVolume    float64
	TakerBuyVolume float64
	TakerBuyQuote  float64
	NumTrades      int32
	FirstTradeID   int64
	LastTradeID    int64
	OpenTime       int64
	CloseTime      int64
	prevOpenTime   int64 // openTime of the last 1m kline added (for continuity check)
	count          int   // number of 1m klines aggregated so far
	dirty          bool  // true if bucket has gaps or incomplete data
	initialized    bool

	// Technical indicators (calculated on emit)
	EmaFast float64
	EmaSlow float64
	Rsi     float64
	Atr     float64
}

// --- emit (lock-free, called from per-symbol goroutine) ---

func (a *KlineAggregator) emitKline(ctx context.Context, symbol, interval string, b *bucket) {
	k := &market.Kline{
		Symbol:         symbol,
		Interval:       interval,
		OpenTime:       b.OpenTime,
		CloseTime:      b.CloseTime,
		Open:           b.Open,
		High:           b.High,
		Low:            b.Low,
		Close:          b.Close,
		Volume:         b.Volume,
		QuoteVolume:    b.QuoteVolume,
		TakerBuyVolume: b.TakerBuyVolume,
		TakerBuyQuote:  b.TakerBuyQuote,
		NumTrades:      b.NumTrades,
		FirstTradeId:   b.FirstTradeID,
		LastTradeId:    b.LastTradeID,
		IsClosed:       true,
		IsDirty:        b.dirty,
		IsTradable:     !b.dirty,
		EventTime:      a.timeSource.Now().UnixMilli(),
		EmaFast:        b.EmaFast,
		EmaSlow:        b.EmaSlow,
		Rsi:            b.Rsi,
		Atr:            b.Atr,
	}

	indicatorStr := ""
	if k.EmaFast != 0 || k.EmaSlow != 0 || k.Rsi != 0 {
		indicatorStr = fmt.Sprintf(" EMA_f=%.2f EMA_s=%.2f RSI=%.2f ATR=%.2f", k.EmaFast, k.EmaSlow, k.Rsi, k.Atr)
	}

	openStr := time.UnixMilli(k.OpenTime).Format("2006-01-02 15:04:05")
	closeStr := time.UnixMilli(k.CloseTime).Format("15:04:05")
	log.Printf("[aggregated %s] %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f QV=%.4f trades=%d%s",
		k.Interval, k.Symbol, openStr, closeStr, k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume, k.NumTrades, indicatorStr)

	// Persist to jsonl file for verification
	a.writeKlineLog(k)

	if err := a.producer.SendMarketData(ctx, k); err != nil {
		a.metrics.KafkaSendErrors.Add(1)
		log.Printf("[aggregator] failed to send %s kline to Kafka: %v", k.Interval, err)
	}

	// Track metrics
	switch interval {
	case "3m":
		a.metrics.Emitted3m.Add(1)
	case "15m":
		a.metrics.Emitted15m.Add(1)
	case "1h":
		a.metrics.Emitted1h.Add(1)
	case "4h":
		a.metrics.Emitted4h.Add(1)
	}
}

// formatFloat formats a float64 to string with 2 decimal places.
func formatFloat(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

// writeKlineLog appends an aggregated kline as JSON line to a daily log file.
// Format: data/kline/ETHUSDT/3m/2026-04-11.jsonl
func (a *KlineAggregator) writeKlineLog(k *market.Kline) {
	if a.klineLogDir == "" {
		return
	}

	// Round float fields to 2 decimal places for cleaner logs
	k.Volume = formatFloat(k.Volume)
	k.QuoteVolume = formatFloat(k.QuoteVolume)
	k.TakerBuyVolume = formatFloat(k.TakerBuyVolume)
	k.TakerBuyQuote = formatFloat(k.TakerBuyQuote)
	k.Open = formatFloat(k.Open)
	k.High = formatFloat(k.High)
	k.Low = formatFloat(k.Low)
	k.Close = formatFloat(k.Close)
	k.EmaFast = formatFloat(k.EmaFast)
	k.EmaSlow = formatFloat(k.EmaSlow)
	k.Rsi = formatFloat(k.Rsi)
	k.Atr = formatFloat(k.Atr)

	dateStr := time.UnixMilli(k.CloseTime).Format("2006-01-02")
	dir := filepath.Join(a.klineLogDir, k.Symbol, k.Interval)

	a.logMu.Lock()
	defer a.logMu.Unlock()

	key := k.Symbol + "/" + k.Interval + "/" + dateStr
	f, ok := a.logFile[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[agg-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[agg-log] failed to open %s: %v", path, err)
			return
		}
		a.logFile[key] = f
	}

	entry := klineLogEntry{
		Symbol:         k.Symbol,
		Interval:       k.Interval,
		OpenTime:       time.UnixMilli(k.OpenTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		Open:           k.Open,
		High:           k.High,
		Low:            k.Low,
		Close:          k.Close,
		Volume:         k.Volume,
		CloseTime:      time.UnixMilli(k.CloseTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		IsClosed:       k.IsClosed,
		EventTime:      time.UnixMilli(k.EventTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		FirstTradeId:   k.FirstTradeId,
		LastTradeId:    k.LastTradeId,
		NumTrades:      k.NumTrades,
		QuoteVolume:    k.QuoteVolume,
		TakerBuyVolume: k.TakerBuyVolume,
		TakerBuyQuote:  k.TakerBuyQuote,
		IsDirty:        k.IsDirty,
		IsTradable:     k.IsTradable,
		EmaFast:        k.EmaFast,
		EmaSlow:        k.EmaSlow,
		Rsi:            k.Rsi,
		Atr:            k.Atr,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[agg-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("[agg-log] write failed: %v", err)
	}
}

// alignToInterval truncates a millisecond timestamp to the start of the
// containing interval period.
func alignToInterval(tsMs int64, d time.Duration) int64 {
	dMs := d.Milliseconds()
	return tsMs - (tsMs % dMs)
}
