package orderlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"exchange-system/app/execution/rpc/internal/exchange"
	strategypb "exchange-system/common/pb/strategy"
)

// ---------------------------------------------------------------------------
// 订单日志记录器
//
// 记录两种日志到 data/ 目录：
//   - 信号日志：data/signal/{SYMBOL}/2026-04-12.jsonl（收到的策略信号）
//   - 订单日志：data/order/{SYMBOL}/2026-04-12.jsonl（下单/成交结果）
//
// 格式：JSONL（每行一条 JSON），方便后续分析/回测
// ---------------------------------------------------------------------------

// Logger 订单日志记录器
type Logger struct {
	signalLogDir string // 信号日志目录
	orderLogDir  string // 订单日志目录

	mu          sync.Mutex
	signalFiles map[string]*os.File // key: "SYMBOL/2026-04-12"
	orderFiles  map[string]*os.File // key: "SYMBOL/2026-04-12"
}

type HarvestPathMeta struct {
	Probability      float64
	RuleProbability  float64
	LSTMProbability  float64
	BookProbability  float64
	BookSummary      string
	VolatilityRegime string
	ThresholdSource  string
	AppliedThreshold float64
	PathAction       string
	RiskLevel        string
	TargetSide       string
	ReferencePrice   float64
	MarketPrice      float64
}

// NewLogger 创建日志记录器
// signalLogDir/orderLogDir 为空则不记录对应日志
func NewLogger(signalLogDir, orderLogDir string) *Logger {
	return &Logger{
		signalLogDir: signalLogDir,
		orderLogDir:  orderLogDir,
		signalFiles:  make(map[string]*os.File),
		orderFiles:   make(map[string]*os.File),
	}
}

// ---------------------------------------------------------------------------
// 信号日志
// ---------------------------------------------------------------------------

// SignalLogEntry 信号日志条目
type SignalLogEntry struct {
	Timestamp    string             `json:"timestamp"`
	StrategyID   string             `json:"strategy_id"`
	Symbol       string             `json:"symbol"`
	SignalType   string             `json:"signal_type"`
	Action       string             `json:"action"`
	Side         string             `json:"side"`
	Interval     string             `json:"interval"`
	Quantity     float64            `json:"quantity"`
	EntryPrice   float64            `json:"entry_price"`
	StopLoss     float64            `json:"stop_loss"`
	TakeProfits  []float64          `json:"take_profits"`
	Atr          float64            `json:"atr"`
	RiskReward   float64            `json:"risk_reward"`
	Reason       string             `json:"reason"`
	SignalReason *SignalReasonEntry `json:"signal_reason,omitempty"`
	Indicators   map[string]float64 `json:"indicators,omitempty"`
}

type SignalReasonEntry struct {
	Summary          string                `json:"summary,omitempty"`
	Phase            string                `json:"phase,omitempty"`
	TrendContext     string                `json:"trend_context,omitempty"`
	SetupContext     string                `json:"setup_context,omitempty"`
	PathContext      string                `json:"path_context,omitempty"`
	ExecutionContext string                `json:"execution_context,omitempty"`
	ExitReasonKind   string                `json:"exit_reason_kind,omitempty"`
	ExitReasonLabel  string                `json:"exit_reason_label,omitempty"`
	Tags             []string              `json:"tags,omitempty"`
	RouteBucket      string                `json:"route_bucket,omitempty"`
	RouteReason      string                `json:"route_reason,omitempty"`
	RouteTemplate    string                `json:"route_template,omitempty"`
	Allocator        *AllocatorStatusEntry `json:"allocator,omitempty"`
	Range            *RangeSignalEntry     `json:"range,omitempty"`
}

type RangeSignalEntry struct {
	H1RangeOK      bool `json:"h1_range_ok"`
	H1AdxOK        bool `json:"h1_adx_ok"`
	H1BollWidthOK  bool `json:"h1_boll_width_ok"`
	M15TouchLower  bool `json:"m15_touch_lower"`
	M15RsiTurnUp   bool `json:"m15_rsi_turn_up"`
	M15TouchUpper  bool `json:"m15_touch_upper"`
	M15RsiTurnDown bool `json:"m15_rsi_turn_down"`
}

type AllocatorStatusEntry struct {
	Template       string  `json:"template,omitempty"`
	RouteBucket    string  `json:"route_bucket,omitempty"`
	RouteReason    string  `json:"route_reason,omitempty"`
	Score          float64 `json:"score,omitempty"`
	ScoreSource    string  `json:"score_source,omitempty"`
	BucketBudget   float64 `json:"bucket_budget,omitempty"`
	StrategyWeight float64 `json:"strategy_weight"`
	SymbolWeight   float64 `json:"symbol_weight"`
	RiskScale      float64 `json:"risk_scale"`
	PositionBudget float64 `json:"position_budget"`
	TradingPaused  bool    `json:"trading_paused"`
	PauseReason    string  `json:"pause_reason,omitempty"`
}

// ProtectionEntry 记录整组保护单的下发结果，方便事后排查官网未显示的原因。
type ProtectionEntry struct {
	Requested  bool                `json:"requested"`
	Status     string              `json:"status,omitempty"`
	Reason     string              `json:"reason,omitempty"`
	StopLoss   *ProtectionLegEntry `json:"stop_loss,omitempty"`
	TakeProfit *ProtectionLegEntry `json:"take_profit,omitempty"`
}

// ProtectionLegEntry 记录单条保护腿的下发结果。
type ProtectionLegEntry struct {
	Requested     bool    `json:"requested"`
	Status        string  `json:"status,omitempty"`
	TriggerPrice  float64 `json:"trigger_price,omitempty"`
	Reason        string  `json:"reason,omitempty"`
	OrderID       string  `json:"order_id,omitempty"`
	ClientOrderID string  `json:"client_order_id,omitempty"`
}

// LogSignal 记录收到的策略信号
func (l *Logger) LogSignal(sig *strategypb.Signal) {
	if l == nil || l.signalLogDir == "" {
		return
	}

	entry := SignalLogEntry{
		Timestamp:    time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		StrategyID:   sig.GetStrategyId(),
		Symbol:       sig.GetSymbol(),
		SignalType:   sig.GetSignalType(),
		Action:       sig.GetAction(),
		Side:         sig.GetSide(),
		Interval:     sig.GetInterval(),
		Quantity:     sig.GetQuantity(),
		EntryPrice:   sig.GetEntryPrice(),
		StopLoss:     sig.GetStopLoss(),
		TakeProfits:  sig.GetTakeProfits(),
		Atr:          sig.GetAtr(),
		RiskReward:   sig.GetRiskReward(),
		Reason:       sig.GetReason(),
		SignalReason: signalReasonEntryFromPB(sig.GetSignalReason()),
		Indicators:   sig.GetIndicators(),
	}

	l.writeJSONL(l.signalLogDir, l.signalFiles, sig.GetSymbol(), &entry)
}

// ---------------------------------------------------------------------------
// 订单日志
// ---------------------------------------------------------------------------

// OrderLogEntry 订单日志条目
type OrderLogEntry struct {
	Timestamp                   string             `json:"timestamp"`
	SignalType                  string             `json:"signal_type"`
	StrategyID                  string             `json:"strategy_id"`
	Symbol                      string             `json:"symbol"`
	OrderID                     string             `json:"order_id"`
	ClientID                    string             `json:"client_id"`
	Side                        string             `json:"side"`
	PositionSide                string             `json:"position_side"`
	Type                        string             `json:"type"`
	Status                      string             `json:"status"`
	Quantity                    float64            `json:"quantity"`
	OrderQuantity               float64            `json:"order_quantity"`
	ExecutedQty                 float64            `json:"executed_qty"`
	AvgPrice                    float64            `json:"avg_price"`
	Commission                  float64            `json:"commission"`
	CommissionAsset             string             `json:"commission_asset"`
	Slippage                    float64            `json:"slippage"`
	StopLoss                    float64            `json:"stop_loss"`
	Atr                         float64            `json:"atr"`
	RiskReward                  float64            `json:"risk_reward"`
	Reason                      string             `json:"reason"`
	SignalReason                *SignalReasonEntry `json:"signal_reason,omitempty"`
	Protection                  *ProtectionEntry   `json:"protection,omitempty"`
	ErrorMessage                string             `json:"error_message,omitempty"`
	TransactTime                string             `json:"transact_time"`
	HarvestPathProbability      float64            `json:"harvest_path_probability,omitempty"`
	HarvestPathRuleProbability  float64            `json:"harvest_path_rule_probability,omitempty"`
	HarvestPathLSTMProbability  float64            `json:"harvest_path_lstm_probability,omitempty"`
	HarvestPathBookProbability  float64            `json:"harvest_path_book_probability,omitempty"`
	HarvestPathBookSummary      string             `json:"harvest_path_book_summary,omitempty"`
	HarvestPathVolatilityRegime string             `json:"harvest_path_volatility_regime,omitempty"`
	HarvestPathThresholdSource  string             `json:"harvest_path_threshold_source,omitempty"`
	HarvestPathAppliedThreshold float64            `json:"harvest_path_applied_threshold,omitempty"`
	HarvestPathAction           string             `json:"harvest_path_action,omitempty"`
	HarvestPathRiskLevel        string             `json:"harvest_path_risk_level,omitempty"`
	HarvestPathTargetSide       string             `json:"harvest_path_target_side,omitempty"`
	HarvestPathReferencePrice   float64            `json:"harvest_path_reference_price,omitempty"`
	HarvestPathMarketPrice      float64            `json:"harvest_path_market_price,omitempty"`
}

// LogOrder 记录订单执行结果，并把保护单下发状态一起落盘。
func (l *Logger) LogOrder(sig *strategypb.Signal, result *exchange.OrderResult, orderQuantity float64, harvestPath *HarvestPathMeta, protection *exchange.ProtectionSetupResult) {
	if l == nil || l.orderLogDir == "" {
		return
	}

	entry := OrderLogEntry{
		Timestamp:       time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		SignalType:      sig.GetSignalType(),
		StrategyID:      sig.GetStrategyId(),
		Symbol:          result.Symbol,
		OrderID:         result.OrderID,
		ClientID:        result.ClientOrderID,
		Side:            string(result.Side),
		PositionSide:    string(result.PositionSide),
		Type:            string(exchange.OrderTypeMarket),
		Status:          string(result.Status),
		Quantity:        sig.GetQuantity(),
		OrderQuantity:   orderQuantity,
		ExecutedQty:     result.ExecutedQuantity,
		AvgPrice:        result.AvgPrice,
		Commission:      result.Commission,
		CommissionAsset: result.CommissionAsset,
		Slippage:        result.Slippage,
		StopLoss:        sig.GetStopLoss(),
		Atr:             sig.GetAtr(),
		RiskReward:      sig.GetRiskReward(),
		Reason:          ComposeHarvestPathReason(sig.GetReason(), harvestPath),
		SignalReason:    signalReasonEntryFromPB(sig.GetSignalReason()),
		Protection:      protectionEntryFromResult(protection),
		ErrorMessage:    result.ErrorMessage,
		TransactTime:    formatMillisTime(result.TransactTime),
	}
	applyHarvestPathMeta(&entry, harvestPath)

	l.writeJSONL(l.orderLogDir, l.orderFiles, result.Symbol, &entry)
}

// LogOrderFailure 记录下单失败结果，便于排查风控拒绝或交易所报错。
func (l *Logger) LogOrderFailure(sig *strategypb.Signal, status exchange.OrderStatus, clientID, errorMessage string, orderQuantity float64, harvestPath *HarvestPathMeta) {
	if l == nil || l.orderLogDir == "" || sig == nil {
		return
	}

	entry := OrderLogEntry{
		Timestamp:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		SignalType:    sig.GetSignalType(),
		StrategyID:    sig.GetStrategyId(),
		Symbol:        sig.GetSymbol(),
		OrderID:       clientID,
		ClientID:      clientID,
		Side:          sig.GetAction(),
		PositionSide:  sig.GetSide(),
		Type:          string(exchange.OrderTypeMarket),
		Status:        string(status),
		Quantity:      sig.GetQuantity(),
		OrderQuantity: orderQuantity,
		StopLoss:      sig.GetStopLoss(),
		Atr:           sig.GetAtr(),
		RiskReward:    sig.GetRiskReward(),
		Reason:        ComposeHarvestPathReason(errorMessage, harvestPath),
		SignalReason:  signalReasonEntryFromPB(sig.GetSignalReason()),
		ErrorMessage:  errorMessage,
		TransactTime:  formatMillisTime(time.Now().UnixMilli()),
	}
	applyHarvestPathMeta(&entry, harvestPath)

	l.writeJSONL(l.orderLogDir, l.orderFiles, sig.GetSymbol(), &entry)
}

func applyHarvestPathMeta(entry *OrderLogEntry, harvestPath *HarvestPathMeta) {
	if entry == nil || harvestPath == nil {
		return
	}
	entry.HarvestPathProbability = harvestPath.Probability
	entry.HarvestPathRuleProbability = harvestPath.RuleProbability
	entry.HarvestPathLSTMProbability = harvestPath.LSTMProbability
	entry.HarvestPathBookProbability = harvestPath.BookProbability
	entry.HarvestPathBookSummary = harvestPath.BookSummary
	entry.HarvestPathVolatilityRegime = harvestPath.VolatilityRegime
	entry.HarvestPathThresholdSource = harvestPath.ThresholdSource
	entry.HarvestPathAppliedThreshold = harvestPath.AppliedThreshold
	entry.HarvestPathAction = harvestPath.PathAction
	entry.HarvestPathRiskLevel = harvestPath.RiskLevel
	entry.HarvestPathTargetSide = harvestPath.TargetSide
	entry.HarvestPathReferencePrice = harvestPath.ReferencePrice
	entry.HarvestPathMarketPrice = harvestPath.MarketPrice
}

func ComposeHarvestPathReason(baseReason string, harvestPath *HarvestPathMeta) string {
	baseReason = strings.TrimSpace(baseReason)
	if harvestPath == nil {
		return baseReason
	}

	parts := make([]string, 0, 5)
	if harvestPath.PathAction != "" {
		parts = append(parts, "path_action="+harvestPath.PathAction)
	}
	if harvestPath.RiskLevel != "" {
		parts = append(parts, "risk="+harvestPath.RiskLevel)
	}
	if harvestPath.TargetSide != "" {
		parts = append(parts, "target="+harvestPath.TargetSide)
	}
	if harvestPath.ReferencePrice > 0 {
		parts = append(parts, "ref="+strconv.FormatFloat(harvestPath.ReferencePrice, 'f', 2, 64))
	}
	if harvestPath.MarketPrice > 0 {
		parts = append(parts, "market="+strconv.FormatFloat(harvestPath.MarketPrice, 'f', 2, 64))
	}
	if len(parts) == 0 {
		return baseReason
	}

	summary := "[harvest-path] " + strings.Join(parts, " | ")
	if baseReason == "" {
		return summary
	}
	return summary + " | " + baseReason
}

func signalReasonEntryFromPB(reason *strategypb.SignalReason) *SignalReasonEntry {
	if reason == nil {
		return nil
	}
	entry := &SignalReasonEntry{
		Summary:          strings.TrimSpace(reason.GetSummary()),
		Phase:            strings.TrimSpace(reason.GetPhase()),
		TrendContext:     strings.TrimSpace(reason.GetTrendContext()),
		SetupContext:     strings.TrimSpace(reason.GetSetupContext()),
		PathContext:      strings.TrimSpace(reason.GetPathContext()),
		ExecutionContext: strings.TrimSpace(reason.GetExecutionContext()),
		ExitReasonKind:   strings.TrimSpace(reason.GetExitReasonKind()),
		ExitReasonLabel:  strings.TrimSpace(reason.GetExitReasonLabel()),
		RouteBucket:      strings.TrimSpace(reason.GetRouteBucket()),
		RouteReason:      strings.TrimSpace(reason.GetRouteReason()),
		RouteTemplate:    strings.TrimSpace(reason.GetRouteTemplate()),
		Allocator:        allocatorStatusEntryFromPB(reason.GetAllocator()),
		Range:            rangeSignalEntryFromPB(reason.GetRange()),
	}
	if tags := reason.GetTags(); len(tags) > 0 {
		entry.Tags = append([]string(nil), tags...)
	}
	if entry.Summary == "" && entry.Phase == "" && entry.TrendContext == "" && entry.SetupContext == "" && entry.PathContext == "" && entry.ExecutionContext == "" && entry.ExitReasonKind == "" && entry.ExitReasonLabel == "" && entry.RouteBucket == "" && entry.RouteReason == "" && entry.RouteTemplate == "" && len(entry.Tags) == 0 && entry.Allocator == nil && entry.Range == nil {
		return nil
	}
	return entry
}

// rangeSignalEntryFromPB 将结构化 range 摘要转换为日志结构。
func rangeSignalEntryFromPB(v *strategypb.RangeSignalReason) *RangeSignalEntry {
	if v == nil {
		return nil
	}
	entry := &RangeSignalEntry{
		H1RangeOK:      v.GetH1RangeOk(),
		H1AdxOK:        v.GetH1AdxOk(),
		H1BollWidthOK:  v.GetH1BollWidthOk(),
		M15TouchLower:  v.GetM15TouchLower(),
		M15RsiTurnUp:   v.GetM15RsiTurnUp(),
		M15TouchUpper:  v.GetM15TouchUpper(),
		M15RsiTurnDown: v.GetM15RsiTurnDown(),
	}
	if !entry.H1RangeOK && !entry.H1AdxOK && !entry.H1BollWidthOK && !entry.M15TouchLower && !entry.M15RsiTurnUp && !entry.M15TouchUpper && !entry.M15RsiTurnDown {
		return nil
	}
	return entry
}

// allocatorStatusEntryFromPB 将 allocator 快照转换为日志友好的结构。
func allocatorStatusEntryFromPB(v *strategypb.PositionAllocatorStatus) *AllocatorStatusEntry {
	if v == nil {
		return nil
	}
	entry := &AllocatorStatusEntry{
		Template:       strings.TrimSpace(v.GetTemplate()),
		RouteBucket:    strings.TrimSpace(v.GetRouteBucket()),
		RouteReason:    strings.TrimSpace(v.GetRouteReason()),
		Score:          v.GetScore(),
		ScoreSource:    strings.TrimSpace(v.GetScoreSource()),
		BucketBudget:   v.GetBucketBudget(),
		StrategyWeight: v.GetStrategyWeight(),
		SymbolWeight:   v.GetSymbolWeight(),
		RiskScale:      v.GetRiskScale(),
		PositionBudget: v.GetPositionBudget(),
		TradingPaused:  v.GetTradingPaused(),
		PauseReason:    strings.TrimSpace(v.GetPauseReason()),
	}
	if entry.Template == "" && entry.RouteBucket == "" && entry.RouteReason == "" && entry.Score == 0 && entry.ScoreSource == "" && entry.BucketBudget == 0 && entry.StrategyWeight == 0 && entry.SymbolWeight == 0 && entry.RiskScale == 0 && entry.PositionBudget == 0 && !entry.TradingPaused && entry.PauseReason == "" {
		return nil
	}
	return entry
}

// protectionEntryFromResult 将交易所返回的结构化保护单结果转换为日志结构。
func protectionEntryFromResult(v *exchange.ProtectionSetupResult) *ProtectionEntry {
	if v == nil {
		return nil
	}
	entry := &ProtectionEntry{
		Requested:  v.Requested,
		Status:     strings.TrimSpace(v.Status),
		Reason:     strings.TrimSpace(v.Reason),
		StopLoss:   protectionLegEntryFromResult(v.StopLoss),
		TakeProfit: protectionLegEntryFromResult(v.TakeProfit),
	}
	if !entry.Requested && entry.Status == "" && entry.Reason == "" && entry.StopLoss == nil && entry.TakeProfit == nil {
		return nil
	}
	return entry
}

// protectionLegEntryFromResult 将单条保护腿结果转换为日志结构。
func protectionLegEntryFromResult(v *exchange.ProtectionLegResult) *ProtectionLegEntry {
	if v == nil {
		return nil
	}
	entry := &ProtectionLegEntry{
		Requested:     v.Requested,
		Status:        strings.TrimSpace(v.Status),
		TriggerPrice:  v.TriggerPrice,
		Reason:        strings.TrimSpace(v.Reason),
		OrderID:       strings.TrimSpace(v.OrderID),
		ClientOrderID: strings.TrimSpace(v.ClientOrderID),
	}
	if !entry.Requested && entry.Status == "" && entry.TriggerPrice == 0 && entry.Reason == "" && entry.OrderID == "" && entry.ClientOrderID == "" {
		return nil
	}
	return entry
}

// ---------------------------------------------------------------------------
// 通用写入
// ---------------------------------------------------------------------------

// writeJSONL 写入 JSONL 日志（线程安全）
// 目录结构：{logDir}/{symbol}/2026-04-12.jsonl
// 数值保留2位小数，禁用 HTML 转义（< > & 不转义为 \u003c 等）
func (l *Logger) writeJSONL(logDir string, files map[string]*os.File, symbol string, entry interface{}) {
	if logDir == "" || symbol == "" {
		return
	}

	// 数值格式化为2位小数
	formatNumbers(entry)

	// 使用 json.Encoder 禁用 HTML 转义，避免 < > & 被编码为 \u003c 等
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(entry); err != nil {
		log.Printf("[order-log] JSON序列化失败: %v", err)
		return
	}
	data := buf.Bytes()

	now := time.Now().UTC()
	dateStr := now.Format("2006-01-02")
	safeSymbol := sanitizePathComponent(symbol)
	key := fmt.Sprintf("%s/%s", safeSymbol, dateStr)
	path := filepath.Join(logDir, safeSymbol, dateStr+".jsonl")

	l.mu.Lock()
	defer l.mu.Unlock()

	f, ok := files[key]
	if !ok {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[order-log] 创建目录失败 %s: %v", dir, err)
			return
		}
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[order-log] 打开文件失败 %s: %v", path, err)
			return
		}
		files[key] = f
	}

	if _, err := f.Write(data); err != nil {
		log.Printf("[order-log] 写入失败，准备重试 path=%s symbol=%s: %v", path, symbol, err)
		_ = f.Close()

		reopened, openErr := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if openErr != nil {
			delete(files, key)
			log.Printf("[order-log] 重开文件失败 %s: %v", path, openErr)
			return
		}
		files[key] = reopened
		if _, retryErr := reopened.Write(data); retryErr != nil {
			log.Printf("[order-log] 重试写入失败 path=%s symbol=%s: %v", path, symbol, retryErr)
			return
		}
	}
}

func sanitizePathComponent(s string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(s)
}

// Close 关闭所有日志文件
func (l *Logger) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	for key, f := range l.signalFiles {
		_ = f.Close()
		delete(l.signalFiles, key)
	}
	for key, f := range l.orderFiles {
		_ = f.Close()
		delete(l.orderFiles, key)
	}
}

// ---------------------------------------------------------------------------
// 数值格式化
// ---------------------------------------------------------------------------

// round2 四舍五入保留2位小数
func round2(v float64) float64 {
	s := strconv.FormatFloat(v, 'f', 2, 64)
	r, _ := strconv.ParseFloat(s, 64)
	return r
}

// round4 四舍五入保留4位小数
func round4(v float64) float64 {
	s := strconv.FormatFloat(v, 'f', 4, 64)
	r, _ := strconv.ParseFloat(s, 64)
	return r
}

func formatMillisTime(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
}

// formatNumbers 将日志条目中的浮点数格式化为2位小数
// 通过反射修改结构体字段值
func formatNumbers(entry interface{}) {
	switch e := entry.(type) {
	case *SignalLogEntry:
		e.Quantity = round4(e.Quantity)
		e.EntryPrice = round2(e.EntryPrice)
		e.StopLoss = round2(e.StopLoss)
		e.Atr = round2(e.Atr)
		e.RiskReward = round2(e.RiskReward)
		for i, tp := range e.TakeProfits {
			e.TakeProfits[i] = round2(tp)
		}
		for k, v := range e.Indicators {
			e.Indicators[k] = round2(v)
		}
	case *OrderLogEntry:
		e.Quantity = round4(e.Quantity)
		e.OrderQuantity = round4(e.OrderQuantity)
		e.ExecutedQty = round4(e.ExecutedQty)
		e.AvgPrice = round2(e.AvgPrice)
		e.Commission = round2(e.Commission)
		e.Slippage = round2(e.Slippage)
		e.StopLoss = round2(e.StopLoss)
		e.Atr = round2(e.Atr)
		e.RiskReward = round2(e.RiskReward)
		e.HarvestPathProbability = round4(e.HarvestPathProbability)
		e.HarvestPathRuleProbability = round4(e.HarvestPathRuleProbability)
		e.HarvestPathLSTMProbability = round4(e.HarvestPathLSTMProbability)
		e.HarvestPathBookProbability = round4(e.HarvestPathBookProbability)
		e.HarvestPathAppliedThreshold = round4(e.HarvestPathAppliedThreshold)
		e.HarvestPathReferencePrice = round2(e.HarvestPathReferencePrice)
		e.HarvestPathMarketPrice = round2(e.HarvestPathMarketPrice)
		if e.Protection != nil {
			if e.Protection.StopLoss != nil {
				e.Protection.StopLoss.TriggerPrice = round2(e.Protection.StopLoss.TriggerPrice)
			}
			if e.Protection.TakeProfit != nil {
				e.Protection.TakeProfit.TriggerPrice = round2(e.Protection.TakeProfit.TriggerPrice)
			}
		}
	}
}
