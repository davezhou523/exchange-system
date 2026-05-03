package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ordercfg "exchange-system/app/order/rpc/internal/config"
	"exchange-system/app/order/rpc/internal/svc"
	pb "exchange-system/common/pb/order"
)

type testExecutionOrderLogEntry struct {
	Timestamp                   string                 `json:"timestamp"`
	SignalType                  string                 `json:"signal_type"`
	StrategyID                  string                 `json:"strategy_id"`
	Symbol                      string                 `json:"symbol"`
	OrderID                     string                 `json:"order_id"`
	ClientID                    string                 `json:"client_id"`
	Side                        string                 `json:"side"`
	PositionSide                string                 `json:"position_side"`
	Type                        string                 `json:"type"`
	Status                      string                 `json:"status"`
	Quantity                    float64                `json:"quantity"`
	ExecutedQty                 float64                `json:"executed_qty"`
	AvgPrice                    float64                `json:"avg_price"`
	Commission                  float64                `json:"commission"`
	CommissionAsset             string                 `json:"commission_asset"`
	Slippage                    float64                `json:"slippage"`
	StopLoss                    float64                `json:"stop_loss"`
	Atr                         float64                `json:"atr"`
	RiskReward                  float64                `json:"risk_reward"`
	Reason                      string                 `json:"reason"`
	SignalReason                map[string]interface{} `json:"signal_reason,omitempty"`
	Protection                  map[string]interface{} `json:"protection,omitempty"`
	TransactTime                int64                  `json:"transact_time"`
	HarvestPathProbability      float64                `json:"harvest_path_probability"`
	HarvestPathRuleProbability  float64                `json:"harvest_path_rule_probability"`
	HarvestPathLSTMProbability  float64                `json:"harvest_path_lstm_probability"`
	HarvestPathBookProbability  float64                `json:"harvest_path_book_probability,omitempty"`
	HarvestPathBookSummary      string                 `json:"harvest_path_book_summary,omitempty"`
	HarvestPathVolatilityRegime string                 `json:"harvest_path_volatility_regime,omitempty"`
	HarvestPathThresholdSource  string                 `json:"harvest_path_threshold_source,omitempty"`
	HarvestPathAppliedThreshold float64                `json:"harvest_path_applied_threshold,omitempty"`
	HarvestPathAction           string                 `json:"harvest_path_action"`
	HarvestPathRiskLevel        string                 `json:"harvest_path_risk_level"`
	HarvestPathTargetSide       string                 `json:"harvest_path_target_side"`
	HarvestPathReferencePrice   float64                `json:"harvest_path_reference_price"`
	HarvestPathMarketPrice      float64                `json:"harvest_path_market_price"`
}

func TestGetAllOrdersFallsBackToExecutionLogs(t *testing.T) {
	t.Parallel()

	server := newMockBinanceServer()
	defer server.Close()

	rootDir := t.TempDir()
	execLogDir := filepath.Join(rootDir, "execution-order")
	dataDir := filepath.Join(rootDir, "order-data")
	transactTime := time.Now().UTC().UnixMilli()

	if err := writeExecutionOrderLog(execLogDir, testExecutionOrderLogEntry{
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
		SignalType:      "OPEN",
		StrategyID:      "trend-following-ETHUSDT",
		Symbol:          "ETHUSDT",
		OrderID:         "10001",
		ClientID:        "cid-10001",
		Side:            "BUY",
		PositionSide:    "LONG",
		Type:            "MARKET",
		Status:          "FILLED",
		Quantity:        0.25,
		ExecutedQty:     0.25,
		AvgPrice:        2449.8,
		Commission:      0.51,
		CommissionAsset: "USDT",
		StopLoss:        2400.5,
		Atr:             18.6,
		RiskReward:      2.0,
		Reason:          "[harvest-path] path_action=WAIT_FOR_RECLAIM | risk=PATH_ALERT | target=UP | ref=2450.50 | market=2448.30 | trend aligned",
		SignalReason: map[string]interface{}{
			"summary":        "open long",
			"phase":          "OPEN_ENTRY",
			"route_bucket":   "trend",
			"route_reason":   "market_state_trend",
			"route_template": "eth-trend",
			"range": map[string]interface{}{
				"h1_range_ok":       true,
				"h1_adx_ok":         true,
				"h1_boll_width_ok":  true,
				"m15_touch_lower":   true,
				"m15_rsi_turn_up":   true,
				"m15_touch_upper":   false,
				"m15_rsi_turn_down": false,
			},
			"allocator": map[string]interface{}{
				"template":        "eth-trend",
				"route_bucket":    "trend",
				"route_reason":    "market_state_trend",
				"score":           1.1,
				"score_source":    "symbol_score",
				"bucket_budget":   0.6,
				"strategy_weight": 0.6,
				"symbol_weight":   0.5,
				"risk_scale":      1.0,
				"position_budget": 0.3,
			},
			"tags": []string{"15m", "trend_following", "long"},
		},
		Protection: map[string]interface{}{
			"requested": true,
			"status":    "partial_success",
			"reason":    "1/2 protection leg(s) created; set take profit failed: trigger too close",
			"stop_loss": map[string]interface{}{
				"requested":       true,
				"status":          "success",
				"trigger_price":   2400.5,
				"order_id":        "9001",
				"client_order_id": "algo-sl-9001",
			},
			"take_profit": map[string]interface{}{
				"requested":     true,
				"status":        "failed",
				"trigger_price": 2480.5,
				"reason":        "set take profit failed: trigger too close",
			},
		},
		TransactTime:               transactTime,
		HarvestPathProbability:     0.82,
		HarvestPathRuleProbability: 0.73,
		HarvestPathLSTMProbability: 0.64,
		HarvestPathAction:          "WAIT_FOR_RECLAIM",
		HarvestPathRiskLevel:       "PATH_ALERT",
		HarvestPathTargetSide:      "UP",
		HarvestPathReferencePrice:  2450.5,
		HarvestPathMarketPrice:     2448.3,
	}); err != nil {
		t.Fatalf("write execution order log failed: %v", err)
	}

	cfg := ordercfg.Config{}
	cfg.Binance.BaseURL = server.URL
	cfg.DataDir = dataDir
	cfg.ExecutionOrderLogDir = execLogDir

	svcCtx, err := svc.NewServiceContext(cfg)
	if err != nil {
		t.Fatalf("new service context failed: %v", err)
	}
	defer func() {
		_ = svcCtx.Close()
	}()

	logic := NewGetAllOrdersLogic(context.Background(), svcCtx)
	resp, err := logic.GetAllOrders(&pb.OrderQueryRequest{Symbol: "ETHUSDT", Limit: 10})
	if err != nil {
		t.Fatalf("GetAllOrders failed: %v", err)
	}
	if len(resp.GetItems()) != 1 {
		t.Fatalf("expected 1 order item, got %d", len(resp.GetItems()))
	}

	item := resp.GetItems()[0]
	if item.GetOrderId() != 10001 {
		t.Fatalf("unexpected order id: %d", item.GetOrderId())
	}
	if item.GetSymbol() != "ETHUSDT" {
		t.Fatalf("unexpected symbol: %s", item.GetSymbol())
	}
	if item.GetStatus() != "FILLED" {
		t.Fatalf("unexpected status: %s", item.GetStatus())
	}
	if item.GetSide() != "BUY" || item.GetPositionSide() != "LONG" {
		t.Fatalf("unexpected side tuple: side=%s positionSide=%s", item.GetSide(), item.GetPositionSide())
	}
	if item.GetClientOrderId() != "cid-10001" {
		t.Fatalf("unexpected client order id: %s", item.GetClientOrderId())
	}
	if item.GetActionType() != "OPEN_LONG" {
		t.Fatalf("unexpected action type: %s", item.GetActionType())
	}
	if item.GetHarvestPathAction() != "WAIT_FOR_RECLAIM" {
		t.Fatalf("unexpected harvest path action: %s", item.GetHarvestPathAction())
	}
	if item.GetHarvestPathRiskLevel() != "PATH_ALERT" {
		t.Fatalf("unexpected harvest path risk level: %s", item.GetHarvestPathRiskLevel())
	}
	if item.GetHarvestPathProbability() != "0.8200" {
		t.Fatalf("unexpected harvest path probability: %s", item.GetHarvestPathProbability())
	}
	if item.GetSignalReason() == nil || item.GetSignalReason().GetSummary() != "open long" {
		t.Fatalf("unexpected signal reason: %+v", item.GetSignalReason())
	}
	if item.GetSignalReason().GetAllocator() == nil || item.GetSignalReason().GetAllocator().GetScoreSource() != "symbol_score" {
		t.Fatalf("unexpected signal_reason allocator: %+v", item.GetSignalReason().GetAllocator())
	}
	if item.GetSignalReason().GetRange() == nil || !item.GetSignalReason().GetRange().GetH1RangeOk() || !item.GetSignalReason().GetRange().GetM15TouchLower() {
		t.Fatalf("unexpected signal_reason range: %+v", item.GetSignalReason().GetRange())
	}
	if item.GetProtection() == nil || item.GetProtection().GetStatus() != "partial_success" {
		t.Fatalf("unexpected protection summary: %+v", item.GetProtection())
	}
	if item.GetProtection().GetStopLoss() == nil || item.GetProtection().GetStopLoss().GetOrderId() != "9001" {
		t.Fatalf("unexpected protection stop_loss: %+v", item.GetProtection().GetStopLoss())
	}
	if item.GetProtection().GetTakeProfit() == nil || item.GetProtection().GetTakeProfit().GetReason() == "" {
		t.Fatalf("unexpected protection take_profit: %+v", item.GetProtection().GetTakeProfit())
	}
	if !strings.Contains(item.GetReason(), "WAIT_FOR_RECLAIM") {
		t.Fatalf("reason should contain harvest path context, got %q", item.GetReason())
	}

	snapshotPath := filepath.Join(dataDir, "all_orders", "ETHUSDT", time.Now().UTC().Format("2006-01-02")+".jsonl")
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected snapshot to contain 1 line, got %d", len(lines))
	}

	resp, err = logic.GetAllOrders(&pb.OrderQueryRequest{Symbol: "ETHUSDT", Limit: 10})
	if err != nil {
		t.Fatalf("second GetAllOrders failed: %v", err)
	}
	content, err = os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot after second call failed: %v", err)
	}
	lines = strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected snapshot overwrite semantics, got %d lines after second call", len(lines))
	}
	if len(resp.GetItems()) != 1 {
		t.Fatalf("expected 1 order item on second call, got %d", len(resp.GetItems()))
	}
}

func TestGetAllOrdersBuildsOpenCloseLifecycleFromExecutionLogs(t *testing.T) {
	t.Parallel()

	server := newMockBinanceServer()
	defer server.Close()

	rootDir := t.TempDir()
	execLogDir := filepath.Join(rootDir, "execution-order")
	dataDir := filepath.Join(rootDir, "order-data")
	openTime := time.Now().UTC().Add(-2 * time.Minute).UnixMilli()
	closeTime := time.Now().UTC().Add(-1 * time.Minute).UnixMilli()

	if err := writeExecutionOrderLog(execLogDir, testExecutionOrderLogEntry{
		Timestamp:       time.UnixMilli(openTime).UTC().Format(time.RFC3339),
		SignalType:      "OPEN",
		StrategyID:      "trend-following-ETHUSDT",
		Symbol:          "ETHUSDT",
		OrderID:         "10001",
		ClientID:        "cid-open-10001",
		Side:            "BUY",
		PositionSide:    "LONG",
		Type:            "MARKET",
		Status:          "FILLED",
		Quantity:        0.25,
		ExecutedQty:     0.25,
		AvgPrice:        2449.8,
		Commission:      0.51,
		CommissionAsset: "USDT",
		StopLoss:        2400.5,
		Atr:             18.6,
		RiskReward:      2.0,
		Reason:          "trend aligned open",
		SignalReason: map[string]interface{}{
			"summary": "open long",
			"phase":   "OPEN_ENTRY",
			"tags":    []string{"15m", "trend_following", "long"},
		},
		TransactTime: openTime,
	}); err != nil {
		t.Fatalf("write open execution order log failed: %v", err)
	}

	if err := writeExecutionOrderLog(execLogDir, testExecutionOrderLogEntry{
		Timestamp:       time.UnixMilli(closeTime).UTC().Format(time.RFC3339),
		SignalType:      "CLOSE",
		StrategyID:      "trend-following-ETHUSDT",
		Symbol:          "ETHUSDT",
		OrderID:         "10002",
		ClientID:        "cid-close-10002",
		Side:            "SELL",
		PositionSide:    "LONG",
		Type:            "MARKET",
		Status:          "FILLED",
		Quantity:        0.25,
		ExecutedQty:     0.25,
		AvgPrice:        2462.1,
		Commission:      0.52,
		CommissionAsset: "USDT",
		Reason:          "ema exit close",
		SignalReason: map[string]interface{}{
			"summary": "close long",
			"phase":   "EXIT",
			"tags":    []string{"ema_exit", "long"},
		},
		TransactTime: closeTime,
	}); err != nil {
		t.Fatalf("write close execution order log failed: %v", err)
	}

	cfg := ordercfg.Config{}
	cfg.Binance.BaseURL = server.URL
	cfg.DataDir = dataDir
	cfg.ExecutionOrderLogDir = execLogDir

	svcCtx, err := svc.NewServiceContext(cfg)
	if err != nil {
		t.Fatalf("new service context failed: %v", err)
	}
	defer func() {
		_ = svcCtx.Close()
	}()

	logic := NewGetAllOrdersLogic(context.Background(), svcCtx)
	resp, err := logic.GetAllOrders(&pb.OrderQueryRequest{Symbol: "ETHUSDT", Limit: 10})
	if err != nil {
		t.Fatalf("GetAllOrders failed: %v", err)
	}
	if len(resp.GetItems()) != 2 {
		t.Fatalf("expected 2 order items, got %d", len(resp.GetItems()))
	}

	latest := resp.GetItems()[0]
	earliest := resp.GetItems()[1]

	if latest.GetOrderId() != 10002 {
		t.Fatalf("expected close order first, got %d", latest.GetOrderId())
	}
	if latest.GetActionType() != "CLOSE_LONG" {
		t.Fatalf("unexpected latest action type: %s", latest.GetActionType())
	}
	if latest.GetSignalReason() == nil || latest.GetSignalReason().GetSummary() != "close long" {
		t.Fatalf("unexpected latest signal reason: %+v", latest.GetSignalReason())
	}

	if earliest.GetOrderId() != 10001 {
		t.Fatalf("expected open order second, got %d", earliest.GetOrderId())
	}
	if earliest.GetActionType() != "OPEN_LONG" {
		t.Fatalf("unexpected earliest action type: %s", earliest.GetActionType())
	}
	if earliest.GetSignalReason() == nil || earliest.GetSignalReason().GetSummary() != "open long" {
		t.Fatalf("unexpected earliest signal reason: %+v", earliest.GetSignalReason())
	}

	if latest.GetPositionCycleId() == "" {
		t.Fatal("expected non-empty position cycle id for close order")
	}
	if earliest.GetPositionCycleId() == "" {
		t.Fatal("expected non-empty position cycle id for open order")
	}
	if latest.GetPositionCycleId() != earliest.GetPositionCycleId() {
		t.Fatalf("expected same position cycle id, got close=%s open=%s", latest.GetPositionCycleId(), earliest.GetPositionCycleId())
	}
	if !strings.HasPrefix(latest.GetPositionCycleId(), "ETHUSDT-LONG-") {
		t.Fatalf("unexpected position cycle id format: %s", latest.GetPositionCycleId())
	}

	snapshotPath := filepath.Join(dataDir, "all_orders", "ETHUSDT", time.Now().UTC().Format("2006-01-02")+".jsonl")
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected snapshot to contain 2 lines, got %d", len(lines))
	}
}

// TestGetAllOrdersBuildsPartialCloseLifecycleFromExecutionLogs 验证 execution 日志中的 PARTIAL_CLOSE 会在查询层保留为独立动作类型。
func TestGetAllOrdersBuildsPartialCloseLifecycleFromExecutionLogs(t *testing.T) {
	t.Parallel()

	server := newMockBinanceServer()
	defer server.Close()

	rootDir := t.TempDir()
	execLogDir := filepath.Join(rootDir, "execution-order")
	dataDir := filepath.Join(rootDir, "order-data")
	openTime := time.Now().UTC().Add(-3 * time.Minute).UnixMilli()
	partialCloseTime := time.Now().UTC().Add(-2 * time.Minute).UnixMilli()
	closeTime := time.Now().UTC().Add(-1 * time.Minute).UnixMilli()

	if err := writeExecutionOrderLog(execLogDir, testExecutionOrderLogEntry{
		Timestamp:    time.UnixMilli(openTime).UTC().Format(time.RFC3339),
		SignalType:   "OPEN",
		StrategyID:   "range-ETHUSDT",
		Symbol:       "ETHUSDT",
		OrderID:      "20001",
		ClientID:     "cid-open-20001",
		Side:         "BUY",
		PositionSide: "LONG",
		Type:         "MARKET",
		Status:       "FILLED",
		Quantity:     0.25,
		ExecutedQty:  0.25,
		AvgPrice:     2450.0,
		Reason:       "range open",
		SignalReason: map[string]interface{}{
			"summary": "open long",
			"phase":   "OPEN_ENTRY",
		},
		TransactTime: openTime,
	}); err != nil {
		t.Fatalf("write open execution order log failed: %v", err)
	}

	if err := writeExecutionOrderLog(execLogDir, testExecutionOrderLogEntry{
		Timestamp:    time.UnixMilli(partialCloseTime).UTC().Format(time.RFC3339),
		SignalType:   "PARTIAL_CLOSE",
		StrategyID:   "range-ETHUSDT",
		Symbol:       "ETHUSDT",
		OrderID:      "20002",
		ClientID:     "cid-partial-20002",
		Side:         "SELL",
		PositionSide: "LONG",
		Type:         "MARKET",
		Status:       "FILLED",
		Quantity:     0.10,
		ExecutedQty:  0.10,
		AvgPrice:     2460.0,
		Reason:       "split tp1 partial close",
		SignalReason: map[string]interface{}{
			"summary":           "partial close long",
			"phase":             "EXIT",
			"exit_reason_kind":  "partial_take_profit",
			"exit_reason_label": "分批止盈",
			"tags":              []string{"split_tp1", "partial_close"},
		},
		TransactTime: partialCloseTime,
	}); err != nil {
		t.Fatalf("write partial close execution order log failed: %v", err)
	}

	if err := writeExecutionOrderLog(execLogDir, testExecutionOrderLogEntry{
		Timestamp:    time.UnixMilli(closeTime).UTC().Format(time.RFC3339),
		SignalType:   "CLOSE",
		StrategyID:   "range-ETHUSDT",
		Symbol:       "ETHUSDT",
		OrderID:      "20003",
		ClientID:     "cid-close-20003",
		Side:         "SELL",
		PositionSide: "LONG",
		Type:         "MARKET",
		Status:       "FILLED",
		Quantity:     0.15,
		ExecutedQty:  0.15,
		AvgPrice:     2465.0,
		Reason:       "final close",
		SignalReason: map[string]interface{}{
			"summary":           "close long",
			"phase":             "EXIT",
			"exit_reason_kind":  "break_even_stop",
			"exit_reason_label": "保本止损",
		},
		TransactTime: closeTime,
	}); err != nil {
		t.Fatalf("write close execution order log failed: %v", err)
	}

	cfg := ordercfg.Config{}
	cfg.Binance.BaseURL = server.URL
	cfg.DataDir = dataDir
	cfg.ExecutionOrderLogDir = execLogDir

	svcCtx, err := svc.NewServiceContext(cfg)
	if err != nil {
		t.Fatalf("new service context failed: %v", err)
	}
	defer func() {
		_ = svcCtx.Close()
	}()

	logic := NewGetAllOrdersLogic(context.Background(), svcCtx)
	resp, err := logic.GetAllOrders(&pb.OrderQueryRequest{Symbol: "ETHUSDT", Limit: 10})
	if err != nil {
		t.Fatalf("GetAllOrders failed: %v", err)
	}
	if len(resp.GetItems()) != 3 {
		t.Fatalf("expected 3 order items, got %d", len(resp.GetItems()))
	}

	latest := resp.GetItems()[0]
	partial := resp.GetItems()[1]
	earliest := resp.GetItems()[2]
	if latest.GetActionType() != "CLOSE_LONG" {
		t.Fatalf("latest action type = %s, want CLOSE_LONG", latest.GetActionType())
	}
	if partial.GetActionType() != "PARTIAL_CLOSE_LONG" {
		t.Fatalf("partial action type = %s, want PARTIAL_CLOSE_LONG", partial.GetActionType())
	}
	if earliest.GetActionType() != "OPEN_LONG" {
		t.Fatalf("earliest action type = %s, want OPEN_LONG", earliest.GetActionType())
	}
	if partial.GetSignalReason() == nil || partial.GetSignalReason().GetSummary() != "partial close long" {
		t.Fatalf("unexpected partial close signal reason: %+v", partial.GetSignalReason())
	}
	if partial.GetSignalReason().GetExitReasonKind() != "partial_take_profit" || partial.GetSignalReason().GetExitReasonLabel() != "分批止盈" {
		t.Fatalf("unexpected partial close exit reason: %+v", partial.GetSignalReason())
	}
	if latest.GetSignalReason().GetExitReasonKind() != "break_even_stop" || latest.GetSignalReason().GetExitReasonLabel() != "保本止损" {
		t.Fatalf("unexpected latest exit reason: %+v", latest.GetSignalReason())
	}
	if partial.GetPositionCycleId() == "" || partial.GetPositionCycleId() != latest.GetPositionCycleId() || partial.GetPositionCycleId() != earliest.GetPositionCycleId() {
		t.Fatalf("expected all orders to share same cycle id, got open=%s partial=%s close=%s", earliest.GetPositionCycleId(), partial.GetPositionCycleId(), latest.GetPositionCycleId())
	}

	snapshotPath := filepath.Join(dataDir, "all_orders", "ETHUSDT", time.Now().UTC().Format("2006-01-02")+".jsonl")
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot failed: %v", err)
	}
	if !strings.Contains(string(content), "\"action_type\":\"PARTIAL_CLOSE_LONG\"") {
		t.Fatalf("snapshot should contain PARTIAL_CLOSE_LONG, got %s", string(content))
	}
}

func writeExecutionOrderLog(logDir string, entry testExecutionOrderLogEntry) error {
	dateStr := time.Now().UTC().Format("2006-01-02")
	filePath := filepath.Join(logDir, entry.Symbol, dateStr+".jsonl")
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return err
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(payload)
	return err
}

func newMockBinanceServer() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/fapi/v1/allOrders", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "simulated allOrders unavailable", http.StatusBadGateway)
	})
	handler.HandleFunc("/fapi/v1/userTrades", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "simulated userTrades unavailable", http.StatusBadGateway)
	})
	handler.HandleFunc("/fapi/v1/exchangeInfo", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"symbols":[{"symbol":"ETHUSDT","pricePrecision":2,"quantityPrecision":3}]}`)
	})
	return httptest.NewServer(handler)
}
