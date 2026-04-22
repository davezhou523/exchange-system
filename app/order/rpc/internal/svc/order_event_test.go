package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ordercfg "exchange-system/app/order/rpc/internal/config"
	orderkafka "exchange-system/app/order/rpc/internal/kafka"
)

type testAccountState struct {
	mu        sync.RWMutex
	positions []map[string]string
}

func (s *testAccountState) setPositions(positions []map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.positions = positions
}

func (s *testAccountState) response() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	positions := make([]map[string]string, len(s.positions))
	copy(positions, s.positions)
	return map[string]interface{}{
		"totalWalletBalance":    "10000",
		"totalUnrealizedProfit": "0",
		"totalMarginBalance":    "10000",
		"availableBalance":      "10000",
		"maxWithdrawAmount":     "10000",
		"positions":             positions,
	}
}

func TestHandleOrderEventRefreshesSnapshotsAndKeepsQueryConsistent(t *testing.T) {
	state := &testAccountState{}
	state.setPositions([]map[string]string{{
		"symbol":           "ETHUSDT",
		"positionAmt":      "0.25",
		"entryPrice":       "2449.8",
		"markPrice":        "2451.2",
		"unrealizedProfit": "0.35",
		"liquidationPrice": "2100.0",
		"leverage":         "7",
		"marginType":       "cross",
	}})

	server := newMockOrderEventBinanceServer(state)
	defer server.Close()

	rootDir := t.TempDir()
	execLogDir := filepath.Join(rootDir, "execution-order")
	dataDir := filepath.Join(rootDir, "order-data")

	cfg := ordercfg.Config{}
	cfg.Binance.BaseURL = server.URL
	cfg.DataDir = dataDir
	cfg.ExecutionOrderLogDir = execLogDir

	svcCtx, err := NewServiceContext(cfg)
	if err != nil {
		t.Fatalf("new service context failed: %v", err)
	}
	defer func() {
		_ = svcCtx.Close()
	}()

	openTime := time.Now().UTC().Add(-2 * time.Minute).UnixMilli()
	closeTime := time.Now().UTC().Add(-1 * time.Minute).UnixMilli()

	if err := writeSvcExecutionOrderLog(execLogDir, executionOrderLogEntry{
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
		SignalReason: &signalReasonJSON{
			Summary: "open long",
			Phase:   "OPEN_ENTRY",
			Tags:    []string{"15m", "trend_following", "long"},
		},
		TransactTime: openTime,
	}); err != nil {
		t.Fatalf("write open execution log failed: %v", err)
	}

	openEvent := &orderkafka.OrderEvent{
		OrderID:      "10001",
		ClientID:     "cid-open-10001",
		Symbol:       "ETHUSDT",
		Status:       "FILLED",
		SignalType:   "OPEN",
		Side:         "BUY",
		PositionSide: "LONG",
		Quantity:     0.25,
		AvgPrice:     2449.8,
		Timestamp:    openTime,
		StrategyID:   "trend-following-ETHUSDT",
	}
	if err := svcCtx.handleOrderEvent(openEvent); err != nil {
		t.Fatalf("handle open event failed: %v", err)
	}

	positionsPath := filepath.Join(dataDir, "positions", "ETHUSDT", time.Now().UTC().Format("2006-01-02")+".jsonl")
	allOrdersPath := filepath.Join(dataDir, "all_orders", "ETHUSDT", time.Now().UTC().Format("2006-01-02")+".jsonl")

	positionsContent, err := os.ReadFile(positionsPath)
	if err != nil {
		t.Fatalf("read positions snapshot failed: %v", err)
	}
	if got := countJSONLLines(positionsContent); got != 1 {
		t.Fatalf("expected 1 position snapshot row after open event, got %d", got)
	}

	openOrders, err := svcCtx.GetAllOrders(context.Background(), "ETHUSDT", 0, 0, 10)
	if err != nil {
		t.Fatalf("GetAllOrders after open event failed: %v", err)
	}
	if len(openOrders) != 1 {
		t.Fatalf("expected 1 order after open event, got %d", len(openOrders))
	}

	state.setPositions([]map[string]string{{
		"symbol":           "ETHUSDT",
		"positionAmt":      "0",
		"entryPrice":       "0",
		"markPrice":        "0",
		"unrealizedProfit": "0",
		"liquidationPrice": "0",
		"leverage":         "7",
		"marginType":       "cross",
	}})
	if err := writeSvcExecutionOrderLog(execLogDir, executionOrderLogEntry{
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
		SignalReason: &signalReasonJSON{
			Summary: "close long",
			Phase:   "EXIT",
			Tags:    []string{"ema_exit", "long"},
		},
		TransactTime: closeTime,
	}); err != nil {
		t.Fatalf("write close execution log failed: %v", err)
	}

	closeEvent := &orderkafka.OrderEvent{
		OrderID:      "10002",
		ClientID:     "cid-close-10002",
		Symbol:       "ETHUSDT",
		Status:       "FILLED",
		SignalType:   "CLOSE",
		Side:         "SELL",
		PositionSide: "LONG",
		Quantity:     0.25,
		AvgPrice:     2462.1,
		Timestamp:    closeTime,
		StrategyID:   "trend-following-ETHUSDT",
	}
	if err := svcCtx.handleOrderEvent(closeEvent); err != nil {
		t.Fatalf("handle close event failed: %v", err)
	}

	if _, err := os.Stat(positionsPath); !os.IsNotExist(err) {
		t.Fatalf("expected positions snapshot removed after close event, got err=%v", err)
	}

	refreshedOrders, err := svcCtx.GetAllOrders(context.Background(), "ETHUSDT", 0, 0, 10)
	if err != nil {
		t.Fatalf("GetAllOrders after close event failed: %v", err)
	}
	if len(refreshedOrders) != 2 {
		t.Fatalf("expected 2 orders after close event, got %d", len(refreshedOrders))
	}
	if refreshedOrders[0].OrderID != 10002 || refreshedOrders[0].Side != "SELL" {
		t.Fatalf("expected close order first, got order=%+v", refreshedOrders[0])
	}
	if refreshedOrders[1].OrderID != 10001 || refreshedOrders[1].Side != "BUY" {
		t.Fatalf("expected open order second, got order=%+v", refreshedOrders[1])
	}

	allOrdersContent, err := os.ReadFile(allOrdersPath)
	if err != nil {
		t.Fatalf("read all_orders snapshot failed: %v", err)
	}
	if got := countJSONLLines(allOrdersContent); got != 2 {
		t.Fatalf("expected 2 all_orders snapshot rows after close event, got %d", got)
	}
	if !strings.Contains(string(allOrdersContent), "\"action_type\":\"CLOSE_LONG\"") {
		t.Fatalf("all_orders snapshot should contain CLOSE_LONG, got %s", string(allOrdersContent))
	}
	if !strings.Contains(string(allOrdersContent), "\"action_type\":\"OPEN_LONG\"") {
		t.Fatalf("all_orders snapshot should contain OPEN_LONG, got %s", string(allOrdersContent))
	}
}

func writeSvcExecutionOrderLog(logDir string, entry executionOrderLogEntry) error {
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

func countJSONLLines(content []byte) int {
	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return 0
	}
	return len(strings.Split(trimmed, "\n"))
}

func newMockOrderEventBinanceServer(state *testAccountState) *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/fapi/v2/account", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state.response())
	})
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
