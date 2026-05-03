package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestNormalizeOrderQuantityUsesExchangePrecision 验证开仓单也会按交易所真实数量精度向下取整。
func TestNormalizeOrderQuantityUsesExchangePrecision(t *testing.T) {
	client := NewBinanceClient("", "", "", "", 1)
	client.quantityPrecisionMap["BTCUSDT"] = 3

	got, precision, err := client.normalizeOrderQuantity(context.Background(), CreateOrderParam{
		Symbol:   "BTCUSDT",
		Quantity: 0.00538128,
	})
	if err != nil {
		t.Fatalf("normalizeOrderQuantity() error = %v", err)
	}
	if precision != 3 {
		t.Fatalf("precision = %d, want 3", precision)
	}
	if got != 0.005 {
		t.Fatalf("quantity = %.8f, want 0.00500000", got)
	}
}

// TestSetStopLossTakeProfitUsesAlgoOrderClosePosition 验证保护单优先按 algoOrder + closePosition 方式下发。
func TestSetStopLossTakeProfitUsesAlgoOrderClosePosition(t *testing.T) {
	t.Parallel()

	var requests []url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/openOrders":
			_ = json.NewEncoder(w).Encode([]any{})
		case "/fapi/v1/exchangeInfo":
			_, _ = w.Write([]byte(`{"symbols":[{"symbol":"SOLUSDT","quantityPrecision":3}]}`))
		case "/fapi/v1/algoOrder":
			requests = append(requests, r.URL.Query())
			_ = json.NewEncoder(w).Encode(map[string]any{
				"symbol":       "SOLUSDT",
				"algoId":       len(requests),
				"clientAlgoId": "algo-created",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewBinanceClient(server.URL, "key", "secret", "", 1)
	result, err := client.SetStopLossTakeProfit(context.Background(), "SOLUSDT", string(PosLong), 4.9575, 83.60, []float64{83.86, 83.90})
	if err != nil {
		t.Fatalf("SetStopLossTakeProfit() error = %v", err)
	}
	if result == nil || result.Status != "success" {
		t.Fatalf("result = %+v, want success", result)
	}
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if got := requests[0].Get("closePosition"); got != "true" {
		t.Fatalf("stop loss closePosition = %q, want true", got)
	}
	if got := requests[0].Get("quantity"); got != "" {
		t.Fatalf("stop loss quantity = %q, want empty", got)
	}
	if got := requests[0].Get("timeInForce"); got != "GTC" {
		t.Fatalf("stop loss timeInForce = %q, want GTC", got)
	}
	if got := requests[0].Get("newOrderRespType"); got != "RESULT" {
		t.Fatalf("stop loss newOrderRespType = %q, want RESULT", got)
	}
	if got := requests[0].Get("type"); got != "STOP_MARKET" {
		t.Fatalf("stop loss type = %q, want STOP_MARKET", got)
	}
	if got := requests[1].Get("type"); got != "TAKE_PROFIT_MARKET" {
		t.Fatalf("take profit type = %q, want TAKE_PROFIT_MARKET", got)
	}
	if result.StopLoss == nil || result.StopLoss.OrderID == "" {
		t.Fatalf("stop loss result = %+v, want order id", result.StopLoss)
	}
	if result.TakeProfit == nil || result.TakeProfit.Status != "success" {
		t.Fatalf("take profit result = %+v, want success", result.TakeProfit)
	}
}

// TestSetStopLossTakeProfitFallsBackToReduceOnlyQuantity 验证 closePosition 失败时会回退到 quantity + reduceOnly。
func TestSetStopLossTakeProfitFallsBackToReduceOnlyQuantity(t *testing.T) {
	t.Parallel()

	var requests []url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/openOrders":
			_ = json.NewEncoder(w).Encode([]any{})
		case "/fapi/v1/exchangeInfo":
			_, _ = w.Write([]byte(`{"symbols":[{"symbol":"SOLUSDT","quantityPrecision":3}]}`))
		case "/fapi/v1/algoOrder":
			requests = append(requests, r.URL.Query())
			if r.URL.Query().Get("closePosition") == "true" {
				http.Error(w, `{"code":-2021,"msg":"closePosition rejected"}`, http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"symbol":       "SOLUSDT",
				"algoId":       77,
				"clientAlgoId": "algo-fallback",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewBinanceClient(server.URL, "key", "secret", "", 1)
	result, err := client.SetStopLossTakeProfit(context.Background(), "SOLUSDT", string(PosLong), 4.9575, 83.60, nil)
	if err != nil {
		t.Fatalf("SetStopLossTakeProfit() error = %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if got := requests[1].Get("quantity"); got != "4.957" {
		t.Fatalf("fallback quantity = %q, want 4.957", got)
	}
	if got := requests[1].Get("reduceOnly"); got != "TRUE" {
		t.Fatalf("fallback reduceOnly = %q, want TRUE", got)
	}
	if result == nil || result.StopLoss == nil || result.StopLoss.OrderID != "77" {
		t.Fatalf("result = %+v, want fallback stop loss order id 77", result)
	}
}
