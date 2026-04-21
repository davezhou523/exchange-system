package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"exchange-system/app/execution/rpc/internal/exchange"
	"exchange-system/app/execution/rpc/internal/idempotent"
	execKafka "exchange-system/app/execution/rpc/internal/kafka"
	"exchange-system/app/execution/rpc/internal/order"
	"exchange-system/app/execution/rpc/internal/orderlog"
	"exchange-system/app/execution/rpc/internal/position"
	"exchange-system/app/execution/rpc/internal/risk"
	marketpb "exchange-system/common/pb/market"
	strategypb "exchange-system/common/pb/strategy"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestHandleSignalKline1mPublishesOrderEventAndWritesOrderLog(t *testing.T) {
	t.Parallel()

	logRoot := t.TempDir()
	orderLogDir := filepath.Join(logRoot, "order")
	signalLogDir := filepath.Join(logRoot, "signal")

	simExchange := exchange.NewSimulatedExchange(exchange.SimConfig{
		InitialBalance:  10000,
		SlippageBPS:     0,
		SlippageModel:   "fixed",
		CommissionRate:  0.0004,
		CommissionAsset: "USDT",
		MatchMode:       exchange.MatchModeKline1m,
	})

	router := exchange.NewRouter(exchange.RouterConfig{
		Strategy:        exchange.RouteSimulated,
		DefaultExchange: "simulated",
	})
	router.Register("simulated", simExchange)

	posManager := position.NewManager()
	orderManager := order.NewManager()
	riskManager := risk.NewManager(risk.RiskConfig{
		MaxPositionSize:     1.0,
		MaxLeverage:         20,
		MaxDailyLossPct:     1.0,
		MaxDrawdownPct:      1.0,
		MaxOpenPositions:    5,
		MaxPositionExposure: 10,
		StopLossPercent:     0.02,
		MinOrderNotional:    5,
	}, posManager)

	eventCh := make(chan map[string]interface{}, 1)
	producer := newMockExecutionProducer(t, eventCh)

	svcCtx := &ServiceContext{
		router:             router,
		riskManager:        riskManager,
		orderManager:       orderManager,
		posManager:         posManager,
		deduplicator:       idempotent.NewSignalDeduplicator(time.Minute),
		logger:             orderlog.NewLogger(signalLogDir, orderLogDir),
		simExchange:        simExchange,
		orderProducer:      producer,
		harvestPathSignals: make(map[string]harvestPathRiskSnapshot),
	}
	defer func() {
		_ = svcCtx.Close()
	}()
	simExchange.SetOnFillCallback(func(result *exchange.OrderResult) {
		svcCtx.handleSimFill(result)
	})

	sig := &strategypb.Signal{
		StrategyId: "trend-following-ETHUSDT",
		Symbol:     "ETHUSDT",
		Action:     "BUY",
		Side:       "LONG",
		SignalType: "OPEN",
		Timestamp:  time.Now().UTC().UnixMilli(),
		Quantity:   0.2,
		EntryPrice: 2500,
		StopLoss:   2450,
		TakeProfits: []float64{
			2550,
			2600,
		},
		Atr:        18.6,
		RiskReward: 2.0,
		Reason:     "trend aligned open",
		Indicators: map[string]float64{
			"m15_rsi": 53.2,
		},
		SignalReason: &strategypb.SignalReason{
			Summary:          "open long",
			Phase:            "OPEN_ENTRY",
			TrendContext:     "4H uptrend",
			SetupContext:     "1H pullback + 15m breakout",
			PathContext:      "harvest_path_guard=disabled",
			ExecutionContext: "next 1m open",
			Tags:             []string{"1m", "trend_following", "long"},
		},
	}

	if err := svcCtx.HandleSignal(context.Background(), sig); err != nil {
		t.Fatalf("HandleSignal failed: %v", err)
	}

	pendingOrders := orderManager.GetOrdersBySymbol("ETHUSDT")
	if len(pendingOrders) != 1 {
		t.Fatalf("expected 1 pending order after HandleSignal, got %d", len(pendingOrders))
	}
	if pendingOrders[0].Status != exchange.StatusNew {
		t.Fatalf("expected pending order status NEW, got %s", pendingOrders[0].Status)
	}

	kline := &marketpb.Kline{
		Symbol:   "ETHUSDT",
		Open:     2501,
		High:     2510,
		Low:      2495,
		Close:    2506,
		OpenTime: time.Now().UTC().UnixMilli(),
	}
	if err := svcCtx.handleKline1m(kline); err != nil {
		t.Fatalf("handleKline1m failed: %v", err)
	}

	select {
	case event := <-eventCh:
		if got := event["signal_type"]; got != "OPEN" {
			t.Fatalf("unexpected signal_type: %v", got)
		}
		if got := event["strategy_id"]; got != "trend-following-ETHUSDT" {
			t.Fatalf("unexpected strategy_id: %v", got)
		}
		if got := event["symbol"]; got != "ETHUSDT" {
			t.Fatalf("unexpected symbol: %v", got)
		}
		if got := event["status"]; got != "FILLED" {
			t.Fatalf("unexpected status: %v", got)
		}
		if got := event["side"]; got != "BUY" {
			t.Fatalf("unexpected side: %v", got)
		}
		if got := event["position_side"]; got != "LONG" {
			t.Fatalf("unexpected position_side: %v", got)
		}
		if got := event["stop_loss"]; got != 2450.0 {
			t.Fatalf("unexpected stop_loss: %v", got)
		}
		if got := event["reason"]; got != "trend aligned open" {
			t.Fatalf("unexpected reason: %v", got)
		}
		signalReason, ok := event["signal_reason"].(map[string]interface{})
		if !ok {
			t.Fatalf("signal_reason type mismatch: %T", event["signal_reason"])
		}
		if signalReason["summary"] != "open long" {
			t.Fatalf("unexpected signal_reason summary: %v", signalReason["summary"])
		}
		indicators, ok := event["indicators"].(map[string]interface{})
		if !ok {
			t.Fatalf("indicators type mismatch: %T", event["indicators"])
		}
		if indicators["m15_rsi"] != 53.2 {
			t.Fatalf("unexpected indicators: %+v", indicators)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected order event after simulated fill")
	}

	filledOrders := orderManager.GetOrdersBySymbol("ETHUSDT")
	if len(filledOrders) != 1 {
		t.Fatalf("expected 1 order after fill, got %d", len(filledOrders))
	}
	if filledOrders[0].Status != exchange.StatusFilled {
		t.Fatalf("expected FILLED order status after kline match, got %s", filledOrders[0].Status)
	}

	pos, ok := posManager.GetPosition("ETHUSDT")
	if !ok {
		t.Fatal("expected position after simulated fill")
	}
	if pos.PositionAmount <= 0 {
		t.Fatalf("expected positive position amount, got %.4f", pos.PositionAmount)
	}
	if pos.StrategyID != "trend-following-ETHUSDT" {
		t.Fatalf("unexpected position strategy id: %s", pos.StrategyID)
	}

	logPath := filepath.Join(orderLogDir, "ETHUSDT", time.Now().UTC().Format("2006-01-02")+".jsonl")
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read order log failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 order log line, got %d", len(lines))
	}
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("unmarshal order log failed: %v", err)
	}
	if entry["signal_type"] != "OPEN" {
		t.Fatalf("unexpected order log signal_type: %v", entry["signal_type"])
	}
	if entry["strategy_id"] != "trend-following-ETHUSDT" {
		t.Fatalf("unexpected order log strategy_id: %v", entry["strategy_id"])
	}
	if entry["reason"] != "trend aligned open" {
		t.Fatalf("unexpected order log reason: %v", entry["reason"])
	}
}

func newMockExecutionProducer(t *testing.T, eventCh chan<- map[string]interface{}) *execKafka.Producer {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	mockProducer := mocks.NewSyncProducer(t, cfg)
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		if msg.Topic != "order" {
			return fmt.Errorf("unexpected topic: %s", msg.Topic)
		}
		payload, err := msg.Value.Encode()
		if err != nil {
			return err
		}
		var event map[string]interface{}
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		eventCh <- event
		return nil
	})

	producer := &execKafka.Producer{}
	setUnexportedField(producer, "producer", sarama.SyncProducer(mockProducer))
	setUnexportedField(producer, "topic", "order")
	return producer
}

func setUnexportedField(target interface{}, fieldName string, value interface{}) {
	field := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}
