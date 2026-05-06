package websocket

import (
	"context"
	"strings"
	"testing"

	"exchange-system/common/pb/market"
)

type mockProducer struct {
	last any
}

func (m *mockProducer) SendMarketData(_ context.Context, data interface{}) error {
	m.last = data
	return nil
}

func TestHandleMessageDepth(t *testing.T) {
	depthProducer := &mockProducer{}
	client := NewBinanceWebSocketClient(
		"wss://fstream.binance.com",
		"",
		[]string{"ETHUSDT"},
		[]string{"1m"},
		nil,
		depthProducer,
		nil,
	)

	payload := []byte(`{
		"stream":"ethusdt@depth20@100ms",
		"data":{
			"e":"depthUpdate",
			"E":1710000000000,
			"s":"ETHUSDT",
			"b":[["3000.10","1.25"],["3000.00","2.50"]],
			"a":[["3000.20","1.50"],["3000.30","2.75"]]
		}
	}`)
	if _, err := client.handleMessage(context.Background(), payload); err != nil {
		t.Fatalf("handleMessage() error = %v", err)
	}

	depth, ok := depthProducer.last.(*market.Depth)
	if !ok || depth == nil {
		t.Fatalf("last produced = %T, want *market.Depth", depthProducer.last)
	}
	if depth.Symbol != "ETHUSDT" {
		t.Fatalf("depth.Symbol = %s, want ETHUSDT", depth.Symbol)
	}
	if len(depth.Bids) != 2 || len(depth.Asks) != 2 {
		t.Fatalf("depth levels = bids:%d asks:%d, want 2/2", len(depth.Bids), len(depth.Asks))
	}
}

func TestBuildStreamURLSkipsDepthWhenProducerIsTypedNil(t *testing.T) {
	var depthProducer *mockProducer
	client := NewBinanceWebSocketClient(
		"wss://fstream.binance.com",
		"",
		[]string{"ETHUSDT"},
		[]string{"1m"},
		nil,
		depthProducer,
		nil,
	)

	got := client.buildStreamURL()
	if strings.Contains(got, "@depth20@100ms") {
		t.Fatalf("buildStreamURL() = %s, should not contain depth stream for typed nil producer", got)
	}
}

func TestHandleMessageKlineWithoutKafkaProducerStillFeedsAggregator(t *testing.T) {
	aggregator := &mockAggregator{}
	client := NewBinanceWebSocketClient(
		"wss://fstream.binance.com",
		"",
		[]string{"ETHUSDT"},
		[]string{"1m"},
		nil,
		nil,
		aggregator,
	)

	payload := []byte(`{
		"stream":"ethusdt@kline_1m",
		"data":{
			"e":"kline",
			"E":1710000000202,
			"s":"ETHUSDT",
			"k":{
				"t":1710000000000,
				"T":1710000059999,
				"s":"ETHUSDT",
				"i":"1m",
				"o":"3000.10",
				"c":"3001.20",
				"h":"3002.00",
				"l":"2999.80",
				"v":"123.45",
				"x":true,
				"f":100,
				"L":200,
				"n":50,
				"q":"370000.00",
				"V":"60.00",
				"Q":"180000.00"
			}
		}
	}`)
	if _, err := client.handleMessage(context.Background(), payload); err != nil {
		t.Fatalf("handleMessage() error = %v", err)
	}
	if aggregator.last == nil {
		t.Fatal("aggregator last = nil, want closed 1m kline")
	}
}

type mockAggregator struct {
	last *market.Kline
}

// OnKline 验证 websocket 在未配置 Kafka producer 时仍能把 1m 闭 K 线喂给进程内聚合链路。
func (m *mockAggregator) OnKline(_ context.Context, k *market.Kline) {
	m.last = k
}
