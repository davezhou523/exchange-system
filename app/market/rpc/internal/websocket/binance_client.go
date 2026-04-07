package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type BinanceWebSocketClient struct {
	baseURL   string
	dialer    *websocket.Dialer
	conn      *websocket.Conn
	producer  KafkaProducer
	symbols   []string
	intervals []string
}

type KafkaProducer interface {
	SendMarketData(ctx context.Context, data interface{}) error
}

type KlineData struct {
	Symbol    string  `json:"s"`
	Interval  string  `json:"i"`
	OpenTime  int64   `json:"t"`
	CloseTime int64   `json:"T"`
	Open      float64 `json:"o,string"`
	High      float64 `json:"h,string"`
	Low       float64 `json:"l,string"`
	Close     float64 `json:"c,string"`
	Volume    float64 `json:"v,string"`
	Closed    bool    `json:"x"`
}

type StreamEnvelope struct {
	Stream string    `json:"stream"`
	Data   KlineData `json:"data"`
}

func NewBinanceWebSocketClient(baseURL string, symbols, intervals []string, producer KafkaProducer) *BinanceWebSocketClient {
	return &BinanceWebSocketClient{
		baseURL:   baseURL,
		dialer:    websocket.DefaultDialer,
		producer:  producer,
		symbols:   symbols,
		intervals: intervals,
	}
}

func (c *BinanceWebSocketClient) Connect(ctx context.Context) error {
	streams := make([]string, 0)
	for _, symbol := range c.symbols {
		for _, interval := range c.intervals {
			streams = append(streams, fmt.Sprintf("%s@kline_%s", strings.ToLower(symbol), interval))
		}
	}

	target := fmt.Sprintf("%s/stream?streams=%s", strings.TrimRight(c.baseURL, "/"), strings.Join(streams, "/"))

	log.Printf("Connecting to Binance WebSocket: %s", target)

	conn, _, err := c.dialer.DialContext(ctx, target, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %v", err)
	}

	c.conn = conn

	conn.SetPingHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})

	return nil
}

func (c *BinanceWebSocketClient) StartStreaming(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("WebSocket not connected")
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = c.conn.Close()
				return
			default:
				_, message, err := c.conn.ReadMessage()
				if err != nil {
					log.Printf("WebSocket read error: %v", err)
					time.Sleep(5 * time.Second)
					if err := c.reconnect(ctx); err != nil {
						log.Printf("Failed to reconnect: %v", err)
					}
					continue
				}

				if err := c.handleMessage(ctx, message); err != nil {
					log.Printf("Error handling message: %v", err)
				}
			}
		}
	}()

	return nil
}

func (c *BinanceWebSocketClient) handleMessage(ctx context.Context, message []byte) error {
	var envelope StreamEnvelope
	if err := json.Unmarshal(message, &envelope); err != nil {
		return fmt.Errorf("failed to unmarshal message: %v", err)
	}

	if strings.Contains(envelope.Stream, "kline") {
		marketData := map[string]interface{}{
			"type":      "kline",
			"symbol":    envelope.Data.Symbol,
			"interval":  envelope.Data.Interval,
			"data":      envelope.Data,
			"timestamp": time.Now().UnixMilli(),
		}

		if err := c.producer.SendMarketData(ctx, marketData); err != nil {
			return fmt.Errorf("failed to send market data to Kafka: %v", err)
		}
	}

	return nil
}

func (c *BinanceWebSocketClient) reconnect(ctx context.Context) error {
	_ = c.conn.Close()
	return c.Connect(ctx)
}

func (c *BinanceWebSocketClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
