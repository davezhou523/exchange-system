package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"exchange-system/common/pb/market"

	"github.com/gorilla/websocket"
)

type BinanceWebSocketClient struct {
	baseURL       string
	proxyURL      string
	dialer        *websocket.Dialer
	conn          *websocket.Conn
	producer      KafkaProducer
	depthProducer KafkaProducer
	symbols       []string
	intervals     []string
	aggregator    KlineAggregator
}

type KlineAggregator interface {
	OnKline(ctx context.Context, k *market.Kline)
}

type KafkaProducer interface {
	SendMarketData(ctx context.Context, data interface{}) error
}

type KlineData struct {
	Symbol         string    `json:"s"`
	Interval       string    `json:"i"`
	OpenTime       int64     `json:"t"`
	CloseTime      int64     `json:"T"`
	Open           flexFloat `json:"o"`
	High           flexFloat `json:"h"`
	Low            flexFloat `json:"l"`
	Close          flexFloat `json:"c"`
	Volume         flexFloat `json:"v"`
	Closed         bool      `json:"x"`
	FirstTradeID   int64     `json:"f"`
	LastTradeID    int64     `json:"L"`
	NumTrades      flexInt   `json:"n"`
	QuoteVolume    flexFloat `json:"q"`
	TakerBuyVolume flexFloat `json:"V"`
	TakerBuyQuote  flexFloat `json:"Q"`
}

// flexFloat can unmarshal from both a JSON string ("1800.00") and a JSON number (1800.00).
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(data []byte) error {
	// Try string first
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		*f = flexFloat(v)
		return nil
	}
	// Fallback to number
	var v float64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*f = flexFloat(v)
	return nil
}

// flexInt can unmarshal from both a JSON string ("123") and a JSON number (123).
type flexInt int64

func (i *flexInt) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		*i = flexInt(v)
		return nil
	}
	var v int64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*i = flexInt(v)
	return nil
}

type StreamEnvelope struct {
	Stream string `json:"stream"`
	Data   struct {
		EventType string           `json:"e"`
		EventTime flexInt          `json:"E"`
		Symbol    string           `json:"s"`
		Kline     KlineData        `json:"k"`
		Bids      []depthLevelData `json:"b"`
		Asks      []depthLevelData `json:"a"`
	} `json:"data"`
}

type depthLevelData struct {
	Price    float64
	Quantity float64
}

func (d *depthLevelData) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) < 2 {
		return fmt.Errorf("invalid depth level length: %d", len(raw))
	}
	var price flexFloat
	if err := json.Unmarshal(raw[0], &price); err != nil {
		return err
	}
	var quantity flexFloat
	if err := json.Unmarshal(raw[1], &quantity); err != nil {
		return err
	}
	d.Price = float64(price)
	d.Quantity = float64(quantity)
	return nil
}

func NewBinanceWebSocketClient(baseURL, proxyURL string, symbols, intervals []string, producer, depthProducer KafkaProducer, aggregator KlineAggregator) *BinanceWebSocketClient {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}

	if proxyURL != "" {
		proxy, err := url.Parse(proxyURL)
		if err != nil {
			log.Printf("Invalid proxy URL %s: %v, connecting without proxy", proxyURL, err)
		} else {
			dialer.Proxy = httpProxyFunc(proxy)
			log.Printf("Using proxy: %s", proxyURL)
		}
	}

	return &BinanceWebSocketClient{
		baseURL:       baseURL,
		proxyURL:      proxyURL,
		dialer:        dialer,
		producer:      producer,
		depthProducer: depthProducer,
		symbols:       symbols,
		intervals:     intervals,
		aggregator:    aggregator,
	}
}

func httpProxyFunc(proxy *url.URL) func(*http.Request) (*url.URL, error) {
	return func(_ *http.Request) (*url.URL, error) {
		return proxy, nil
	}
}

func (c *BinanceWebSocketClient) buildStreamURL() string {
	streams := make([]string, 0)
	for _, symbol := range c.symbols {
		for _, interval := range c.intervals {
			streams = append(streams, fmt.Sprintf("%s@kline_%s", strings.ToLower(symbol), interval))
		}
		if c.depthProducer != nil {
			streams = append(streams, fmt.Sprintf("%s@depth20@100ms", strings.ToLower(symbol)))
		}
	}
	return fmt.Sprintf("%s/stream?streams=%s", strings.TrimRight(c.baseURL, "/"), strings.Join(streams, "/"))
}

func (c *BinanceWebSocketClient) Connect(ctx context.Context) error {
	target := c.buildStreamURL()
	log.Printf("Connecting to Binance WebSocket: %s", target)

	conn, _, err := c.dialer.DialContext(ctx, target, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %v", err)
	}

	c.conn = conn

	conn.SetPingHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})

	log.Printf("WebSocket connected successfully")
	return nil
}

// StartInBackground launches the WebSocket connection and streaming loop in a
// background goroutine. If the initial connection fails it retries
// automatically, so the calling service can start without blocking.
func (c *BinanceWebSocketClient) StartInBackground(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := c.Connect(ctx); err != nil {
				log.Printf("WebSocket connect failed: %v, retrying in 10s...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
					continue
				}
			}

			c.readLoop(ctx) // blocks until error

			// brief pause before reconnecting
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()
}

// readLoop reads messages from the WebSocket connection until an error occurs.
func (c *BinanceWebSocketClient) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			_ = c.conn.Close()
			return
		default:
		}

		_, message, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("WebSocket read error: %v", err)
			_ = c.conn.Close()
			return
		}

		if err := c.handleMessage(ctx, message); err != nil {
			log.Printf("Error handling message: %v", err)
		}
	}
}

func (c *BinanceWebSocketClient) handleMessage(ctx context.Context, message []byte) error {
	var envelope StreamEnvelope
	if err := json.Unmarshal(message, &envelope); err != nil {
		return fmt.Errorf("failed to unmarshal message: %v", err)
	}

	if strings.Contains(envelope.Stream, "kline") {
		kd := envelope.Data.Kline
		k := market.Kline{
			Symbol:         kd.Symbol,
			Interval:       kd.Interval,
			OpenTime:       kd.OpenTime,
			Open:           float64(kd.Open),
			High:           float64(kd.High),
			Low:            float64(kd.Low),
			Close:          float64(kd.Close),
			Volume:         float64(kd.Volume),
			CloseTime:      kd.CloseTime,
			IsClosed:       kd.Closed,
			EventTime:      int64(envelope.Data.EventTime),
			FirstTradeId:   kd.FirstTradeID,
			LastTradeId:    kd.LastTradeID,
			NumTrades:      int32(kd.NumTrades),
			QuoteVolume:    float64(kd.QuoteVolume),
			TakerBuyVolume: float64(kd.TakerBuyVolume),
			TakerBuyQuote:  float64(kd.TakerBuyQuote),
		}

		openTime := time.UnixMilli(k.OpenTime).Format("15:04:05")
		closeTime := time.UnixMilli(k.CloseTime).Format("15:04:05")

		if k.IsClosed {
			log.Printf("[%s kline] %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f QV=%.4f trades=%d closed=%v",
				k.Interval, k.Symbol, openTime, closeTime, k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume, k.NumTrades, k.IsClosed)

			// 不再写入根级别日志（klineLogDir/SYMBOL/YYYY-MM-DD.jsonl）
			// 1m K线经 aggregator 指标计算后会写入 klineLogDir/SYMBOL/1m/YYYY-MM-DD.jsonl，避免重复

			if err := c.producer.SendMarketData(ctx, &k); err != nil {
				return fmt.Errorf("failed to send kline to Kafka: %v", err)
			}
			if c.aggregator != nil {
				c.aggregator.OnKline(ctx, &k)
			}
		}
	}
	if strings.Contains(envelope.Stream, "@depth") && c.depthProducer != nil {
		depth := market.Depth{
			Symbol:    envelope.Data.Symbol,
			Timestamp: int64(envelope.Data.EventTime),
			Bids:      make([]*market.DepthItem, 0, len(envelope.Data.Bids)),
			Asks:      make([]*market.DepthItem, 0, len(envelope.Data.Asks)),
		}
		for _, item := range envelope.Data.Bids {
			if item.Price <= 0 || item.Quantity <= 0 {
				continue
			}
			depth.Bids = append(depth.Bids, &market.DepthItem{
				Price:    item.Price,
				Quantity: item.Quantity,
			})
		}
		for _, item := range envelope.Data.Asks {
			if item.Price <= 0 || item.Quantity <= 0 {
				continue
			}
			depth.Asks = append(depth.Asks, &market.DepthItem{
				Price:    item.Price,
				Quantity: item.Quantity,
			})
		}
		if len(depth.Bids) > 0 && len(depth.Asks) > 0 {
			if err := c.depthProducer.SendMarketData(ctx, &depth); err != nil {
				return fmt.Errorf("failed to send depth to Kafka: %v", err)
			}
		}
	}

	return nil
}

func (c *BinanceWebSocketClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
