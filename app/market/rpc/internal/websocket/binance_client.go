package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"exchange-system/common/pb/market"

	"github.com/gorilla/websocket"
	"golang.org/x/net/proxy"
)

type BinanceWebSocketClient struct {
	mu            sync.RWMutex
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

type wsMessageStats struct {
	isKline    bool
	isClosed1m bool
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
		proxyCfg, err := url.Parse(proxyURL)
		if err != nil {
			log.Printf("Invalid proxy URL %s: %v, connecting without proxy", proxyURL, err)
		} else {
			configureDialerProxy(dialer, proxyCfg)
		}
	}
	if !hasKafkaProducer(depthProducer) {
		depthProducer = nil
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

func hasKafkaProducer(p KafkaProducer) bool {
	if p == nil {
		return false
	}
	v := reflect.ValueOf(p)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !v.IsNil()
	default:
		return true
	}
}

func httpProxyFunc(proxy *url.URL) func(*http.Request) (*url.URL, error) {
	return func(_ *http.Request) (*url.URL, error) {
		return proxy, nil
	}
}

func configureDialerProxy(dialer *websocket.Dialer, proxyCfg *url.URL) {
	if dialer == nil || proxyCfg == nil {
		return
	}
	switch strings.ToLower(proxyCfg.Scheme) {
	case "socks5", "socks5h":
		base := &net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}
		socksDialer, err := proxy.FromURL(proxyCfg, base)
		if err != nil {
			log.Printf("Invalid SOCKS5 proxy %s: %v, connecting without proxy", proxyCfg.String(), err)
			return
		}
		dialer.Proxy = nil
		dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if contextDialer, ok := socksDialer.(proxy.ContextDialer); ok {
				return contextDialer.DialContext(ctx, network, addr)
			}
			return socksDialer.Dial(network, addr)
		}
		log.Printf("Using SOCKS5 proxy dialer: %s", proxyCfg.String())
	default:
		dialer.Proxy = httpProxyFunc(proxyCfg)
		log.Printf("Using HTTP proxy: %s", proxyCfg.String())
	}
}

// buildStreamURL 根据当前 symbol 和 interval 快照构建 Binance 多路复用流地址。
func (c *BinanceWebSocketClient) buildStreamURL() string {
	c.mu.RLock()
	symbols := append([]string(nil), c.symbols...)
	intervals := append([]string(nil), c.intervals...)
	depthProducer := c.depthProducer
	c.mu.RUnlock()

	streams := make([]string, 0)
	for _, symbol := range symbols {
		for _, interval := range intervals {
			streams = append(streams, fmt.Sprintf("%s@kline_%s", strings.ToLower(symbol), interval))
		}
		if hasKafkaProducer(depthProducer) {
			streams = append(streams, fmt.Sprintf("%s@depth20@100ms", strings.ToLower(symbol)))
		}
	}
	return fmt.Sprintf("%s/stream?streams=%s", strings.TrimRight(c.baseURL, "/"), strings.Join(streams, "/"))
}

// Connect 建立到 Binance 多路复用 WebSocket 的连接。
func (c *BinanceWebSocketClient) Connect(ctx context.Context) error {
	target := c.buildStreamURL()
	log.Printf("Connecting to Binance WebSocket: %s", target)

	conn, _, err := c.dialer.DialContext(ctx, target, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %v", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	const readWait = 75 * time.Second
	conn.SetPingHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})
	if err := conn.SetReadDeadline(time.Now().Add(readWait)); err != nil {
		log.Printf("[ws debug] set initial read deadline failed: %v", err)
	}
	conn.SetPongHandler(func(appData string) error {
		if err := conn.SetReadDeadline(time.Now().Add(readWait)); err != nil {
			log.Printf("[ws debug] extend read deadline on pong failed: %v", err)
			return err
		}
		log.Printf("[ws debug] pong received payload=%q", appData)
		return nil
	})

	log.Printf("WebSocket connected successfully")
	log.Printf("[ws debug] read loop armed read_wait=%s", readWait)
	return nil
}

// StartInBackground 在后台维持 WebSocket 连接，并在断开后自动重连。
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

// readLoop 持续读取 WebSocket 消息，直到连接断开或上下文取消。
func (c *BinanceWebSocketClient) readLoop(ctx context.Context) {
	const pingInterval = 20 * time.Second
	var totalMessages atomic.Int64
	var klineMessages atomic.Int64
	var closed1mMessages atomic.Int64
	connectedAt := time.Now().UTC()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				log.Printf("[ws stats] total_messages=%d kline_messages=%d closed_1m_messages=%d",
					totalMessages.Swap(0), klineMessages.Swap(0), closed1mMessages.Swap(0))
			}
		}
	}()
	defer close(done)

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if totalMessages.Load() == 0 {
					log.Printf("[ws debug] no messages yet elapsed=%s", time.Since(connectedAt).Round(time.Second))
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				conn := c.currentConn()
				if conn == nil {
					return
				}
				if err := conn.WriteControl(websocket.PingMessage, []byte("ws-health"), time.Now().Add(3*time.Second)); err != nil {
					log.Printf("[ws debug] ping failed: %v", err)
					return
				}
				log.Printf("[ws debug] ping sent")
			}
		}
	}()

	for {
		conn := c.currentConn()
		if conn == nil {
			return
		}
		select {
		case <-ctx.Done():
			_ = conn.Close()
			return
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			var closeErr *websocket.CloseError
			switch {
			case errors.As(err, &closeErr):
				log.Printf("WebSocket read error: close_code=%d text=%s", closeErr.Code, closeErr.Text)
			case isNetTimeout(err):
				log.Printf("WebSocket read error: timeout waiting for message elapsed=%s", time.Since(connectedAt).Round(time.Second))
			default:
				log.Printf("WebSocket read error: %v", err)
			}
			_ = conn.Close()
			return
		}
		if totalMessages.Add(1) == 1 {
			log.Printf("[ws] first message received")
		}

		stats, err := c.handleMessage(ctx, message)
		if stats.isKline {
			klineMessages.Add(1)
		}
		if stats.isClosed1m {
			closed1mMessages.Add(1)
		}
		if err != nil {
			log.Printf("Error handling message: %v", err)
		}
	}
}

func isNetTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (c *BinanceWebSocketClient) handleMessage(ctx context.Context, message []byte) (wsMessageStats, error) {
	stats := wsMessageStats{}
	var envelope StreamEnvelope
	if err := json.Unmarshal(message, &envelope); err != nil {
		return stats, fmt.Errorf("failed to unmarshal message: %v", err)
	}

	if strings.Contains(envelope.Stream, "kline") {
		stats.isKline = true
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
			if k.Interval == "1m" {
				stats.isClosed1m = true
			}
			log.Printf("[%s kline] %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f QV=%.4f trades=%d closed=%v",
				k.Interval, k.Symbol, openTime, closeTime, k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume, k.NumTrades, k.IsClosed)

			// 不再写入根级别日志（klineLogDir/SYMBOL/YYYY-MM-DD.jsonl）
			// 1m K线经 aggregator 指标计算后会写入 klineLogDir/SYMBOL/1m/YYYY-MM-DD.jsonl，避免重复

			if err := c.producer.SendMarketData(ctx, &k); err != nil {
				return stats, fmt.Errorf("failed to send kline to Kafka: %v", err)
			}
			if c.aggregator != nil {
				c.aggregator.OnKline(ctx, &k)
			}
		}
	}
	if strings.Contains(envelope.Stream, "@depth") && hasKafkaProducer(c.depthProducer) {
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
				return stats, fmt.Errorf("failed to send depth to Kafka: %v", err)
			}
		}
	}

	return stats, nil
}

// CurrentSymbols 返回当前正在使用的订阅 symbol 集合快照。
func (c *BinanceWebSocketClient) CurrentSymbols() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string(nil), c.symbols...)
}

// UpdateSymbols 更新订阅 symbol 集合，并通过关闭当前连接触发后台 loop 按新集合重连。
func (c *BinanceWebSocketClient) UpdateSymbols(symbols []string) error {
	if c == nil {
		return nil
	}
	next := append([]string(nil), symbols...)
	c.mu.Lock()
	c.symbols = next
	conn := c.conn
	c.mu.Unlock()

	log.Printf("Binance symbols updated: %v", next)
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// currentConn 返回当前连接快照，避免读循环与更新逻辑直接竞争字段。
func (c *BinanceWebSocketClient) currentConn() *websocket.Conn {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// Close 主动关闭当前 WebSocket 连接。
func (c *BinanceWebSocketClient) Close() error {
	conn := c.currentConn()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
