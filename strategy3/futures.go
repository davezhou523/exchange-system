package strategy3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type FuturesRESTClient struct {
	BaseURL    string
	HTTPClient *http.Client
	APIKey     string
	SecretKey  string
}

func NewBinanceFuturesRESTClient() *FuturesRESTClient {
	return &FuturesRESTClient{
		BaseURL: "https://fapi.binance.com",
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *FuturesRESTClient) FetchCandles(ctx context.Context, symbol, interval string, limit int) ([]Candle, error) {
	if limit <= 0 {
		limit = 100
	}
	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}

	endpoint, err := url.Parse(baseURL + "/fapi/v1/klines")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("symbol", strings.ToUpper(symbol))
	query.Set("interval", interval)
	query.Set("limit", strconv.Itoa(limit))
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	var resp *http.Response
	maxRetries := 3
	retryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		resp, err = httpClient.Do(req)
		if err == nil {
			break
		}
		// 如果是临时性网络错误且还有重试机会，则继续重试
		if isTemporaryError(err) && i < maxRetries-1 {
			continue
		}
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("binance futures klines request failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var payload [][]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	candles := make([]Candle, 0, len(payload))
	for _, row := range payload {
		if len(row) < 7 {
			return nil, fmt.Errorf("unexpected kline payload length: %d", len(row))
		}
		openTime, err := toInt64(row[0])
		if err != nil {
			return nil, err
		}
		open, err := toFloat(row[1])
		if err != nil {
			return nil, err
		}
		high, err := toFloat(row[2])
		if err != nil {
			return nil, err
		}
		low, err := toFloat(row[3])
		if err != nil {
			return nil, err
		}
		closePrice, err := toFloat(row[4])
		if err != nil {
			return nil, err
		}
		volume, err := toFloat(row[5])
		if err != nil {
			return nil, err
		}
		closeTime, err := toInt64(row[6])
		if err != nil {
			return nil, err
		}

		candles = append(candles, Candle{
			OpenTime:  time.UnixMilli(openTime).UTC(),
			CloseTime: time.UnixMilli(closeTime).UTC(),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closePrice,
			Volume:    volume,
			Closed:    true,
		})
	}

	return candles, nil
}

// --- 账户和订单API ---

// GetServerTime 获取Binance服务器时间
func (c *FuturesRESTClient) GetServerTime(ctx context.Context) (int64, error) {
	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	endpoint := fmt.Sprintf("%s/fapi/v1/time", baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("get server time failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var result struct {
		ServerTime int64 `json:"serverTime"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	return result.ServerTime, nil
}

// signRequest 对查询参数进行HMAC SHA256签名
func (c *FuturesRESTClient) signRequest(query url.Values) string {
	// 获取服务器时间以避免时间偏差问题
	serverTime, err := c.GetServerTime(context.Background())
	var timestamp int64
	if err == nil && serverTime > 0 {
		timestamp = serverTime
		log.Printf("[时间同步] 使用服务器时间: %d", timestamp)
	} else {
		timestamp = time.Now().UnixMilli()
		log.Printf("[时间同步] 使用本地时间: %d (服务器时间获取失败: %v)", timestamp, err)
	}

	query.Set("timestamp", strconv.FormatInt(timestamp, 10))

	// 手动构建查询字符串（按字母顺序排序）
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf strings.Builder
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(k)
		buf.WriteByte('=')
		buf.WriteString(query.Get(k))
	}
	queryString := buf.String()

	// 计算HMAC SHA256签名
	mac := hmac.New(sha256.New, []byte(c.SecretKey))
	mac.Write([]byte(queryString))
	signature := hex.EncodeToString(mac.Sum(nil))

	// 调试日志
	log.Printf("[签名调试] 时间戳=%s, 查询字符串=%s, 签名=%s", strconv.FormatInt(timestamp, 10), queryString, signature)

	// 将签名添加到查询参数（按字母顺序）
	query.Set("signature", signature)

	// 重新按字母顺序构建完整的查询字符串
	keys = make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var finalBuf strings.Builder
	for i, k := range keys {
		if i > 0 {
			finalBuf.WriteByte('&')
		}
		finalBuf.WriteString(k)
		finalBuf.WriteByte('=')
		finalBuf.WriteString(query.Get(k))
	}

	return finalBuf.String()
}

// doSignedRequest 执行需要签名的HTTP请求
func (c *FuturesRESTClient) doSignedRequest(ctx context.Context, method, path string, query url.Values) (*http.Response, error) {
	if c.APIKey == "" || c.SecretKey == "" {
		return nil, fmt.Errorf("API key and secret key are required for signed requests")
	}

	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	// 签名请求
	signedQuery := c.signRequest(query)
	endpoint := fmt.Sprintf("%s%s?%s", baseURL, path, signedQuery)

	log.Printf("[请求调试] 目标URL: %s", endpoint)

	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", c.APIKey)
	log.Printf("[请求调试] 设置API密钥头: %s...", c.APIKey[:8])

	// 重试逻辑
	var resp *http.Response
	maxRetries := 3
	retryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		resp, err = httpClient.Do(req)
		if err == nil {
			break
		}
		if isTemporaryError(err) && i < maxRetries-1 {
			continue
		}
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		return nil, fmt.Errorf("binance signed request failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	return resp, nil
}

// AccountInfo 表示Binance合约账户信息
type AccountInfo struct {
	Assets                      []Asset           `json:"assets"`
	Positions                   []BinancePosition `json:"positions"`
	TotalWalletBalance          string            `json:"totalWalletBalance"`
	TotalUnrealizedProfit       string            `json:"totalUnrealizedProfit"`
	TotalMarginBalance          string            `json:"totalMarginBalance"`
	TotalInitialMargin          string            `json:"totalInitialMargin"`
	TotalMaintMargin            string            `json:"totalMaintMargin"`
	TotalPositionInitialMargin  string            `json:"totalPositionInitialMargin"`
	TotalOpenOrderInitialMargin string            `json:"totalOpenOrderInitialMargin"`
	TotalCrossWalletBalance     string            `json:"totalCrossWalletBalance"`
	TotalCrossUnPnl             string            `json:"totalCrossUnPnl"`
	AvailableBalance            string            `json:"availableBalance"`
	MaxWithdrawAmount           string            `json:"maxWithdrawAmount"`
}

// Asset 表示账户资产
type Asset struct {
	Asset                  string `json:"asset"`
	WalletBalance          string `json:"walletBalance"`
	UnrealizedProfit       string `json:"unrealizedProfit"`
	MarginBalance          string `json:"marginBalance"`
	MaintMargin            string `json:"maintMargin"`
	InitialMargin          string `json:"initialMargin"`
	PositionInitialMargin  string `json:"positionInitialMargin"`
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
	CrossWalletBalance     string `json:"crossWalletBalance"`
	CrossUnPnl             string `json:"crossUnPnl"`
	AvailableBalance       string `json:"availableBalance"`
	MaxWithdrawAmount      string `json:"maxWithdrawAmount"`
}

// BinancePosition 表示合约持仓
type BinancePosition struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	UnrealizedProfit string `json:"unrealizedProfit"`
	LiquidationPrice string `json:"liquidationPrice"`
	Leverage         string `json:"leverage"`
	MaxNotionalValue string `json:"maxNotionalValue"`
	MarginType       string `json:"marginType"`
	IsolatedMargin   string `json:"isolatedMargin"`
	IsAutoAddMargin  string `json:"isAutoAddMargin"`
	PositionSide     string `json:"positionSide"`
	Notional         string `json:"notional"`
	IsolatedWallet   string `json:"isolatedWallet"`
}

// OrderInfo 表示Binance合约订单信息
type OrderInfo struct {
	OrderId       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	ClientOrderId string `json:"clientOrderId"`
	Price         string `json:"price"`
	AvgPrice      string `json:"avgPrice"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	CumQuote      string `json:"cumQuote"`
	TimeInForce   string `json:"timeInForce"`
	Type          string `json:"type"`
	ReduceOnly    bool   `json:"reduceOnly"`
	ClosePosition bool   `json:"closePosition"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	StopPrice     string `json:"stopPrice"`
	WorkingType   string `json:"workingType"`
	PriceProtect  bool   `json:"priceProtect"`
	OrigType      string `json:"origType"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
}

// GetAccountInfo 获取合约账户信息
func (c *FuturesRESTClient) GetAccountInfo(ctx context.Context) (*AccountInfo, error) {
	query := url.Values{}
	query.Set("recvWindow", "30000")
	resp, err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v2/account", query)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var accountInfo AccountInfo
	if err := json.NewDecoder(resp.Body).Decode(&accountInfo); err != nil {
		return nil, err
	}
	return &accountInfo, nil
}

// GetOrders 获取合约订单历史
func (c *FuturesRESTClient) GetOrders(ctx context.Context, symbol string, limit int) ([]OrderInfo, error) {
	query := url.Values{}
	query.Set("symbol", strings.ToUpper(symbol))
	query.Set("recvWindow", "30000")
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}

	resp, err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/allOrders", query)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var orders []OrderInfo
	if err := json.NewDecoder(resp.Body).Decode(&orders); err != nil {
		return nil, err
	}
	return orders, nil
}

type StreamEvent struct {
	Symbol   string
	Interval string
	Candle   Candle
}

type FuturesWebSocketClient struct {
	BaseURL string
	Dialer  *websocket.Dialer
}

func NewBinanceFuturesWebSocketClient() *FuturesWebSocketClient {
	return &FuturesWebSocketClient{
		BaseURL: "wss://fstream.binance.com",
		Dialer:  websocket.DefaultDialer,
	}
}

func (c *FuturesWebSocketClient) SubscribeKlines(ctx context.Context, symbol string, intervals []string, handler func(StreamEvent) error) error {
	if len(intervals) == 0 {
		return fmt.Errorf("intervals cannot be empty")
	}
	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = "wss://fstream.binance.com"
	}
	dialer := c.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	streams := make([]string, 0, len(intervals))
	streamSymbol := strings.ToLower(symbol)
	for _, interval := range intervals {
		streams = append(streams, fmt.Sprintf("%s@kline_%s", streamSymbol, interval))
	}
	target := fmt.Sprintf("%s/stream?streams=%s", strings.TrimRight(baseURL, "/"), strings.Join(streams, "/"))

	const initialRetryDelay = 2 * time.Second
	const maxRetryDelay = 30 * time.Second
	retryDelay := initialRetryDelay

	for {
		// 连接阶段
		conn, _, err := dialer.DialContext(ctx, target, nil)
		if err != nil {
			// 连接失败，等待后重试
			select {
			case <-time.After(retryDelay):
				// 指数退避
				retryDelay *= 2
				if retryDelay > maxRetryDelay {
					retryDelay = maxRetryDelay
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// 连接成功，重置重试延迟
		retryDelay = initialRetryDelay

		// 设置ping/pong处理程序以保持连接活跃
		conn.SetPingHandler(func(appData string) error {
			err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
			if err != nil {
				conn.Close()
			}
			return err
		})

		if deadline, ok := ctx.Deadline(); ok {
			_ = conn.SetReadDeadline(deadline)
		}

		go func() {
			<-ctx.Done()
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"), time.Now().Add(time.Second))
		}()

		// 消息循环
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// 连接错误，跳出消息循环，进行重连
				break
			}
			// 检查是否是错误消息
			var msg map[string]interface{}
			if err := json.Unmarshal(payload, &msg); err == nil {
				if _, ok := msg["error"].(map[string]interface{}); ok {
					// 服务器返回错误，记录并断开连接
					conn.Close()
					break
				}
				// 忽略订阅成功等控制消息
				if _, ok := msg["result"]; ok {
					continue
				}
			}
			event, err := decodeFuturesStreamEvent(payload)
			if err != nil {
				// 解码错误不影响连接，继续处理下一条消息
				continue
			}
			if handler != nil {
				if err := handler(event); err != nil {
					// 处理器错误，返回错误，不重连
					conn.Close()
					return err
				}
			}
		}
		// 连接断开，等待后重连
		select {
		case <-time.After(retryDelay):
			retryDelay *= 2
			if retryDelay > maxRetryDelay {
				retryDelay = maxRetryDelay
			}
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type futuresStreamEnvelope struct {
	Stream string           `json:"stream"`
	Data   futuresKlineData `json:"data"`
}

type futuresKlineData struct {
	Symbol string             `json:"s"`
	Kline  futuresKlineDetail `json:"k"`
}

type futuresKlineDetail struct {
	OpenTime  int64       `json:"t"`
	CloseTime int64       `json:"T"`
	Interval  string      `json:"i"`
	Open      json.Number `json:"o"`
	Close     json.Number `json:"c"`
	High      json.Number `json:"h"`
	Low       json.Number `json:"l"`
	Volume    json.Number `json:"v"`
	Closed    bool        `json:"x"`
}

func decodeFuturesStreamEvent(payload []byte) (StreamEvent, error) {
	var envelope futuresStreamEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return StreamEvent{}, err
	}
	open, err := toFloat(envelope.Data.Kline.Open)
	if err != nil {
		return StreamEvent{}, err
	}
	closePrice, err := toFloat(envelope.Data.Kline.Close)
	if err != nil {
		return StreamEvent{}, err
	}
	high, err := toFloat(envelope.Data.Kline.High)
	if err != nil {
		return StreamEvent{}, err
	}
	low, err := toFloat(envelope.Data.Kline.Low)
	if err != nil {
		return StreamEvent{}, err
	}
	volume, err := toFloat(envelope.Data.Kline.Volume)
	if err != nil {
		return StreamEvent{}, err
	}

	return StreamEvent{
		Symbol:   envelope.Data.Symbol,
		Interval: envelope.Data.Kline.Interval,
		Candle: Candle{
			OpenTime:  time.UnixMilli(envelope.Data.Kline.OpenTime).UTC(),
			CloseTime: time.UnixMilli(envelope.Data.Kline.CloseTime).UTC(),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closePrice,
			Volume:    volume,
			Closed:    envelope.Data.Kline.Closed,
		},
	}, nil
}

func isTemporaryError(err error) bool {
	if err == nil {
		return false
	}
	// 检查是否为网络错误
	if netErr, ok := err.(net.Error); ok {
		return netErr.Temporary() || netErr.Timeout()
	}
	// 检查错误字符串中是否包含超时相关关键词
	errStr := err.Error()
	return strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") || strings.Contains(errStr, "connection refused")
}
