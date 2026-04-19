package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Binance 期货合约 API 客户端（公共包）
//
// 从 execution/internal/exchange 提取，供多个微服务共享
// API 文档: https://binance-docs.github.io/apidocs/futures/cn/
// 基础端点: https://demo-fapi.binance.com (测试网)
// ---------------------------------------------------------------------------

// Client 币安期货 API 客户端
type Client struct {
	baseURL              string
	apiKey               string
	secretKey            string
	client               *http.Client
	precisionMu          sync.RWMutex
	pricePrecisionMap    map[string]int
	quantityPrecisionMap map[string]int
}

// NewClient 创建币安期货客户端
// baseURL 为空时默认使用正式环境 https://fapi.binance.com
func NewClient(baseURL, apiKey, secretKey, proxyURL string) *Client {
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	transport := &http.Transport{}
	if proxyURL != "" {
		if proxy, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxy)
		}
	}
	return &Client{
		baseURL:   strings.TrimRight(baseURL, "/"),
		apiKey:    apiKey,
		secretKey: secretKey,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		pricePrecisionMap:    make(map[string]int),
		quantityPrecisionMap: make(map[string]int),
	}
}

// ---------------------------------------------------------------------------
// HTTP 请求基础设施
// ---------------------------------------------------------------------------

// signedQuery 生成带 HMAC-SHA256 签名的查询字符串
func (c *Client) signedQuery(query url.Values) (string, error) {
	query.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	encoded := query.Encode()
	mac := hmac.New(sha256.New, []byte(c.secretKey))
	_, _ = mac.Write([]byte(encoded))
	signature := hex.EncodeToString(mac.Sum(nil))
	return encoded + "&signature=" + url.QueryEscape(signature), nil
}

// doSignedRequest 执行带签名的 HTTP 请求
func (c *Client) doSignedRequest(ctx context.Context, method, path string, query url.Values, out any) error {
	sq, err := c.signedQuery(query)
	if err != nil {
		return err
	}
	endpoint := c.baseURL + path + "?" + sq

	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return err
	}
	if c.apiKey != "" {
		req.Header.Set("X-MBX-APIKEY", c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("binance request failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ---------------------------------------------------------------------------
// 订单相关 API
// ---------------------------------------------------------------------------

// OrderResponse 币安期货下单 API 响应结构
type OrderResponse struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"` // NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"updateTime"`
}

// CancelResponse 币安取消订单 API 响应结构
type CancelResponse struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"updateTime"`
}

// CreateOrder 创建订单
func (c *Client) CreateOrder(ctx context.Context, symbol, side, positionSide, orderType string,
	quantity, price, stopPrice float64, reduceOnly, closePosition bool, clientID string) (*OrderResponse, error) {
	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	q.Set("side", side)
	q.Set("positionSide", positionSide)

	if orderType == "" {
		orderType = "MARKET"
	}
	q.Set("type", orderType)
	q.Set("quantity", fmt.Sprintf("%.6f", quantity))

	if orderType == "LIMIT" {
		if price <= 0 {
			return nil, fmt.Errorf("limit order requires price > 0")
		}
		q.Set("price", fmt.Sprintf("%.2f", price))
		q.Set("timeInForce", "GTC")
	}

	if orderType == "STOP" {
		if stopPrice <= 0 {
			return nil, fmt.Errorf("stop order requires stop_price > 0")
		}
		q.Set("stopPrice", fmt.Sprintf("%.2f", stopPrice))
		q.Set("timeInForce", "GTC")
		if price > 0 {
			q.Set("price", fmt.Sprintf("%.2f", price))
		}
	}

	if reduceOnly {
		q.Set("reduceOnly", "true")
	}
	if closePosition {
		q.Set("closePosition", "true")
		q.Del("quantity")
	}
	if clientID != "" {
		q.Set("newClientOrderId", clientID)
	}
	q.Set("recvWindow", "5000")

	var resp OrderResponse
	if err := c.doSignedRequest(ctx, http.MethodPost, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CancelOrder 取消订单
func (c *Client) CancelOrder(ctx context.Context, symbol, orderID, clientOrderID string) (*CancelResponse, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if orderID == "" && clientOrderID == "" {
		return nil, fmt.Errorf("order_id or client_order_id is required")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	if orderID != "" {
		q.Set("orderId", orderID)
	}
	if clientOrderID != "" {
		q.Set("origClientOrderId", clientOrderID)
	}
	q.Set("recvWindow", "5000")

	var resp CancelResponse
	if err := c.doSignedRequest(ctx, http.MethodDelete, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// QueryOrder 查询订单状态
func (c *Client) QueryOrder(ctx context.Context, symbol, orderID, clientOrderID string) (*OrderResponse, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if orderID == "" && clientOrderID == "" {
		return nil, fmt.Errorf("order_id or client_order_id is required")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	if orderID != "" {
		q.Set("orderId", orderID)
	}
	if clientOrderID != "" {
		q.Set("origClientOrderId", clientOrderID)
	}
	q.Set("recvWindow", "5000")

	var resp OrderResponse
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ---------------------------------------------------------------------------
// 账户信息 API
// ---------------------------------------------------------------------------

// AccountResponse 币安期货账户 API 响应结构
type AccountResponse struct {
	TotalWalletBalance string         `json:"totalWalletBalance"`
	TotalUnrealizedPNL string         `json:"totalUnrealizedProfit"`
	TotalMarginBalance string         `json:"totalMarginBalance"`
	AvailableBalance   string         `json:"availableBalance"`
	MaxWithdrawAmount  string         `json:"maxWithdrawAmount"`
	Positions          []PositionResp `json:"positions"`
}

// PositionResp 币安期货仓位信息
type PositionResp struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	UnrealizedProfit string `json:"unrealizedProfit"`
	LiquidationPrice string `json:"liquidationPrice"`
	Leverage         string `json:"leverage"`
	MarginType       string `json:"marginType"`
}

// GetAccountInfo 获取账户信息（含全部持仓）
func (c *Client) GetAccountInfo(ctx context.Context) (*AccountResponse, error) {
	q := url.Values{}
	q.Set("recvWindow", "5000")

	var resp AccountResponse
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v2/account", q, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ---------------------------------------------------------------------------
// 合约查询 API（当前委托、历史委托、历史成交、资金流水、资金费用）
// ---------------------------------------------------------------------------

// OpenOrder 当前委托条目
type OpenOrder struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
	ReduceOnly    bool   `json:"reduceOnly"`
	ClosePosition bool   `json:"closePosition"`
	TimeInForce   string `json:"timeInForce"`
}

// GetOpenOrders 查询当前委托（未成交订单）
// symbol 为空时查询所有交易对的当前委托
func (c *Client) GetOpenOrders(ctx context.Context, symbol string) ([]OpenOrder, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(symbol))
	}
	q.Set("recvWindow", "5000")

	var resp []OpenOrder
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/openOrders", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// AllOrder 历史委托条目
type AllOrder struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
	ReduceOnly    bool   `json:"reduceOnly"`
	ClosePosition bool   `json:"closePosition"`
	TimeInForce   string `json:"timeInForce"`
}

const (
	fallbackPricePrecision    = 2
	fallbackQuantityPrecision = 3
)

type exchangeInfoResponse struct {
	Symbols []struct {
		Symbol            string `json:"symbol"`
		PricePrecision    int    `json:"pricePrecision"`
		QuantityPrecision int    `json:"quantityPrecision"`
	} `json:"symbols"`
}

func (c *Client) GetSymbolPrecisions(ctx context.Context, symbol string) (int, int, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return fallbackPricePrecision, fallbackQuantityPrecision, fmt.Errorf("symbol is required")
	}

	c.precisionMu.RLock()
	pricePrecision, priceOK := c.pricePrecisionMap[symbol]
	quantityPrecision, qtyOK := c.quantityPrecisionMap[symbol]
	c.precisionMu.RUnlock()
	if priceOK && qtyOK {
		return pricePrecision, quantityPrecision, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/fapi/v1/exchangeInfo", nil)
	if err != nil {
		return fallbackPricePrecision, fallbackQuantityPrecision, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fallbackPricePrecision, fallbackQuantityPrecision, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fallbackPricePrecision, fallbackQuantityPrecision, fmt.Errorf("binance exchangeInfo failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var info exchangeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fallbackPricePrecision, fallbackQuantityPrecision, err
	}
	for _, item := range info.Symbols {
		if strings.EqualFold(item.Symbol, symbol) {
			c.precisionMu.Lock()
			c.pricePrecisionMap[symbol] = item.PricePrecision
			c.quantityPrecisionMap[symbol] = item.QuantityPrecision
			c.precisionMu.Unlock()
			return item.PricePrecision, item.QuantityPrecision, nil
		}
	}

	return fallbackPricePrecision, fallbackQuantityPrecision, fmt.Errorf("symbol %s not found in exchangeInfo", symbol)
}

// GetAllOrders 查询历史委托（所有订单）
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认500
func (c *Client) GetAllOrders(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]AllOrder, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required for allOrders")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("recvWindow", "5000")

	var resp []AllOrder
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/allOrders", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// UserTrade 历史成交条目
type UserTrade struct {
	ID              int64  `json:"id"`
	Symbol          string `json:"symbol"`
	OrderID         int64  `json:"orderId"`
	Side            string `json:"side"`
	PositionSide    string `json:"positionSide"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	RealizedPnl     string `json:"realizedPnl"`
	MarginAsset     string `json:"marginAsset"`
	QuoteQty        string `json:"quoteQty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
	Buyer           bool   `json:"buyer"`
	Maker           bool   `json:"maker"`
}

// GetUserTrades 查询历史成交
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认500
func (c *Client) GetUserTrades(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]UserTrade, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required for userTrades")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("recvWindow", "5000")

	var resp []UserTrade
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/userTrades", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Income 资金流水条目
type Income struct {
	Symbol     string `json:"symbol"`
	IncomeType string `json:"incomeType"` // TRANSFER, WELCOME_BONUS, REALIZED_PNL, FUNDING_FEE, COMMISSION, etc.
	Income     string `json:"income"`
	Asset      string `json:"asset"`
	Time       int64  `json:"time"`
	TranID     int64  `json:"tranId"`
	TradeID    string `json:"tradeId"`
}

// GetIncomeHistory 查询资金流水
// incomeType 过滤收入类型，空字符串表示所有类型
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *Client) GetIncomeHistory(ctx context.Context, symbol, incomeType string, startTime, endTime int64, limit int) ([]Income, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(symbol))
	}
	if incomeType != "" {
		q.Set("incomeType", incomeType)
	}
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("recvWindow", "5000")

	var resp []Income
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/income", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// FundingRate 资金费率条目
type FundingRate struct {
	Symbol      string `json:"symbol"`
	FundingRate string `json:"fundingRate"`
	FundingTime int64  `json:"fundingTime"`
	MarkPrice   string `json:"markPrice"`
}

// GetFundingRate 查询资金费率历史
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *Client) GetFundingRate(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]FundingRate, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(symbol))
	}
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}

	var resp []FundingRate
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/fundingRate", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// FundingFee 资金费用条目（从资金流水中提取 FUNDING_FEE 类型）
type FundingFee struct {
	Symbol     string `json:"symbol"`
	IncomeType string `json:"incomeType"`
	Income     string `json:"income"`
	Asset      string `json:"asset"`
	Time       int64  `json:"time"`
	TranID     int64  `json:"tranId"`
	TradeID    string `json:"tradeId"`
}

// GetFundingFees 查询资金费用（从资金流水中筛选 FUNDING_FEE 类型）
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *Client) GetFundingFees(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]FundingFee, error) {
	incomes, err := c.GetIncomeHistory(ctx, symbol, "FUNDING_FEE", startTime, endTime, limit)
	if err != nil {
		return nil, err
	}

	fees := make([]FundingFee, 0, len(incomes))
	for _, inc := range incomes {
		fees = append(fees, FundingFee{
			Symbol:     inc.Symbol,
			IncomeType: inc.IncomeType,
			Income:     inc.Income,
			Asset:      inc.Asset,
			Time:       inc.Time,
			TranID:     inc.TranID,
			TradeID:    inc.TradeID,
		})
	}
	return fees, nil
}

// parseFloat 安全解析浮点数字符串
func parseFloat(v string) float64 {
	f, _ := strconv.ParseFloat(v, 64)
	return f
}
