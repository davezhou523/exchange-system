package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Binance 期货交易所客户端
// 实现 Exchange 接口
// API 文档: https://binance-docs.github.io/apidocs/futures/cn/
// ---------------------------------------------------------------------------

// BinanceClient 币安期货 API 客户端
type BinanceClient struct {
	baseURL   string
	apiKey    string
	secretKey string
	client    *http.Client

	precisionMu          sync.RWMutex
	quantityPrecisionMap map[string]int
}

// NewBinanceClient 创建币安期货客户端
// baseURL 为空时默认使用正式环境 https://fapi.binance.com
func NewBinanceClient(baseURL, apiKey, secretKey, proxyURL string) *BinanceClient {
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	transport := &http.Transport{}
	if proxyURL != "" {
		if proxy, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxy)
		}
	}
	return &BinanceClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		apiKey:    apiKey,
		secretKey: secretKey,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		quantityPrecisionMap: make(map[string]int),
	}
}

// Name 返回交易所名称
func (c *BinanceClient) Name() string { return "binance" }

// ---------------------------------------------------------------------------
// HTTP 请求基础设施
// ---------------------------------------------------------------------------

// signedQuery 生成带 HMAC-SHA256 签名的查询字符串
func (c *BinanceClient) signedQuery(query url.Values) (string, error) {
	query.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	encoded := query.Encode()
	mac := hmac.New(sha256.New, []byte(c.secretKey))
	_, _ = mac.Write([]byte(encoded))
	signature := hex.EncodeToString(mac.Sum(nil))
	return encoded + "&signature=" + url.QueryEscape(signature), nil
}

// doSignedRequest 执行带签名的 HTTP 请求
func (c *BinanceClient) doSignedRequest(ctx context.Context, method, path string, query url.Values, out any) error {
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

// binanceOrderResponse 币安期货下单 API 响应结构
type binanceOrderResponse struct {
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

// binanceCancelResponse 币安取消订单 API 响应结构
type binanceCancelResponse struct {
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
func (c *BinanceClient) CreateOrder(ctx context.Context, param CreateOrderParam) (*OrderResult, error) {
	normalizedQty, qtyPrecision, err := c.normalizeOrderQuantity(ctx, param.Symbol, param.Quantity)
	if err != nil {
		return nil, err
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(param.Symbol))
	q.Set("side", string(param.Side))
	q.Set("positionSide", string(param.PositionSide))

	// 订单类型，默认市价
	orderType := string(param.Type)
	if orderType == "" {
		orderType = string(OrderTypeMarket)
	}
	q.Set("type", orderType)

	q.Set("quantity", fmt.Sprintf("%.*f", qtyPrecision, normalizedQty))

	// LIMIT 单必须设置价格和 TimeInForce
	if orderType == string(OrderTypeLimit) {
		if param.Price <= 0 {
			return nil, fmt.Errorf("limit order requires price > 0")
		}
		q.Set("price", fmt.Sprintf("%.2f", param.Price))
		q.Set("timeInForce", "GTC")
	}

	// STOP 单需要止损触发价
	if orderType == string(OrderTypeStop) {
		if param.StopPrice <= 0 {
			return nil, fmt.Errorf("stop order requires stop_price > 0")
		}
		q.Set("stopPrice", fmt.Sprintf("%.2f", param.StopPrice))
		q.Set("timeInForce", "GTC")
		if param.Price > 0 {
			q.Set("price", fmt.Sprintf("%.2f", param.Price))
		}
	}

	// 仅减仓
	if param.ReduceOnly && (param.PositionSide == "" || param.PositionSide == PosBoth) {
		q.Set("reduceOnly", "true")
	}

	// 全部平仓（与 quantity 互斥）
	if param.ClosePosition {
		q.Set("closePosition", "true")
		q.Del("quantity")
	}

	// 客户端自定义订单ID（用于幂等防重）
	if param.ClientID != "" {
		q.Set("newClientOrderId", param.ClientID)
	}

	q.Set("recvWindow", "5000")

	var resp binanceOrderResponse
	if err := c.doSignedRequest(ctx, http.MethodPost, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}

	return c.convertOrderResponse(&resp), nil
}

const fallbackQuantityPrecision = 3

type binanceExchangeInfoResponse struct {
	Symbols []struct {
		Symbol            string `json:"symbol"`
		QuantityPrecision int    `json:"quantityPrecision"`
	} `json:"symbols"`
}

func (c *BinanceClient) normalizeOrderQuantity(ctx context.Context, symbol string, quantity float64) (float64, int, error) {
	if quantity <= 0 {
		return 0, fallbackQuantityPrecision, fmt.Errorf("quantity must be positive")
	}

	precision, err := c.getQuantityPrecision(ctx, symbol)
	if err != nil {
		precision = fallbackQuantityPrecision
	}
	if precision < 0 {
		precision = 0
	}
	if precision > 8 {
		precision = 8
	}

	scale := math.Pow10(precision)
	normalized := math.Floor(quantity*scale+1e-9) / scale
	if normalized <= 0 {
		return 0, precision, fmt.Errorf("quantity %.8f is below minimum precision for %s", quantity, symbol)
	}
	return normalized, precision, nil
}

func (c *BinanceClient) getQuantityPrecision(ctx context.Context, symbol string) (int, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return fallbackQuantityPrecision, fmt.Errorf("symbol is required")
	}

	c.precisionMu.RLock()
	if precision, ok := c.quantityPrecisionMap[symbol]; ok {
		c.precisionMu.RUnlock()
		return precision, nil
	}
	c.precisionMu.RUnlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/fapi/v1/exchangeInfo", nil)
	if err != nil {
		return fallbackQuantityPrecision, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fallbackQuantityPrecision, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fallbackQuantityPrecision, fmt.Errorf("binance exchangeInfo failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var info binanceExchangeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fallbackQuantityPrecision, err
	}
	for _, item := range info.Symbols {
		if strings.EqualFold(item.Symbol, symbol) {
			c.precisionMu.Lock()
			c.quantityPrecisionMap[symbol] = item.QuantityPrecision
			c.precisionMu.Unlock()
			return item.QuantityPrecision, nil
		}
	}
	return fallbackQuantityPrecision, fmt.Errorf("symbol %s not found in exchangeInfo", symbol)
}

// CancelOrder 取消订单
func (c *BinanceClient) CancelOrder(ctx context.Context, param CancelOrderParam) (*OrderResult, error) {
	if param.Symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if param.OrderID == "" && param.ClientOrderID == "" {
		return nil, fmt.Errorf("order_id or client_order_id is required")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(param.Symbol))
	if param.OrderID != "" {
		q.Set("orderId", param.OrderID)
	}
	if param.ClientOrderID != "" {
		q.Set("origClientOrderId", param.ClientOrderID)
	}
	q.Set("recvWindow", "5000")

	var resp binanceCancelResponse
	if err := c.doSignedRequest(ctx, http.MethodDelete, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}

	return &OrderResult{
		OrderID:          strconv.FormatInt(resp.OrderID, 10),
		ClientOrderID:    resp.ClientOrderID,
		Symbol:           resp.Symbol,
		Status:           OrderStatus(resp.Status),
		Side:             OrderSide(resp.Side),
		ExecutedQuantity: parseFloat(resp.ExecutedQty),
		AvgPrice:         parseFloat(resp.AvgPrice),
		TransactTime:     resp.Time,
	}, nil
}

// QueryOrder 查询订单状态
func (c *BinanceClient) QueryOrder(ctx context.Context, param QueryOrderParam) (*OrderResult, error) {
	if param.Symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if param.OrderID == "" && param.ClientOrderID == "" {
		return nil, fmt.Errorf("order_id or client_order_id is required")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(param.Symbol))
	if param.OrderID != "" {
		q.Set("orderId", param.OrderID)
	}
	if param.ClientOrderID != "" {
		q.Set("origClientOrderId", param.ClientOrderID)
	}
	q.Set("recvWindow", "5000")

	var resp binanceOrderResponse
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}

	return c.convertOrderResponse(&resp), nil
}

// convertOrderResponse 将币安 API 响应转换为统一的 OrderResult
func (c *BinanceClient) convertOrderResponse(resp *binanceOrderResponse) *OrderResult {
	return &OrderResult{
		OrderID:          strconv.FormatInt(resp.OrderID, 10),
		ClientOrderID:    resp.ClientOrderID,
		Symbol:           resp.Symbol,
		Status:           OrderStatus(resp.Status),
		Side:             OrderSide(resp.Side),
		PositionSide:     PositionSide(resp.PositionSide),
		ExecutedQuantity: parseFloat(resp.ExecutedQty),
		AvgPrice:         parseFloat(resp.AvgPrice),
		TransactTime:     resp.Time,
	}
}

// ---------------------------------------------------------------------------
// 账户信息 API
// ---------------------------------------------------------------------------

// binanceAccountResponse 币安期货账户 API 响应结构
type binanceAccountResponse struct {
	TotalWalletBalance string                `json:"totalWalletBalance"`
	TotalUnrealizedPNL string                `json:"totalUnrealizedProfit"`
	TotalMarginBalance string                `json:"totalMarginBalance"`
	AvailableBalance   string                `json:"availableBalance"`
	MaxWithdrawAmount  string                `json:"maxWithdrawAmount"`
	Positions          []binancePositionResp `json:"positions"`
}

// binancePositionResp 币安期货仓位信息
type binancePositionResp struct {
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
func (c *BinanceClient) GetAccountInfo(ctx context.Context) (*AccountResult, error) {
	q := url.Values{}
	q.Set("recvWindow", "5000")

	var resp binanceAccountResponse
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v2/account", q, &resp); err != nil {
		return nil, err
	}

	result := &AccountResult{
		TotalWalletBalance: parseFloat(resp.TotalWalletBalance),
		TotalUnrealizedPnl: parseFloat(resp.TotalUnrealizedPNL),
		TotalMarginBalance: parseFloat(resp.TotalMarginBalance),
		AvailableBalance:   parseFloat(resp.AvailableBalance),
		MaxWithdrawAmount:  parseFloat(resp.MaxWithdrawAmount),
		Positions:          make([]PositionInfo, 0, len(resp.Positions)),
	}

	// 只返回有持仓的仓位（positionAmt != 0）
	for _, p := range resp.Positions {
		amt := parseFloat(p.PositionAmt)
		if amt == 0 {
			continue
		}
		result.Positions = append(result.Positions, PositionInfo{
			Symbol:           p.Symbol,
			PositionAmount:   amt,
			EntryPrice:       parseFloat(p.EntryPrice),
			MarkPrice:        parseFloat(p.MarkPrice),
			UnrealizedPnl:    parseFloat(p.UnrealizedProfit),
			LiquidationPrice: parseFloat(p.LiquidationPrice),
			Leverage:         parseFloat(p.Leverage),
			MarginType:       p.MarginType,
		})
	}

	return result, nil
}

// parseFloat 安全解析浮点数字符串
func parseFloat(v string) float64 {
	f, _ := strconv.ParseFloat(v, 64)
	return f
}

// ---------------------------------------------------------------------------
// 合约查询 API（当前委托、历史委托、历史成交、资金流水、资金费用）
// ---------------------------------------------------------------------------

// BinanceOpenOrder 币安合约当前委托响应结构
type BinanceOpenOrder struct {
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
func (c *BinanceClient) GetOpenOrders(ctx context.Context, symbol string) ([]BinanceOpenOrder, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(symbol))
	}
	q.Set("recvWindow", "5000")

	var resp []BinanceOpenOrder
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/openOrders", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceAllOrder 币安合约历史委托响应结构
type BinanceAllOrder struct {
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

// GetAllOrders 查询历史委托（所有订单）
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认500
func (c *BinanceClient) GetAllOrders(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceAllOrder, error) {
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

	var resp []BinanceAllOrder
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/allOrders", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceUserTrade 币安合约历史成交响应结构
type BinanceUserTrade struct {
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
func (c *BinanceClient) GetUserTrades(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceUserTrade, error) {
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

	var resp []BinanceUserTrade
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/userTrades", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceIncome 币安合约资金流水响应结构
type BinanceIncome struct {
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
func (c *BinanceClient) GetIncomeHistory(ctx context.Context, symbol, incomeType string, startTime, endTime int64, limit int) ([]BinanceIncome, error) {
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

	var resp []BinanceIncome
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/income", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceFundingRate 币安合约资金费率响应结构
type BinanceFundingRate struct {
	Symbol      string `json:"symbol"`
	FundingRate string `json:"fundingRate"`
	FundingTime int64  `json:"fundingTime"`
	MarkPrice   string `json:"markPrice"`
}

// BinanceFundingFee 资金费用条目（从资金流水中提取 FUNDING_FEE 类型）
type BinanceFundingFee struct {
	Symbol     string `json:"symbol"`
	IncomeType string `json:"incomeType"`
	Income     string `json:"income"`
	Asset      string `json:"asset"`
	Time       int64  `json:"time"`
	TranID     int64  `json:"tranId"`
	TradeID    string `json:"tradeId"`
}

// GetFundingRate 查询资金费率历史
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *BinanceClient) GetFundingRate(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceFundingRate, error) {
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

	var resp []BinanceFundingRate
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/fundingRate", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetFundingFees 查询资金费用（从资金流水中筛选 FUNDING_FEE 类型）
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *BinanceClient) GetFundingFees(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceFundingFee, error) {
	// 资金费用本质是 incomeType=FUNDING_FEE 的资金流水
	incomes, err := c.GetIncomeHistory(ctx, symbol, "FUNDING_FEE", startTime, endTime, limit)
	if err != nil {
		return nil, err
	}

	fees := make([]BinanceFundingFee, 0, len(incomes))
	for _, inc := range incomes {
		fees = append(fees, BinanceFundingFee{
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
