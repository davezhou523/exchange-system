package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type BinanceClient struct {
	baseURL   string
	apiKey    string
	secretKey string
	client    *http.Client
}

type OrderResponse struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	ClientOrderID string `json:"clientOrderId"`
	Price         string `json:"price"`
	AvgPrice      string `json:"avgPrice"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	CumQuote      string `json:"cumQuote"`
	TimeInForce   string `json:"timeInForce"`
	Type          string `json:"type"`
	Side          string `json:"side"`
	StopPrice     string `json:"stopPrice"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
}

type AccountInfo struct {
	Assets             []Asset    `json:"assets"`
	Positions          []Position `json:"positions"`
	TotalWalletBalance string     `json:"totalWalletBalance"`
	AvailableBalance   string     `json:"availableBalance"`
}

type Asset struct {
	Asset            string `json:"asset"`
	WalletBalance    string `json:"walletBalance"`
	AvailableBalance string `json:"availableBalance"`
}

type Position struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	UnrealizedProfit string `json:"unrealizedProfit"`
	Leverage         string `json:"leverage"`
}

func NewBinanceClient(baseURL, apiKey, secretKey string) *BinanceClient {
	return &BinanceClient{
		baseURL:   baseURL,
		apiKey:    apiKey,
		secretKey: secretKey,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *BinanceClient) signRequest(query url.Values) string {
	// 添加时间戳
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	query.Set("timestamp", timestamp)

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
	mac := hmac.New(sha256.New, []byte(c.secretKey))
	mac.Write([]byte(queryString))
	signature := hex.EncodeToString(mac.Sum(nil))

	log.Printf("[签名] 时间戳=%s, 查询字符串=%s, 签名=%s", timestamp, queryString, signature)

	// 将签名添加到查询参数
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

func (c *BinanceClient) doSignedRequest(ctx context.Context, method, path string, query url.Values) (*http.Response, error) {
	signedQuery := c.signRequest(query)
	endpoint := fmt.Sprintf("%s%s?%s", c.baseURL, path, signedQuery)

	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		return nil, fmt.Errorf("binance request failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	return resp, nil
}

func (c *BinanceClient) CreateOrder(ctx context.Context, symbol, side, positionSide string, quantity float64) (*OrderResponse, error) {
	query := url.Values{}
	query.Set("symbol", strings.ToUpper(symbol))
	query.Set("side", strings.ToUpper(side))
	query.Set("positionSide", strings.ToUpper(positionSide))
	query.Set("type", "MARKET")
	query.Set("quantity", fmt.Sprintf("%.6f", quantity))
	query.Set("recvWindow", "5000")

	resp, err := c.doSignedRequest(ctx, "POST", "/fapi/v1/order", query)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var orderResp OrderResponse
	if err := json.NewDecoder(resp.Body).Decode(&orderResp); err != nil {
		return nil, err
	}

	return &orderResp, nil
}

func (c *BinanceClient) GetAccountInfo(ctx context.Context) (*AccountInfo, error) {
	query := url.Values{}
	query.Set("recvWindow", "5000")

	resp, err := c.doSignedRequest(ctx, "GET", "/fapi/v2/account", query)
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
