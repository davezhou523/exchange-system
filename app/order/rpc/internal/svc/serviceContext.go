package svc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"exchange-system/app/order/rpc/internal/config"
	"exchange-system/common/binance"
)

const defaultAllOrdersLookback = 7 * 24 * time.Hour

// ---------------------------------------------------------------------------
// ServiceContext — Order 微服务核心
//
// 职责：
//   1. 封装币安合约 API 客户端
//   2. 提供6种合约数据查询能力
//   3. 数据保存为 JSONL 到 data 目录
// ---------------------------------------------------------------------------

// ServiceContext Order 服务上下文
type ServiceContext struct {
	Config               config.Config
	client               *binance.Client
	dataDir              string
	executionOrderLogDir string
}

// NewServiceContext 创建 Order 服务上下文
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	client := binance.NewClient(
		c.Binance.BaseURL,
		c.Binance.APIKey,
		c.Binance.SecretKey,
		c.Binance.Proxy,
	)

	dataDir := c.DataDir
	if dataDir == "" {
		dataDir = "data/futures"
	}

	executionOrderLogDir := c.ExecutionOrderLogDir
	if executionOrderLogDir == "" {
		executionOrderLogDir = "../../execution/rpc/data/order"
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	log.Printf("[Order服务] 初始化完成 | 端点=%s 数据目录=%s", c.Binance.BaseURL, dataDir)
	defaultStartTime := defaultAllOrdersStartTime()
	log.Printf("[Order服务] GetAllOrders 查询上下文 | baseURL=%s defaultSymbol=%s startTime=%d(%s) endTime=0(no-limit)",
		c.Binance.BaseURL, "ETHUSDT", defaultStartTime, formatMillis(defaultStartTime))

	return &ServiceContext{
		Config:               c,
		client:               client,
		dataDir:              dataDir,
		executionOrderLogDir: executionOrderLogDir,
	}, nil
}

// GetClient 获取币安合约 API 客户端
func (s *ServiceContext) GetClient() *binance.Client {
	return s.client
}

// ---------------------------------------------------------------------------
// 合约数据查询方法
// ---------------------------------------------------------------------------

// GetOpenOrders 查询当前委托（未成交订单）
func (s *ServiceContext) GetOpenOrders(ctx context.Context, symbol string) ([]binance.OpenOrder, error) {
	return s.client.GetOpenOrders(ctx, symbol)
}

// GetAllOrders 查询历史委托
func (s *ServiceContext) GetAllOrders(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.AllOrder, error) {
	startTime = normalizeAllOrdersStartTime(startTime)
	log.Printf("[Order服务] GetAllOrders 请求 | baseURL=%s symbol=%s startTime=%d(%s) endTime=%d(%s) limit=%d",
		s.Config.Binance.BaseURL, symbol, startTime, formatMillis(startTime), endTime, formatMillis(endTime), limit)
	orders, apiErr := s.client.GetAllOrders(ctx, symbol, startTime, endTime, limit)
	simOrders, simErr := s.readSimulatedAllOrders(symbol, startTime, endTime)
	if simErr != nil {
		log.Printf("[Order服务] 读取 simulated 历史委托失败: %v", simErr)
	}
	if apiErr != nil {
		if len(simOrders) == 0 {
			return nil, apiErr
		}
		log.Printf("[Order服务] Binance 历史委托查询失败，回退使用 simulated 本地订单: %v", apiErr)
		orders = nil
	}
	orders = append(orders, simOrders...)
	sort.Slice(orders, func(i, j int) bool {
		if orders[i].UpdateTime == orders[j].UpdateTime {
			return orders[i].OrderID > orders[j].OrderID
		}
		return orders[i].UpdateTime > orders[j].UpdateTime
	})
	if limit > 0 && len(orders) > limit {
		orders = orders[:limit]
	}
	if err := s.writeJSONL("all_orders", symbol, orders); err != nil {
		log.Printf("[Order服务] 写入历史委托失败: %v", err)
	}
	return orders, nil
}

// GetUserTrades 查询历史成交
func (s *ServiceContext) GetUserTrades(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.UserTrade, error) {
	trades, apiErr := s.client.GetUserTrades(ctx, symbol, startTime, endTime, limit)
	simTrades, simErr := s.readSimulatedUserTrades(symbol, startTime, endTime)
	if simErr != nil {
		log.Printf("[Order服务] 读取 simulated 历史成交失败: %v", simErr)
	}
	if apiErr != nil {
		if len(simTrades) == 0 {
			return nil, apiErr
		}
		log.Printf("[Order服务] Binance 历史成交查询失败，回退使用 simulated 本地成交: %v", apiErr)
		trades = nil
	}
	trades = append(trades, simTrades...)
	sort.Slice(trades, func(i, j int) bool {
		if trades[i].Time == trades[j].Time {
			return trades[i].ID > trades[j].ID
		}
		return trades[i].Time > trades[j].Time
	})
	if limit > 0 && len(trades) > limit {
		trades = trades[:limit]
	}
	return trades, nil
}

// GetIncomeHistory 查询资金流水
func (s *ServiceContext) GetIncomeHistory(ctx context.Context, symbol, incomeType string, startTime, endTime int64, limit int) ([]binance.Income, error) {
	return s.client.GetIncomeHistory(ctx, symbol, incomeType, startTime, endTime, limit)
}

// GetFundingRateHistory 查询资金费率历史
func (s *ServiceContext) GetFundingRateHistory(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.FundingRate, error) {
	return s.client.GetFundingRate(ctx, symbol, startTime, endTime, limit)
}

// GetFundingFees 查询资金费用
func (s *ServiceContext) GetFundingFees(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.FundingFee, error) {
	return s.client.GetFundingFees(ctx, symbol, startTime, endTime, limit)
}

// ---------------------------------------------------------------------------
// JSONL 数据保存
// ---------------------------------------------------------------------------

// writeJSONL 将数据以 JSONL 格式写入文件
// 目录结构：{dataDir}/{category}/{symbol}/2026-04-17.jsonl
func (s *ServiceContext) writeJSONL(category, symbol string, items interface{}) error {
	dateStr := time.Now().UTC().Format("2006-01-02")
	dir := filepath.Join(s.dataDir, category, symbol)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	filePath := filepath.Join(dir, dateStr+".jsonl")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s: %w", filePath, err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetEscapeHTML(false)

	switch v := items.(type) {
	case []binance.OpenOrder:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.AllOrder:
		for _, item := range v {
			entry := toAllOrderJSONLEntry(item)
			if err := enc.Encode(entry); err != nil {
				return err
			}
		}
	case []binance.UserTrade:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.Income:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.FundingRate:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.FundingFee:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	}

	return nil
}

type executionOrderLogEntry struct {
	Timestamp       string  `json:"timestamp"`
	SignalType      string  `json:"signal_type"`
	StrategyID      string  `json:"strategy_id"`
	Symbol          string  `json:"symbol"`
	OrderID         string  `json:"order_id"`
	ClientID        string  `json:"client_id"`
	Side            string  `json:"side"`
	PositionSide    string  `json:"position_side"`
	Type            string  `json:"type"`
	Status          string  `json:"status"`
	Quantity        float64 `json:"quantity"`
	ExecutedQty     float64 `json:"executed_qty"`
	AvgPrice        float64 `json:"avg_price"`
	Commission      float64 `json:"commission"`
	CommissionAsset string  `json:"commission_asset"`
	Slippage        float64 `json:"slippage"`
	StopLoss        float64 `json:"stop_loss"`
	Atr             float64 `json:"atr"`
	RiskReward      float64 `json:"risk_reward"`
	Reason          string  `json:"reason"`
	TransactTime    int64   `json:"transact_time"`
}

type allOrderJSONLEntry struct {
	Time          string `json:"time"`
	OrderID       int64  `json:"order_id"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	PositionSide  string `json:"position_side"`
	Type          string `json:"type"`
	OrigQty       string `json:"orig_qty"`
	ExecutedQty   string `json:"executed_qty"`
	AvgPrice      string `json:"avg_price"`
	Price         string `json:"price"`
	StopPrice     string `json:"stop_price"`
	ClientOrderID string `json:"client_order_id"`
	ReduceOnly    bool   `json:"reduce_only"`
	ClosePosition bool   `json:"close_position"`
	UpdateTime    string `json:"update_time"`
	TimeInForce   string `json:"time_in_force"`
}

func (s *ServiceContext) readSimulatedAllOrders(symbol string, startTime, endTime int64) ([]binance.AllOrder, error) {
	entries, err := s.readExecutionOrderLogs(symbol, startTime, endTime)
	if err != nil {
		return nil, err
	}
	orders := make([]binance.AllOrder, 0, len(entries))
	for _, e := range entries {
		orders = append(orders, binance.AllOrder{
			OrderID:       stableID(e.OrderID),
			Symbol:        e.Symbol,
			Status:        e.Status,
			Side:          e.Side,
			PositionSide:  e.PositionSide,
			Type:          e.Type,
			OrigQty:       formatFloat(e.Quantity),
			ExecutedQty:   formatFloat(e.ExecutedQty),
			AvgPrice:      formatFloat(e.AvgPrice),
			Price:         formatFloat(e.AvgPrice),
			StopPrice:     formatFloat(e.StopLoss),
			ClientOrderID: e.ClientID,
			Time:          e.TransactTime,
			UpdateTime:    e.TransactTime,
			ReduceOnly:    false,
			ClosePosition: false,
			TimeInForce:   "IOC",
		})
	}
	return orders, nil
}

func (s *ServiceContext) readSimulatedUserTrades(symbol string, startTime, endTime int64) ([]binance.UserTrade, error) {
	entries, err := s.readExecutionOrderLogs(symbol, startTime, endTime)
	if err != nil {
		return nil, err
	}
	trades := make([]binance.UserTrade, 0, len(entries))
	for _, e := range entries {
		price := e.AvgPrice
		qty := e.ExecutedQty
		trades = append(trades, binance.UserTrade{
			ID:              stableID(e.ClientID + ":" + e.OrderID),
			Symbol:          e.Symbol,
			OrderID:         stableID(e.OrderID),
			Side:            e.Side,
			PositionSide:    e.PositionSide,
			Price:           formatFloat(price),
			Qty:             formatFloat(qty),
			RealizedPnl:     "0",
			MarginAsset:     e.CommissionAsset,
			QuoteQty:        formatFloat(price * qty),
			Commission:      formatFloat(e.Commission),
			CommissionAsset: e.CommissionAsset,
			Time:            e.TransactTime,
			Buyer:           strings.EqualFold(e.Side, "BUY"),
			Maker:           false,
		})
	}
	return trades, nil
}

func (s *ServiceContext) readExecutionOrderLogs(symbol string, startTime, endTime int64) ([]executionOrderLogEntry, error) {
	if s.executionOrderLogDir == "" || symbol == "" {
		return nil, nil
	}
	pattern := filepath.Join(s.executionOrderLogDir, symbol, "*.jsonl")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", pattern, err)
	}
	if len(paths) == 0 {
		return nil, nil
	}

	var out []executionOrderLogEntry
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}

		scanner := bufio.NewScanner(f)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)

		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var entry executionOrderLogEntry
			if err := json.Unmarshal(line, &entry); err != nil {
				continue
			}
			if entry.Symbol != symbol {
				continue
			}
			if startTime > 0 && entry.TransactTime < startTime {
				continue
			}
			if endTime > 0 && entry.TransactTime > endTime {
				continue
			}
			out = append(out, entry)
		}
		closeErr := f.Close()
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("scan %s: %w", path, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close %s: %w", path, closeErr)
		}
	}
	return out, nil
}

func stableID(s string) int64 {
	if s == "" {
		return 0
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	var digits strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	if digits.Len() > 0 {
		if n, err := strconv.ParseInt(digits.String(), 10, 64); err == nil {
			return n
		}
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func formatMillis(ms int64) string {
	if ms <= 0 {
		return "no-limit"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func formatOrderTime(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
}

func toAllOrderJSONLEntry(item binance.AllOrder) allOrderJSONLEntry {
	return allOrderJSONLEntry{
		Time:          formatOrderTime(item.Time),
		OrderID:       item.OrderID,
		Symbol:        item.Symbol,
		Status:        item.Status,
		Side:          item.Side,
		PositionSide:  item.PositionSide,
		Type:          item.Type,
		OrigQty:       item.OrigQty,
		ExecutedQty:   item.ExecutedQty,
		AvgPrice:      item.AvgPrice,
		Price:         item.Price,
		StopPrice:     item.StopPrice,
		ClientOrderID: item.ClientOrderID,
		ReduceOnly:    item.ReduceOnly,
		ClosePosition: item.ClosePosition,
		UpdateTime:    formatOrderTime(item.UpdateTime),
		TimeInForce:   item.TimeInForce,
	}
}

func normalizeAllOrdersStartTime(startTime int64) int64 {
	if startTime > 0 {
		return startTime
	}
	return defaultAllOrdersStartTime()
}

func defaultAllOrdersStartTime() int64 {
	return time.Now().Add(-defaultAllOrdersLookback).UnixMilli()
}

// FetchAllData 拉取全部合约数据并保存到本地 JSONL
func (s *ServiceContext) FetchAllData(ctx context.Context, symbol string, startTime, endTime int64) error {
	log.Printf("[Order服务] 开始拉取全部合约数据 | symbol=%s", symbol)

	// 1. 当前委托
	orders, err := s.GetOpenOrders(ctx, symbol)
	if err != nil {
		log.Printf("[Order服务] 当前委托拉取失败: %v", err)
	} else if len(orders) > 0 {
		grouped := make(map[string][]binance.OpenOrder)
		for _, o := range orders {
			sym := o.Symbol
			if sym == "" {
				sym = symbol
			}
			grouped[sym] = append(grouped[sym], o)
		}
		for sym, items := range grouped {
			if err := s.writeJSONL("open_orders", sym, items); err != nil {
				log.Printf("[Order服务] 写入当前委托失败: %v", err)
			}
		}
		log.Printf("[Order服务] 当前委托 | 共 %d 条", len(orders))
	}

	// 2. 历史委托
	if symbol != "" {
		allOrders, err := s.GetAllOrders(ctx, symbol, startTime, endTime, 1000)
		if err != nil {
			log.Printf("[Order服务] 历史委托拉取失败: %v", err)
		} else {
			if err := s.writeJSONL("all_orders", symbol, allOrders); err != nil {
				log.Printf("[Order服务] 写入历史委托失败: %v", err)
			}
			log.Printf("[Order服务] 历史委托 | 共 %d 条", len(allOrders))
		}
	}

	// 3. 历史成交
	if symbol != "" {
		trades, err := s.GetUserTrades(ctx, symbol, startTime, endTime, 1000)
		if err != nil {
			log.Printf("[Order服务] 历史成交拉取失败: %v", err)
		} else {
			if err := s.writeJSONL("user_trades", symbol, trades); err != nil {
				log.Printf("[Order服务] 写入历史成交失败: %v", err)
			}
			log.Printf("[Order服务] 历史成交 | 共 %d 条", len(trades))
		}
	}

	// 4. 资金流水
	incomes, err := s.GetIncomeHistory(ctx, symbol, "", startTime, endTime, 1000)
	if err != nil {
		log.Printf("[Order服务] 资金流水拉取失败: %v", err)
	} else if len(incomes) > 0 {
		grouped := make(map[string][]binance.Income)
		for _, inc := range incomes {
			sym := inc.Symbol
			if sym == "" {
				sym = symbol
			}
			if sym == "" {
				sym = "ALL"
			}
			grouped[sym] = append(grouped[sym], inc)
		}
		for sym, items := range grouped {
			if err := s.writeJSONL("income", sym, items); err != nil {
				log.Printf("[Order服务] 写入资金流水失败: %v", err)
			}
		}
		log.Printf("[Order服务] 资金流水 | 共 %d 条", len(incomes))
	}

	// 5. 资金费用
	fees, err := s.GetFundingFees(ctx, symbol, startTime, endTime, 1000)
	if err != nil {
		log.Printf("[Order服务] 资金费用拉取失败: %v", err)
	} else if len(fees) > 0 {
		grouped := make(map[string][]binance.FundingFee)
		for _, fee := range fees {
			sym := fee.Symbol
			if sym == "" {
				sym = symbol
			}
			if sym == "" {
				sym = "ALL"
			}
			grouped[sym] = append(grouped[sym], fee)
		}
		for sym, items := range grouped {
			if err := s.writeJSONL("funding_fee", sym, items); err != nil {
				log.Printf("[Order服务] 写入资金费用失败: %v", err)
			}
		}
		log.Printf("[Order服务] 资金费用 | 共 %d 条", len(fees))
	}

	log.Printf("[Order服务] 全部数据拉取完成 | symbol=%s", symbol)
	return nil
}
