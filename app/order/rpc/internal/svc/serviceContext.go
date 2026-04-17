package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"exchange-system/app/order/rpc/internal/config"
	"exchange-system/common/binance"
)

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
	Config  config.Config
	client  *binance.Client
	dataDir string
}

// NewServiceContext 创建 Order 服务上下文
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	client := binance.NewClient(
		c.Binance.BaseURL,
		c.Binance.APIKey,
		c.Binance.SecretKey,
	)

	dataDir := c.DataDir
	if dataDir == "" {
		dataDir = "data/futures"
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	log.Printf("[Order服务] 初始化完成 | 端点=%s 数据目录=%s", c.Binance.BaseURL, dataDir)

	return &ServiceContext{
		Config:  c,
		client:  client,
		dataDir: dataDir,
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
	return s.client.GetAllOrders(ctx, symbol, startTime, endTime, limit)
}

// GetUserTrades 查询历史成交
func (s *ServiceContext) GetUserTrades(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.UserTrade, error) {
	return s.client.GetUserTrades(ctx, symbol, startTime, endTime, limit)
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
			if err := enc.Encode(item); err != nil {
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
