package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"exchange-system/app/execution/rpc/internal/config"
	"exchange-system/app/execution/rpc/internal/exchange"
	"exchange-system/app/execution/rpc/internal/idempotent"
	"exchange-system/app/execution/rpc/internal/kafka"
	"exchange-system/app/execution/rpc/internal/order"
	"exchange-system/app/execution/rpc/internal/orderlog"
	"exchange-system/app/execution/rpc/internal/position"
	"exchange-system/app/execution/rpc/internal/risk"
	commonkafka "exchange-system/common/kafka"
	strategypb "exchange-system/common/pb/strategy"
)

// ---------------------------------------------------------------------------
// ServiceContext 执行服务上下文
//
// 职责：
//   1. 接收策略信号 (Kafka signal topic)
//   2. 幂等检查（防重复下单）
//   3. 风控检查（仓位/杠杆/日亏损/敞口）
//   4. 路由下单（Binance / OKX）
//   5. 订单管理（状态跟踪）
//   6. 仓位管理（同步更新）
//   7. 发布订单结果 (Kafka order topic)
// ---------------------------------------------------------------------------

// ServiceContext 执行服务上下文
type ServiceContext struct {
	Config config.Config

	// 核心组件
	router       *exchange.Router               // 下单路由器
	riskManager  *risk.Manager                  // 风控管理器
	posMonitor   *risk.PositionMonitor          // 主动降仓监控器
	orderManager *order.Manager                 // 订单管理器
	posManager   *position.Manager              // 仓位管理器
	deduplicator *idempotent.SignalDeduplicator // 幂等去重器
	logger       *orderlog.Logger               // 日志记录器

	// Kafka
	signalConsumer      *kafka.Consumer
	harvestPathConsumer *kafka.HarvestPathConsumer
	orderProducer       *kafka.Producer

	harvestPathMu      sync.RWMutex
	harvestPathSignals map[string]harvestPathRiskSnapshot
	analyticsWriter    *executionClickHouseWriter

	// 生命周期
	cancel context.CancelFunc
}

type harvestPathRiskSnapshot struct {
	Symbol                 string
	EventTime              int64
	HarvestPathProbability float64
	RuleProbability        float64
	LSTMProbability        float64
	BookProbability        float64
	BookSummary            string
	VolatilityRegime       string
	ThresholdSource        string
	AppliedThreshold       float64
	PathAction             string
	RiskLevel              string
	TargetSide             string
	ReferencePrice         float64
	MarketPrice            float64
	ReceivedAt             time.Time
}

// NewServiceContext 创建执行服务上下文
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 1. 创建 Kafka 订单生产者
	orderProducer, err := kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.Order)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create order producer: %v", err)
	}

	// 2. 创建 Kafka 信号消费者
	groupID := c.Kafka.Group
	if groupID == "" {
		groupID = "execution-rpc-signal"
	}
	signalConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, groupID, c.Kafka.Topics.Signal)
	if err != nil {
		_ = orderProducer.Close()
		cancel()
		return nil, fmt.Errorf("create signal consumer: %v", err)
	}
	var harvestPathConsumer *kafka.HarvestPathConsumer
	if c.Kafka.Topics.HarvestPathSignal != "" {
		harvestPathConsumer, err = kafka.NewHarvestPathConsumer(c.Kafka.Addrs, groupID+"-harvest-path", c.Kafka.Topics.HarvestPathSignal)
		if err != nil {
			_ = signalConsumer.Close()
			_ = orderProducer.Close()
			cancel()
			return nil, fmt.Errorf("create harvest path consumer: %v", err)
		}
	}

	// Periodic lag print for ops/troubleshooting.
	commonkafka.StartConsumerGroupLagReporter(ctx, c.Kafka.Addrs, groupID, c.Kafka.Topics.Signal, 30*time.Second)
	if c.Kafka.Topics.HarvestPathSignal != "" {
		commonkafka.StartConsumerGroupLagReporter(ctx, c.Kafka.Addrs, groupID+"-harvest-path", c.Kafka.Topics.HarvestPathSignal, 30*time.Second)
	}

	// 3. 创建下单路由器并注册交易所
	router := exchange.NewRouter(exchange.RouterConfig{
		Strategy:        exchange.RouteStrategy(c.Exchange.RouteStrategy),
		DefaultExchange: c.Exchange.DefaultExchange,
		SymbolRoutes:    c.Exchange.SymbolRoutes,
	})

	// 注册币安交易所
	binanceClient := exchange.NewBinanceClient(
		c.Exchange.Binance.BaseURL,
		c.Exchange.Binance.APIKey,
		c.Exchange.Binance.SecretKey,
		c.Exchange.Binance.Proxy,
		c.Risk.MaxLeverage,
	)
	router.Register("binance", binanceClient)
	log.Printf("[初始化] 交易路由配置 | RouteStrategy=%s DefaultExchange=%s BinanceBaseURL=%s BinanceProxy=%s",
		c.Exchange.RouteStrategy, c.Exchange.DefaultExchange, c.Exchange.Binance.BaseURL, c.Exchange.Binance.Proxy)

	// 注册 OKX 交易所（占位）
	if c.Exchange.OKX.APIKey != "" {
		okxClient := exchange.NewOKXClient(exchange.OKXConfig{
			APIKey:     c.Exchange.OKX.APIKey,
			SecretKey:  c.Exchange.OKX.SecretKey,
			Passphrase: c.Exchange.OKX.Passphrase,
			BaseURL:    c.Exchange.OKX.BaseURL,
		})
		router.Register("okx", okxClient)
	}

	// 4. 创建仓位管理器和订单管理器
	posManager := position.NewManager()
	orderManager := order.NewManager()

	// 5. 创建风控管理器
	riskConfig := risk.RiskConfig{
		MaxPositionSize:     c.Risk.MaxPositionSize,
		MaxLeverage:         c.Risk.MaxLeverage,
		MaxDailyLossPct:     c.Risk.MaxDailyLossPct,
		MaxOpenPositions:    c.Risk.MaxOpenPositions,
		MaxPositionExposure: c.Risk.MaxPositionExposure,
		StopLossPercent:     c.Risk.StopLossPercent,
		MinOrderNotional:    c.Risk.MinOrderNotional,
	}
	riskManager := risk.NewManager(riskConfig, posManager)

	// 6. 创建幂等去重器
	ttlSeconds := c.Idempotent.TTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = 600
	}
	deduplicator := idempotent.NewSignalDeduplicator(time.Duration(ttlSeconds) * time.Second)

	// 7. 创建日志记录器
	logger := orderlog.NewLogger(c.SignalLogDir, c.OrderLogDir)
	analyticsWriter, err := newExecutionClickHouseWriter(ctx, c.ClickHouse)
	if err != nil {
		_ = signalConsumer.Close()
		if harvestPathConsumer != nil {
			_ = harvestPathConsumer.Close()
		}
		_ = orderProducer.Close()
		cancel()
		return nil, fmt.Errorf("init execution clickhouse writer: %v", err)
	}

	// 8. 构建服务上下文
	svcCtx := &ServiceContext{
		Config:              c,
		router:              router,
		riskManager:         riskManager,
		posMonitor:          nil,
		orderManager:        orderManager,
		posManager:          posManager,
		deduplicator:        deduplicator,
		logger:              logger,
		signalConsumer:      signalConsumer,
		harvestPathConsumer: harvestPathConsumer,
		orderProducer:       orderProducer,
		harvestPathSignals:  make(map[string]harvestPathRiskSnapshot),
		analyticsWriter:     analyticsWriter,
		cancel:              cancel,
	}

	// 9. 创建并启动主动降仓监控器
	if c.PositionMonitor.Enabled {
		reduceFn := func(ctx context.Context, symbol, positionSide string, reduceQty float64) error {
			if svcCtx.router == nil {
				return fmt.Errorf("router not initialized")
			}
			_, err := svcCtx.router.CreateOrder(ctx, exchange.CreateOrderParam{
				Symbol:       symbol,
				Side:         exchange.SideSell,
				PositionSide: exchange.PositionSide(positionSide),
				Type:         exchange.OrderTypeMarket,
				Quantity:     reduceQty,
				ReduceOnly:   true,
				ClientID:     fmt.Sprintf("posmon-%s-%d", symbol, time.Now().UnixMilli()),
			})
			return err
		}
		posMonitorCfg := risk.PositionMonitorConfig{
			Enabled:           true,
			CheckInterval:     c.PositionMonitor.CheckInterval,
			DrawdownThreshold: c.PositionMonitor.DrawdownThreshold,
			ReduceRatio:       c.PositionMonitor.ReduceRatio,
			MinReduceNotional: c.PositionMonitor.MinReduceNotional,
		}
		svcCtx.posMonitor = risk.NewPositionMonitor(posMonitorCfg, posManager, reduceFn)
		svcCtx.posMonitor.Start(ctx)
		log.Printf("[初始化] 主动降仓监控已启动 | interval=%v threshold=%.0f%% reduce=%.0f%%",
			c.PositionMonitor.CheckInterval, c.PositionMonitor.DrawdownThreshold*100, c.PositionMonitor.ReduceRatio*100)
	}

	if harvestPathConsumer != nil {
		if err := harvestPathConsumer.StartConsuming(ctx, func(sig *kafka.HarvestPathSignal) error {
			return svcCtx.handleHarvestPathSignal(sig)
		}); err != nil {
			_ = harvestPathConsumer.Close()
			_ = signalConsumer.Close()
			_ = orderProducer.Close()
			logger.Close()
			cancel()
			return nil, fmt.Errorf("start harvest path consumer: %v", err)
		}
		log.Printf("[初始化] 收割路径风险消费者已启动 | topic=%s group=%s", c.Kafka.Topics.HarvestPathSignal, groupID+"-harvest-path")
	}

	// 12. 启动信号消费
	if err := signalConsumer.StartConsuming(ctx, func(sig *strategypb.Signal) error {
		return svcCtx.HandleSignal(ctx, sig)
	}); err != nil {
		_ = signalConsumer.Close()
		if harvestPathConsumer != nil {
			_ = harvestPathConsumer.Close()
		}
		_ = orderProducer.Close()
		logger.Close()
		cancel()
		return nil, fmt.Errorf("start signal consumer: %v", err)
	}

	signalLogAbs, signalLogAbsErr := filepath.Abs(c.SignalLogDir)
	if signalLogAbsErr != nil {
		signalLogAbs = c.SignalLogDir
	}
	orderLogAbs, orderLogAbsErr := filepath.Abs(c.OrderLogDir)
	if orderLogAbsErr != nil {
		orderLogAbs = c.OrderLogDir
	}
	log.Printf("[初始化] 日志目录绝对路径 | SignalLogDir=%s OrderLogDir=%s", signalLogAbs, orderLogAbs)
	log.Printf("[初始化] 执行服务就绪 | 路由策略=%s 默认交易所=%s 风控杠杆=%.0fx | 信号日志=%s 订单日志=%s",
		c.Exchange.RouteStrategy, c.Exchange.DefaultExchange, riskConfig.MaxLeverage, c.SignalLogDir, c.OrderLogDir)

	return svcCtx, nil
}

// ---------------------------------------------------------------------------
// 核心业务：信号处理
// ---------------------------------------------------------------------------

// HandleSignal 处理策略交易信号
// 流程：解析信号类型 → 幂等检查 → 风控检查(仅OPEN) → 路由下单 → 更新订单/仓位 → 发布结果
//
// execution 只消费"可执行信号"：
//   - OPEN 信号：开仓，需过风控 → 下市价单
//   - CLOSE 信号：平仓，跳过开仓风控 → 下市价平仓单
//   - HOLD 信号：持仓观察，仅记录日志，不下单
func (s *ServiceContext) HandleSignal(ctx context.Context, sig *strategypb.Signal) error {
	if sig == nil {
		return nil
	}

	symbol := sig.GetSymbol()
	action := sig.GetAction()
	side := sig.GetSide()
	quantity := sig.GetQuantity()
	entryPrice := sig.GetEntryPrice()
	strategyID := sig.GetStrategyId()
	stopLoss := sig.GetStopLoss()
	takeProfits := sig.GetTakeProfits()
	signalType := sig.GetSignalType()
	atr := sig.GetAtr()
	riskReward := sig.GetRiskReward()

	log.Printf("[信号] 收到策略信号 | type=%s strategy=%s symbol=%s action=%s side=%s qty=%.4f price=%.2f atr=%.2f rr=%.2f",
		signalType, strategyID, symbol, action, side, quantity, entryPrice, atr, riskReward)

	// 记录收到的信号日志
	s.logger.LogSignal(sig)

	// HOLD 信号：仅日志，不下单
	if signalType == "HOLD" || action == "HOLD" {
		log.Printf("[信号] HOLD信号，仅观察 | strategy=%s symbol=%s", strategyID, symbol)
		return nil
	}

	// 1. 幂等检查：防重复下单
	sigKey := idempotent.SignalKey(strategyID, symbol, sig.GetTimestamp())
	if duplicate, existingOrderID := s.deduplicator.Check(sigKey); duplicate {
		log.Printf("[信号] 重复信号已忽略 | key=%s 已有订单=%s", sigKey, existingOrderID)
		return nil
	}

	// 3. 确定订单参数
	orderSide := mapActionToSide(action)
	posSide := mapSideToPositionSide(side)
	clientID := buildSignalClientOrderID(strategyID, sig.GetTimestamp())

	// 平仓信号：使用 reduce_only 参数（交易所会自动平掉对应方向的仓位）
	reduceOnly := signalType == "CLOSE" || signalType == "PARTIAL_CLOSE"
	if reduceOnly {
		closeQty, err := s.resolveCloseQuantity(ctx, symbol, side, quantity)
		if err != nil {
			s.logger.LogOrderFailure(sig, exchange.StatusRejected, clientID, fmt.Sprintf("resolve close quantity failed: %v", err), quantity, s.currentHarvestPathMeta(symbol, side))
			s.recordOrderFailureAnalytics(sig, clientID, quantity, fmt.Sprintf("resolve close quantity failed: %v", err), reduceOnly)
			s.deduplicator.Remove(sigKey)
			return fmt.Errorf("resolve close quantity failed: %v", err)
		}
		if closeQty <= 0 {
			log.Printf("[信号] CLOSE信号忽略 | symbol=%s side=%s 无可平仓位", symbol, side)
			s.deduplicator.Remove(sigKey)
			return nil
		}
		quantity = closeQty
	}

	// 2. 风控检查（仅开仓信号需要，平仓信号跳过）
	if signalType == "OPEN" || signalType == "" {
		wickAdjustedQty, wickErr := s.applyHarvestPathRisk(symbol, side, quantity, entryPrice)
		if wickErr != nil {
			s.logger.LogOrderFailure(sig, exchange.StatusRejected, clientID, fmt.Sprintf("harvest path risk rejected: %v", wickErr), quantity, s.currentHarvestPathMeta(symbol, side))
			s.recordOrderFailureAnalytics(sig, clientID, quantity, fmt.Sprintf("harvest path risk rejected: %v", wickErr), reduceOnly)
			s.deduplicator.Remove(sigKey)
			return fmt.Errorf("harvest path risk rejected: %v", wickErr)
		}
		quantity = wickAdjustedQty
		adjustedQty, err := s.performRiskCheck(ctx, symbol, action, quantity, entryPrice)
		if err != nil {
			s.logger.LogOrderFailure(sig, exchange.StatusRejected, clientID, fmt.Sprintf("risk check failed: %v", err), quantity, s.currentHarvestPathMeta(symbol, side))
			s.recordOrderFailureAnalytics(sig, clientID, quantity, fmt.Sprintf("risk check failed: %v", err), reduceOnly)
			// 风控拒绝时移除幂等记录，允许后续重试
			s.deduplicator.Remove(sigKey)
			return fmt.Errorf("risk check failed: %v", err)
		}
		quantity = adjustedQty

		// 新开仓前先清理该交易对遗留的保护单，避免官网页面残留旧止损止盈造成混淆。
		if err := s.cancelRiskOrdersBeforeOpen(ctx, symbol); err != nil {
			s.logger.LogOrderFailure(sig, exchange.StatusRejected, clientID, fmt.Sprintf("cancel stale stop/take orders failed: %v", err), quantity, s.currentHarvestPathMeta(symbol, side))
			s.recordOrderFailureAnalytics(sig, clientID, quantity, fmt.Sprintf("cancel stale stop/take orders failed: %v", err), reduceOnly)
			s.deduplicator.Remove(sigKey)
			return fmt.Errorf("cancel stale stop/take orders failed: %v", err)
		}
	}

	// 4. 路由下单
	orderResult, err := s.router.CreateOrder(ctx, exchange.CreateOrderParam{
		Symbol:       symbol,
		Side:         orderSide,
		PositionSide: posSide,
		Type:         exchange.OrderTypeMarket,
		Quantity:     quantity,
		Price:        entryPrice,
		StopPrice:    sig.GetStopLoss(),
		ClientID:     clientID,
		ReduceOnly:   reduceOnly,
	})
	if err != nil {
		s.logger.LogOrderFailure(sig, exchange.StatusRejected, clientID, fmt.Sprintf("create order failed: %v", err), quantity, s.currentHarvestPathMeta(symbol, side))
		s.recordOrderFailureAnalytics(sig, clientID, quantity, fmt.Sprintf("create order failed: %v", err), reduceOnly)
		// 下单失败时移除幂等记录，允许重试
		s.deduplicator.Remove(sigKey)
		return fmt.Errorf("create order failed: %v", err)
	}

	// 处理已成交订单的后续逻辑
	s.handleFilledOrder(sig, orderResult, sigKey, signalType, strategyID, symbol, quantity, stopLoss, atr, riskReward, takeProfits)

	return nil
}

// handleFilledOrder 处理已成交订单的后续逻辑（更新管理器、发布Kafka、记录日志）
func (s *ServiceContext) handleFilledOrder(sig *strategypb.Signal, orderResult *exchange.OrderResult, sigKey, signalType, strategyID, symbol string, quantity, stopLoss, atr, riskReward float64, takeProfits []float64) {
	// 标记幂等记录的订单ID
	s.deduplicator.MarkOrderID(sigKey, orderResult.OrderID)

	// 更新订单管理器
	s.orderManager.AddOrder(&order.OrderState{
		OrderID:         orderResult.OrderID,
		ClientID:        orderResult.ClientOrderID,
		Symbol:          symbol,
		Side:            orderResult.Side,
		PositionSide:    orderResult.PositionSide,
		Type:            exchange.OrderTypeMarket,
		Status:          orderResult.Status,
		Quantity:        quantity,
		ExecutedQty:     orderResult.ExecutedQuantity,
		AvgPrice:        orderResult.AvgPrice,
		Commission:      orderResult.Commission,
		CommissionAsset: orderResult.CommissionAsset,
		StrategyID:      strategyID,
		SignalKey:       sigKey,
		SignalType:      signalType,
		CreateTime:      time.Now(),
		TransactTime:    time.UnixMilli(orderResult.TransactTime),
		Slippage:        orderResult.Slippage,
		StopLoss:        stopLoss,
		TakeProfits:     takeProfits,
		Atr:             atr,
		RiskReward:      riskReward,
		Reason:          sig.GetReason(),
		SignalReason:    cloneSignalReason(sig.GetSignalReason()),
		Indicators:      cloneIndicators(sig.GetIndicators()),
	})

	// 更新仓位管理器。减仓成交优先回拉交易所真实仓位，避免本地账本缺失时把 PARTIAL_CLOSE 误记成清仓。
	s.updatePositionStateAfterFill(context.Background(), symbol, signalType, orderResult, strategyID, stopLoss, takeProfits)

	// 发布订单结果到 Kafka
	orderEvent := s.buildOrderEvent(sig, orderResult)
	if err := s.orderProducer.SendMarketData(context.Background(), orderEvent); err != nil {
		log.Printf("[信号] 发布订单结果失败: %v", err)
	}

	log.Printf("[信号] 订单完成 | type=%s ID=%s 状态=%s 成交=%.4f@%.2f 手续费=%.4f 滑点=%.4f",
		signalType, orderResult.OrderID, orderResult.Status,
		orderResult.ExecutedQuantity, orderResult.AvgPrice,
		orderResult.Commission, orderResult.Slippage)

	var protectionResult *exchange.ProtectionSetupResult

	// 开仓成交后设置止损止盈
	if signalType == "OPEN" || signalType == "" {
		if stopLoss > 0 || len(takeProfits) > 0 {
			targetPositionSide := string(orderResult.PositionSide)
			if targetPositionSide == "" || targetPositionSide == string(exchange.PosBoth) {
				targetPositionSide = string(mapSideToPositionSide(sig.GetSide()))
			}
			protectionQuantity := resolveProtectionQuantity(orderResult, quantity)
			result, err := s.router.SetStopLossTakeProfit(context.Background(), symbol, targetPositionSide, protectionQuantity, stopLoss, takeProfits)
			protectionResult = result
			if err != nil {
				log.Printf("[信号] 设置止损止盈失败 | symbol=%s status=%s reason=%s error=%v", symbol, resultStatus(result), resultReason(result), err)
			} else {
				log.Printf("[信号] 止损止盈设置成功 | symbol=%s positionSide=%s protection_qty=%.4f status=%s reason=%s 止损=%.2f 止盈=%v", symbol, targetPositionSide, protectionQuantity, resultStatus(result), resultReason(result), stopLoss, takeProfits)
			}
		}
	}

	// 记录订单执行结果日志，包含保护单下发结果，方便后续查询与排障。
	s.logger.LogOrder(sig, orderResult, quantity, s.currentHarvestPathMeta(symbol, string(orderResult.PositionSide)), protectionResult)
	s.recordFilledOrderAnalytics(sig, orderResult, quantity, signalType == "CLOSE" || signalType == "PARTIAL_CLOSE")
}

// resolveProtectionQuantity 选择保护单应使用的数量，优先取真实成交量，避免信号原始数量与持仓不一致。
func resolveProtectionQuantity(orderResult *exchange.OrderResult, requestedQty float64) float64 {
	if orderResult != nil && orderResult.ExecutedQuantity > 0 {
		return orderResult.ExecutedQuantity
	}
	return requestedQty
}

// updatePositionStateAfterFill 在订单成交后刷新本地仓位账本。
// 对 reduce-only 成交优先按交易所真实仓位同步，只有同步失败时才回退到本地增量更新。
func (s *ServiceContext) updatePositionStateAfterFill(ctx context.Context, symbol, signalType string, orderResult *exchange.OrderResult, strategyID string, stopLoss float64, takeProfits []float64) {
	if s == nil || s.posManager == nil || orderResult == nil {
		return
	}
	normalizedSignalType := strings.ToUpper(strings.TrimSpace(signalType))
	if normalizedSignalType == "PARTIAL_CLOSE" || normalizedSignalType == "CLOSE" {
		if s.syncPositionFromExchangeBySymbol(ctx, symbol) {
			return
		}
	}
	s.posManager.UpdateFromOrder(orderResult, strategyID, normalizedSignalType, stopLoss, takeProfits)
}

// syncPositionFromExchangeBySymbol 按交易对从交易所回拉仓位真相，用于 reduce-only 成交后的账本修正。
func (s *ServiceContext) syncPositionFromExchangeBySymbol(ctx context.Context, symbol string) bool {
	if s == nil || s.router == nil || s.posManager == nil || strings.TrimSpace(symbol) == "" {
		return false
	}
	beforeLocal := currentLocalPositionForLog(s.posManager, symbol)
	account, err := s.router.GetAccountInfoForSymbol(ctx, symbol)
	if err != nil {
		log.Printf("[仓位管理] 回拉交易所仓位失败 | symbol=%s error=%v", symbol, err)
		return false
	}
	exchangeQty, exchangeFound := exchangePositionAmountForLog(account, symbol)
	s.posManager.UpdateFromExchange(account)
	log.Printf("[仓位管理] 交易所同步 | symbol=%s before_local=%s exchange_qty=%s", symbol, beforeLocal, formatPositionAmountForLog(exchangeQty, exchangeFound))
	return true
}

// currentLocalPositionForLog 返回当前本地仓位的日志文本，便于 reduce-only 同步时快速对比前后差异。
func currentLocalPositionForLog(manager *position.Manager, symbol string) string {
	if manager == nil {
		return "none"
	}
	if pos, ok := manager.GetPosition(symbol); ok {
		return formatPositionAmountForLog(pos.PositionAmount, true)
	}
	return "none"
}

// exchangePositionAmountForLog 汇总交易所账户快照中指定交易对的净持仓数量，便于和本地账本直接对比。
func exchangePositionAmountForLog(account *exchange.AccountResult, symbol string) (float64, bool) {
	if account == nil {
		return 0, false
	}
	total := 0.0
	found := false
	for _, pos := range account.Positions {
		if !strings.EqualFold(strings.TrimSpace(pos.Symbol), symbol) || pos.PositionAmount == 0 {
			continue
		}
		total += pos.PositionAmount
		found = true
	}
	return total, found
}

// formatPositionAmountForLog 把仓位数量统一格式化成日志友好的文本，无仓位时输出 none。
func formatPositionAmountForLog(amount float64, ok bool) string {
	if !ok {
		return "none"
	}
	return fmt.Sprintf("%.4f", amount)
}

// resultStatus 提供保护单结果的安全状态读取，避免日志打印时空指针。
func resultStatus(v *exchange.ProtectionSetupResult) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(v.Status)
}

// resultReason 提供保护单结果的安全原因读取，避免日志打印时空指针。
func resultReason(v *exchange.ProtectionSetupResult) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(v.Reason)
}

func cloneSignalReason(v *strategypb.SignalReason) *strategypb.SignalReason {
	if v == nil {
		return nil
	}
	out := &strategypb.SignalReason{
		Summary:          v.GetSummary(),
		Phase:            v.GetPhase(),
		TrendContext:     v.GetTrendContext(),
		SetupContext:     v.GetSetupContext(),
		PathContext:      v.GetPathContext(),
		ExecutionContext: v.GetExecutionContext(),
		ExitReasonKind:   v.GetExitReasonKind(),
		ExitReasonLabel:  v.GetExitReasonLabel(),
		RouteBucket:      v.GetRouteBucket(),
		RouteReason:      v.GetRouteReason(),
		RouteTemplate:    v.GetRouteTemplate(),
		Allocator:        cloneAllocatorStatus(v.GetAllocator()),
		Range:            cloneRangeSignalReason(v.GetRange()),
	}
	if tags := v.GetTags(); len(tags) > 0 {
		out.Tags = append([]string(nil), tags...)
	}
	return out
}

// cloneRangeSignalReason 复制 signal 携带的 range 摘要，避免下游复用原对象。
func cloneRangeSignalReason(v *strategypb.RangeSignalReason) *strategypb.RangeSignalReason {
	if v == nil {
		return nil
	}
	return &strategypb.RangeSignalReason{
		H1RangeOk:      v.GetH1RangeOk(),
		H1AdxOk:        v.GetH1AdxOk(),
		H1BollWidthOk:  v.GetH1BollWidthOk(),
		M15TouchLower:  v.GetM15TouchLower(),
		M15RsiTurnUp:   v.GetM15RsiTurnUp(),
		M15TouchUpper:  v.GetM15TouchUpper(),
		M15RsiTurnDown: v.GetM15RsiTurnDown(),
	}
}

// cloneAllocatorStatus 复制 signal 携带的 allocator 快照，避免下游复用原对象。
func cloneAllocatorStatus(v *strategypb.PositionAllocatorStatus) *strategypb.PositionAllocatorStatus {
	if v == nil {
		return nil
	}
	return &strategypb.PositionAllocatorStatus{
		Template:       v.GetTemplate(),
		RouteBucket:    v.GetRouteBucket(),
		RouteReason:    v.GetRouteReason(),
		Score:          v.GetScore(),
		ScoreSource:    v.GetScoreSource(),
		BucketBudget:   v.GetBucketBudget(),
		StrategyWeight: v.GetStrategyWeight(),
		SymbolWeight:   v.GetSymbolWeight(),
		RiskScale:      v.GetRiskScale(),
		PositionBudget: v.GetPositionBudget(),
		TradingPaused:  v.GetTradingPaused(),
		PauseReason:    v.GetPauseReason(),
	}
}

func cloneIndicators(v map[string]float64) map[string]float64 {
	if len(v) == 0 {
		return nil
	}
	out := make(map[string]float64, len(v))
	for key, value := range v {
		out[key] = value
	}
	return out
}

// performRiskCheck 执行风控检查
func (s *ServiceContext) performRiskCheck(ctx context.Context, symbol, action string, quantity, price float64) (float64, error) {
	// 获取账户信息
	account, err := s.router.GetAccountInfo(ctx)
	if err != nil {
		return quantity, fmt.Errorf("get account info: %v", err)
	}

	// 同步仓位到管理器
	s.posManager.UpdateFromExchange(account)

	// 执行风控检查
	result := s.riskManager.CheckPreOrder(account, symbol, action, quantity, price)
	if !result.Passed {
		return quantity, fmt.Errorf("risk rejected: %v", result.Reasons)
	}
	return result.AdjustedQuantity, nil
}

func (s *ServiceContext) handleHarvestPathSignal(sig *kafka.HarvestPathSignal) error {
	if s == nil || sig == nil || strings.TrimSpace(sig.Symbol) == "" {
		return nil
	}

	snapshot := harvestPathRiskSnapshot{
		Symbol:                 strings.ToUpper(strings.TrimSpace(sig.Symbol)),
		EventTime:              sig.EventTime,
		HarvestPathProbability: sig.HarvestPathProbability,
		RuleProbability:        sig.RuleProbability,
		LSTMProbability:        sig.LSTMProbability,
		BookProbability:        sig.BookProbability,
		BookSummary:            strings.TrimSpace(sig.BookSummary),
		VolatilityRegime:       strings.ToUpper(strings.TrimSpace(sig.VolatilityRegime)),
		ThresholdSource:        strings.TrimSpace(sig.ThresholdSource),
		AppliedThreshold:       sig.AppliedThreshold,
		PathAction:             strings.ToUpper(strings.TrimSpace(sig.PathAction)),
		RiskLevel:              strings.ToUpper(strings.TrimSpace(sig.RiskLevel)),
		TargetSide:             strings.ToUpper(strings.TrimSpace(sig.TargetSide)),
		ReferencePrice:         sig.ReferencePrice,
		MarketPrice:            sig.MarketPrice,
		ReceivedAt:             time.Now().UTC(),
	}

	s.harvestPathMu.Lock()
	prev, ok := s.harvestPathSignals[snapshot.Symbol]
	if ok && snapshot.EventTime < prev.EventTime {
		s.harvestPathMu.Unlock()
		return nil
	}
	s.harvestPathSignals[snapshot.Symbol] = snapshot
	s.harvestPathMu.Unlock()

	log.Printf("[harvest-path risk] update symbol=%s level=%s path_action=%s prob=%.2f rule_prob=%.2f lstm_prob=%.2f book_prob=%.2f threshold=%.2f regime=%s source=%s target=%s ref=%.2f market=%.2f",
		snapshot.Symbol, snapshot.RiskLevel, snapshot.PathAction, snapshot.HarvestPathProbability, snapshot.RuleProbability, snapshot.LSTMProbability,
		snapshot.BookProbability, snapshot.AppliedThreshold, snapshot.VolatilityRegime, snapshot.ThresholdSource, snapshot.TargetSide, snapshot.ReferencePrice, snapshot.MarketPrice)
	return nil
}

func (s *ServiceContext) applyHarvestPathRisk(symbol, side string, quantity, price float64) (float64, error) {
	if s == nil || quantity <= 0 {
		return quantity, nil
	}
	snapshot, ok := s.matchedHarvestPathSignal(symbol, side)
	if !ok {
		return quantity, nil
	}
	entrySide := strings.ToUpper(strings.TrimSpace(side))

	if snapshot.PathAction == "WAIT_FOR_RECLAIM" || snapshot.RiskLevel == "PATH_ALERT" {
		return quantity, fmt.Errorf("symbol=%s path_alert: wait_for_reclaim prob=%.2f path_action=%s", symbol, snapshot.HarvestPathProbability, snapshot.PathAction)
	}
	if snapshot.PathAction != "REDUCE_PROBE_SIZE" && snapshot.RiskLevel != "PATH_PRESSURE" {
		return quantity, nil
	}

	scale := math.Max(0.25, 1-snapshot.HarvestPathProbability)
	adjusted := quantity * scale
	if adjusted >= quantity {
		return quantity, nil
	}

	log.Printf("[harvest-path risk] trim probe size symbol=%s side=%s qty=%.4f->%.4f prob=%.2f path_action=%s risk=%s price=%.2f ref=%.2f",
		strings.ToUpper(strings.TrimSpace(symbol)), entrySide, quantity, adjusted, snapshot.HarvestPathProbability,
		snapshot.PathAction, snapshot.RiskLevel, price, snapshot.ReferencePrice)
	return adjusted, nil
}

func (s *ServiceContext) getLatestHarvestPathSignal(symbol string) (harvestPathRiskSnapshot, bool) {
	if s == nil {
		return harvestPathRiskSnapshot{}, false
	}
	s.harvestPathMu.RLock()
	defer s.harvestPathMu.RUnlock()
	snapshot, ok := s.harvestPathSignals[strings.ToUpper(strings.TrimSpace(symbol))]
	return snapshot, ok
}

func (s *ServiceContext) matchedHarvestPathSignal(symbol, side string) (harvestPathRiskSnapshot, bool) {
	snapshot, ok := s.getLatestHarvestPathSignal(symbol)
	if !ok {
		return harvestPathRiskSnapshot{}, false
	}
	if time.Since(snapshot.ReceivedAt) > 2*time.Minute {
		return harvestPathRiskSnapshot{}, false
	}
	entrySide := strings.ToUpper(strings.TrimSpace(side))
	if (entrySide == "LONG" && snapshot.TargetSide != "UP") || (entrySide == "SHORT" && snapshot.TargetSide != "DOWN") {
		return harvestPathRiskSnapshot{}, false
	}
	return snapshot, true
}

func (s *ServiceContext) currentHarvestPathMeta(symbol, side string) *orderlog.HarvestPathMeta {
	snapshot, ok := s.matchedHarvestPathSignal(symbol, side)
	if !ok {
		return nil
	}
	return &orderlog.HarvestPathMeta{
		Probability:      snapshot.HarvestPathProbability,
		RuleProbability:  snapshot.RuleProbability,
		LSTMProbability:  snapshot.LSTMProbability,
		BookProbability:  snapshot.BookProbability,
		BookSummary:      snapshot.BookSummary,
		VolatilityRegime: snapshot.VolatilityRegime,
		ThresholdSource:  snapshot.ThresholdSource,
		AppliedThreshold: snapshot.AppliedThreshold,
		PathAction:       snapshot.PathAction,
		RiskLevel:        snapshot.RiskLevel,
		TargetSide:       snapshot.TargetSide,
		ReferencePrice:   snapshot.ReferencePrice,
		MarketPrice:      snapshot.MarketPrice,
	}
}

// resolveCloseQuantity 根据当前交易所仓位和策略请求的平仓数量，计算本次 reduce-only 应实际下发的数量。
func (s *ServiceContext) resolveCloseQuantity(ctx context.Context, symbol, side string, requestedQty float64) (float64, error) {
	account, err := s.router.GetAccountInfoForSymbol(ctx, symbol)
	if err != nil {
		return 0, fmt.Errorf("get account info: %v", err)
	}
	s.posManager.UpdateFromExchange(account)

	wantPositive := side == "LONG"
	totalQty := 0.0
	for _, pos := range account.Positions {
		if pos.Symbol != symbol || pos.PositionAmount == 0 {
			continue
		}
		if wantPositive && pos.PositionAmount > 0 {
			totalQty += pos.PositionAmount
		}
		if !wantPositive && pos.PositionAmount < 0 {
			totalQty += -pos.PositionAmount
		}
	}
	if requestedQty > 0 && requestedQty < totalQty {
		return requestedQty, nil
	}
	return totalQty, nil
}

// buildOrderEvent 构建订单结果事件，发布到 Kafka order topic
func (s *ServiceContext) buildOrderEvent(sig *strategypb.Signal, result *exchange.OrderResult) map[string]interface{} {
	event := map[string]interface{}{
		"order_id":         result.OrderID,
		"client_id":        result.ClientOrderID,
		"symbol":           result.Symbol,
		"status":           string(result.Status),
		"signal_type":      sig.GetSignalType(),
		"side":             sig.GetAction(),
		"position_side":    sig.GetSide(),
		"quantity":         result.ExecutedQuantity,
		"avg_price":        result.AvgPrice,
		"commission":       result.Commission,
		"commission_asset": result.CommissionAsset,
		"slippage":         result.Slippage,
		"timestamp":        result.TransactTime,
		"strategy_id":      sig.GetStrategyId(),
		"stop_loss":        sig.GetStopLoss(),
		"take_profits":     sig.GetTakeProfits(),
		"reason":           orderlog.ComposeHarvestPathReason(sig.GetReason(), s.currentHarvestPathMeta(result.Symbol, sig.GetSide())),
		"atr":              sig.GetAtr(),
		"risk_reward":      sig.GetRiskReward(),
		"indicators":       sig.GetIndicators(),
	}
	if sr := orderlogSignalReasonMap(sig.GetSignalReason()); sr != nil {
		event["signal_reason"] = sr
	}
	if harvestPath := s.currentHarvestPathMeta(result.Symbol, sig.GetSide()); harvestPath != nil {
		event["harvest_path_probability"] = harvestPath.Probability
		event["harvest_path_rule_probability"] = harvestPath.RuleProbability
		event["harvest_path_lstm_probability"] = harvestPath.LSTMProbability
		event["harvest_path_action"] = harvestPath.PathAction
		event["harvest_path_risk_level"] = harvestPath.RiskLevel
		event["harvest_path_target_side"] = harvestPath.TargetSide
		event["harvest_path_reference_price"] = harvestPath.ReferencePrice
		event["harvest_path_market_price"] = harvestPath.MarketPrice
	}
	return event
}

func orderlogSignalReasonMap(reason *strategypb.SignalReason) map[string]interface{} {
	if reason == nil {
		return nil
	}
	out := map[string]interface{}{
		"summary":           strings.TrimSpace(reason.GetSummary()),
		"phase":             strings.TrimSpace(reason.GetPhase()),
		"trend_context":     strings.TrimSpace(reason.GetTrendContext()),
		"setup_context":     strings.TrimSpace(reason.GetSetupContext()),
		"path_context":      strings.TrimSpace(reason.GetPathContext()),
		"execution_context": strings.TrimSpace(reason.GetExecutionContext()),
		"exit_reason_kind":  strings.TrimSpace(reason.GetExitReasonKind()),
		"exit_reason_label": strings.TrimSpace(reason.GetExitReasonLabel()),
		"route_bucket":      strings.TrimSpace(reason.GetRouteBucket()),
		"route_reason":      strings.TrimSpace(reason.GetRouteReason()),
		"route_template":    strings.TrimSpace(reason.GetRouteTemplate()),
	}
	if allocator := allocatorStatusMap(reason.GetAllocator()); allocator != nil {
		out["allocator"] = allocator
	}
	if tags := reason.GetTags(); len(tags) > 0 {
		out["tags"] = append([]string(nil), tags...)
	}
	empty := true
	for _, v := range out {
		switch x := v.(type) {
		case string:
			if x != "" {
				empty = false
			}
		case []string:
			if len(x) > 0 {
				empty = false
			}
		}
	}
	if empty {
		return nil
	}
	return out
}

// allocatorStatusMap 将 allocator 快照转成日志/事件可直接序列化的 map。
func allocatorStatusMap(v *strategypb.PositionAllocatorStatus) map[string]interface{} {
	if v == nil {
		return nil
	}
	out := map[string]interface{}{
		"template":        strings.TrimSpace(v.GetTemplate()),
		"route_bucket":    strings.TrimSpace(v.GetRouteBucket()),
		"route_reason":    strings.TrimSpace(v.GetRouteReason()),
		"score":           v.GetScore(),
		"score_source":    strings.TrimSpace(v.GetScoreSource()),
		"bucket_budget":   v.GetBucketBudget(),
		"strategy_weight": v.GetStrategyWeight(),
		"symbol_weight":   v.GetSymbolWeight(),
		"risk_scale":      v.GetRiskScale(),
		"position_budget": v.GetPositionBudget(),
		"trading_paused":  v.GetTradingPaused(),
		"pause_reason":    strings.TrimSpace(v.GetPauseReason()),
	}
	empty := true
	for _, value := range out {
		switch x := value.(type) {
		case string:
			if x != "" {
				empty = false
			}
		case float64:
			if x != 0 {
				empty = false
			}
		case bool:
			if x {
				empty = false
			}
		}
	}
	if empty {
		return nil
	}
	return out
}

// ---------------------------------------------------------------------------
// 映射辅助函数
// ---------------------------------------------------------------------------

// mapActionToSide 将策略 action 映射为交易所 order side
func mapActionToSide(action string) exchange.OrderSide {
	if action == "BUY" {
		return exchange.SideBuy
	}
	return exchange.SideSell
}

// mapSideToPositionSide 将策略 side 映射为交易所 position side
func mapSideToPositionSide(side string) exchange.PositionSide {
	switch side {
	case "LONG":
		return exchange.PosLong
	case "SHORT":
		return exchange.PosShort
	default:
		return exchange.PosBoth
	}
}

// ---------------------------------------------------------------------------
// gRPC 服务调用的方法
// ---------------------------------------------------------------------------

// CreateOrderViaGRPC gRPC 创建订单入口
func (s *ServiceContext) CreateOrderViaGRPC(ctx context.Context, symbol, side, positionSide string, quantity float64, price float64, orderType string, clientID string) (*exchange.OrderResult, error) {
	if s == nil || s.router == nil {
		return nil, fmt.Errorf("execution service not initialized")
	}
	clientID = normalizeClientOrderID(clientID)

	// 风控检查
	account, err := s.router.GetAccountInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("get account info: %v", err)
	}
	s.posManager.UpdateFromExchange(account)

	result := s.riskManager.CheckPreOrder(account, symbol, side, quantity, price)
	if !result.Passed {
		return nil, fmt.Errorf("risk rejected: %v", result.Reasons)
	}
	quantity = result.AdjustedQuantity

	// 下单
	return s.router.CreateOrder(ctx, exchange.CreateOrderParam{
		Symbol:       symbol,
		Side:         exchange.OrderSide(side),
		PositionSide: exchange.PositionSide(positionSide),
		Type:         exchange.OrderType(orderType),
		Quantity:     quantity,
		Price:        price,
		ClientID:     clientID,
	})
}

// CancelOrderViaGRPC gRPC 取消订单入口
func (s *ServiceContext) CancelOrderViaGRPC(ctx context.Context, symbol, orderID, clientOrderID string) (*exchange.OrderResult, error) {
	if s == nil || s.router == nil {
		return nil, fmt.Errorf("execution service not initialized")
	}
	return s.router.CancelOrder(ctx, exchange.CancelOrderParam{
		Symbol:        symbol,
		OrderID:       orderID,
		ClientOrderID: clientOrderID,
	})
}

const maxClientOrderIDLen = 36

func buildSignalClientOrderID(strategyID string, timestamp int64) string {
	return normalizeClientOrderID(fmt.Sprintf("sig-%s-%d", strategyID, timestamp))
}

func normalizeClientOrderID(clientID string) string {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" || len(clientID) <= maxClientOrderIDLen {
		return clientID
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(clientID))

	suffix := ""
	if idx := strings.LastIndex(clientID, "-"); idx >= 0 && idx < len(clientID)-1 {
		suffix = clientID[idx+1:]
	}
	if len(suffix) > 13 {
		suffix = suffix[len(suffix)-13:]
	}
	if suffix != "" {
		return fmt.Sprintf("sig-%010x-%s", h.Sum64()&0xffffffffff, suffix)
	}
	return fmt.Sprintf("cid-%010x", h.Sum64()&0xffffffffff)
}

// QueryOrderViaGRPC gRPC 查询订单入口
func (s *ServiceContext) QueryOrderViaGRPC(ctx context.Context, symbol, orderID, clientOrderID string) (*exchange.OrderResult, error) {
	if s == nil || s.router == nil {
		return nil, fmt.Errorf("execution service not initialized")
	}

	// 先从内存订单簿查询
	if orderID != "" {
		if state, ok := s.orderManager.GetOrder(orderID); ok {
			return &exchange.OrderResult{
				OrderID:          state.OrderID,
				ClientOrderID:    state.ClientID,
				Symbol:           state.Symbol,
				Status:           state.Status,
				Side:             state.Side,
				PositionSide:     state.PositionSide,
				ExecutedQuantity: state.ExecutedQty,
				AvgPrice:         state.AvgPrice,
				Commission:       state.Commission,
				CommissionAsset:  state.CommissionAsset,
				TransactTime:     state.TransactTime.UnixMilli(),
				ErrorMessage:     state.ErrorMessage,
			}, nil
		}
	}

	// 内存未找到，查交易所
	return s.router.QueryOrder(ctx, exchange.QueryOrderParam{
		Symbol:        symbol,
		OrderID:       orderID,
		ClientOrderID: clientOrderID,
	})
}

// GetAccountInfoViaGRPC gRPC 获取账户信息入口
func (s *ServiceContext) GetAccountInfoViaGRPC(ctx context.Context, symbol string) (*exchange.AccountResult, error) {
	if s == nil || s.router == nil {
		return nil, fmt.Errorf("execution service not initialized")
	}
	if symbol != "" {
		routedExchange, err := s.router.Route(symbol)
		if err != nil {
			return nil, err
		}
		if binanceClient, ok := routedExchange.(*exchange.BinanceClient); ok {
			return binanceClient.GetAccountInfoBySymbol(ctx, symbol)
		}
	}
	return s.router.GetAccountInfo(ctx)
}

func (s *ServiceContext) GetLocalPosition(symbol string) (*position.PositionState, bool) {
	if s == nil || s.posManager == nil || strings.TrimSpace(symbol) == "" {
		return nil, false
	}
	return s.posManager.GetPosition(strings.ToUpper(strings.TrimSpace(symbol)))
}

func (s *ServiceContext) GetRiskTargetsFromSignalLog(symbol, positionSide string) (float64, float64, bool) {
	if s == nil || strings.TrimSpace(symbol) == "" || strings.TrimSpace(positionSide) == "" || s.Config.SignalLogDir == "" {
		return 0, 0, false
	}

	dateStr := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(s.Config.SignalLogDir, strings.ToUpper(strings.TrimSpace(symbol)), dateStr+".jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, false
	}

	type signalLogEntry struct {
		SignalType  string    `json:"signal_type"`
		Side        string    `json:"side"`
		StopLoss    float64   `json:"stop_loss"`
		TakeProfits []float64 `json:"take_profits"`
	}

	targetSide := strings.ToUpper(strings.TrimSpace(positionSide))
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		var entry signalLogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(entry.Side)) != targetSide {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(entry.SignalType)) {
		case "CLOSE":
			return 0, 0, false
		case "OPEN":
			takeProfit := 0.0
			for _, tp := range entry.TakeProfits {
				if tp > 0 {
					takeProfit = tp
					break
				}
			}
			return entry.StopLoss, takeProfit, true
		}
	}

	return 0, 0, false
}

func (s *ServiceContext) cancelRiskOrdersBeforeOpen(ctx context.Context, symbol string) error {
	if s == nil || s.router == nil || strings.TrimSpace(symbol) == "" {
		return nil
	}
	ex, err := s.router.Route(symbol)
	if err != nil {
		return err
	}

	type riskOrderCleaner interface {
		CancelStopLossTakeProfit(ctx context.Context, symbol string, positionSide string) error
	}
	cleaner, ok := ex.(riskOrderCleaner)
	if !ok {
		return nil
	}
	return cleaner.CancelStopLossTakeProfit(ctx, symbol, "")
}

// ---------------------------------------------------------------------------
// 生命周期
// ---------------------------------------------------------------------------

// Close 关闭服务上下文
func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	var firstErr error
	if s.logger != nil {
		s.logger.Close()
	}
	if s.signalConsumer != nil {
		if err := s.signalConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.harvestPathConsumer != nil {
		if err := s.harvestPathConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.orderProducer != nil {
		if err := s.orderProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.analyticsWriter != nil {
		if err := s.analyticsWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// recordOrderFailureAnalytics 记录下单失败对应的 execution/order 分析事件。
func (s *ServiceContext) recordOrderFailureAnalytics(sig *strategypb.Signal, clientID string, requestedQty float64, errorMessage string, reduceOnly bool) {
	if s == nil || s.analyticsWriter == nil || sig == nil {
		return
	}
	s.analyticsWriter.LogRejectedOrder(sig, clientID, requestedQty, reduceOnly, s.currentExchangeName(sig.GetSymbol()), orderlog.ComposeHarvestPathReason(sig.GetReason(), s.currentHarvestPathMeta(sig.GetSymbol(), sig.GetSide())), errorMessage)
}

// recordPendingOrderAnalytics 记录挂单中的 execution/order 分析事件。
func (s *ServiceContext) recordPendingOrderAnalytics(sig *strategypb.Signal, result *exchange.OrderResult, requestedQty float64, reduceOnly bool) {
	if s == nil || s.analyticsWriter == nil || sig == nil || result == nil {
		return
	}
	s.analyticsWriter.LogPendingOrder(sig, result, requestedQty, reduceOnly, s.currentExchangeName(result.Symbol), orderlog.ComposeHarvestPathReason(sig.GetReason(), s.currentHarvestPathMeta(result.Symbol, sig.GetSide())))
}

// recordFilledOrderAnalytics 记录成交完成后的 execution/order/position_cycle 分析事件。
func (s *ServiceContext) recordFilledOrderAnalytics(sig *strategypb.Signal, result *exchange.OrderResult, requestedQty float64, reduceOnly bool) {
	if s == nil || s.analyticsWriter == nil || sig == nil || result == nil {
		return
	}
	s.analyticsWriter.LogFilledOrder(sig, result, requestedQty, reduceOnly, s.currentExchangeName(result.Symbol), orderlog.ComposeHarvestPathReason(sig.GetReason(), s.currentHarvestPathMeta(result.Symbol, sig.GetSide())))
}

// currentExchangeName 根据当前路由配置返回交易对落单的交易所名称。
func (s *ServiceContext) currentExchangeName(symbol string) string {
	if s == nil || s.router == nil {
		return ""
	}
	ex, err := s.router.Route(symbol)
	if err != nil || ex == nil {
		return ""
	}
	return ex.Name()
}

// executionAnalyticsEnvelope 封装待写入 ClickHouse 的执行分析事件。
type executionAnalyticsEnvelope struct {
	table string
	row   interface{}
}

// executionEventFactRow 定义 execution_event_fact 的 JSONEachRow 行结构。
type executionEventFactRow struct {
	EventTime     string  `json:"event_time"`
	AccountID     string  `json:"account_id"`
	Symbol        string  `json:"symbol"`
	StrategyID    string  `json:"strategy_id"`
	SignalID      string  `json:"signal_id"`
	OrderID       string  `json:"order_id"`
	ClientOrderID string  `json:"client_order_id"`
	Action        string  `json:"action"`
	Side          string  `json:"side"`
	SignalType    string  `json:"signal_type"`
	RequestedQty  float64 `json:"requested_qty"`
	ExecutedQty   float64 `json:"executed_qty"`
	Price         float64 `json:"price"`
	Status        string  `json:"status"`
	Exchange      string  `json:"exchange"`
	LatencyMs     uint32  `json:"latency_ms"`
	ErrorCode     string  `json:"error_code"`
	ErrorMessage  string  `json:"error_message"`
	TraceID       string  `json:"trace_id"`
}

// orderEventFactRow 定义 order_event_fact 的 JSONEachRow 行结构。
type orderEventFactRow struct {
	EventTime        string  `json:"event_time"`
	AccountID        string  `json:"account_id"`
	Symbol           string  `json:"symbol"`
	OrderID          string  `json:"order_id"`
	ClientOrderID    string  `json:"client_order_id"`
	PositionCycleID  string  `json:"position_cycle_id"`
	StrategyID       string  `json:"strategy_id"`
	Template         string  `json:"template"`
	ActionType       string  `json:"action_type"`
	Side             string  `json:"side"`
	OrderType        string  `json:"order_type"`
	Price            float64 `json:"price"`
	OrigQty          float64 `json:"orig_qty"`
	ExecutedQty      float64 `json:"executed_qty"`
	Status           string  `json:"status"`
	ReduceOnly       uint8   `json:"reduce_only"`
	Reason           string  `json:"reason"`
	SignalReasonJSON string  `json:"signal_reason_json"`
	TraceID          string  `json:"trace_id"`
}

// positionCycleFactRow 定义 position_cycle_fact 的 JSONEachRow 行结构。
type positionCycleFactRow struct {
	OpenTime               string  `json:"open_time"`
	CloseTime              *string `json:"close_time"`
	AccountID              string  `json:"account_id"`
	Symbol                 string  `json:"symbol"`
	PositionCycleID        string  `json:"position_cycle_id"`
	StrategyID             string  `json:"strategy_id"`
	Template               string  `json:"template"`
	PositionSide           string  `json:"position_side"`
	CycleStatus            string  `json:"cycle_status"`
	EntryPrice             float64 `json:"entry_price"`
	ExitPrice              float64 `json:"exit_price"`
	EntryQty               float64 `json:"entry_qty"`
	ExitQty                float64 `json:"exit_qty"`
	RealizedPnl            float64 `json:"realized_pnl"`
	MaxProfit              float64 `json:"max_profit"`
	MaxDrawdown            float64 `json:"max_drawdown"`
	PartialCloseOrderCount uint32  `json:"partial_close_order_count"`
	FinalCloseOrderCount   uint32  `json:"final_close_order_count"`
	ExitReasonKind         string  `json:"exit_reason_kind"`
	ExitReasonLabel        string  `json:"exit_reason_label"`
	TraceID                string  `json:"trace_id"`
}

// positionCycleState 保存运行中的仓位生命周期快照。
type positionCycleState struct {
	OpenTime               time.Time
	CloseTime              *time.Time
	AccountID              string
	Symbol                 string
	PositionCycleID        string
	StrategyID             string
	Template               string
	PositionSide           string
	CycleStatus            string
	EntryPrice             float64
	ExitPrice              float64
	EntryQty               float64
	ExitQty                float64
	RealizedPnl            float64
	MaxProfit              float64
	MaxDrawdown            float64
	PartialCloseOrderCount uint32
	FinalCloseOrderCount   uint32
	ExitReasonKind         string
	ExitReasonLabel        string
	TraceID                string
}

// executionClickHouseWriter 负责把执行分析事件异步写入 ClickHouse。
type executionClickHouseWriter struct {
	endpoint      string
	database      string
	username      string
	password      string
	accountID     string
	client        *http.Client
	queue         chan executionAnalyticsEnvelope
	flushInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	mu              sync.Mutex
	cycles          map[string]*positionCycleState
	pendingCycleIDs map[string]string
}

// newExecutionClickHouseWriter 创建执行分析异步写入器。
func newExecutionClickHouseWriter(parent context.Context, cfg config.ClickHouseConfig) (*executionClickHouseWriter, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("execution clickhouse endpoint is required when enabled")
	}
	accountID := strings.TrimSpace(cfg.AccountID)
	if accountID == "" {
		return nil, fmt.Errorf("execution clickhouse account_id is required when enabled")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 2048
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	writer := &executionClickHouseWriter{
		endpoint:        endpoint,
		database:        strings.TrimSpace(cfg.Database),
		username:        strings.TrimSpace(cfg.Username),
		password:        cfg.Password,
		accountID:       accountID,
		client:          &http.Client{Timeout: timeout},
		queue:           make(chan executionAnalyticsEnvelope, queueSize),
		flushInterval:   flushInterval,
		ctx:             ctx,
		cancel:          cancel,
		cycles:          make(map[string]*positionCycleState),
		pendingCycleIDs: make(map[string]string),
	}
	if writer.database == "" {
		writer.database = "exchange_analytics"
	}
	writer.wg.Add(1)
	go writer.run()
	return writer, nil
}

// LogRejectedOrder 记录被拒绝的执行事件与订单事件。
func (w *executionClickHouseWriter) LogRejectedOrder(sig *strategypb.Signal, clientID string, requestedQty float64, reduceOnly bool, exchangeName, reason, errorMessage string) {
	if w == nil || sig == nil {
		return
	}
	eventTime := time.Now().UTC()
	cycleID := w.lookupCycleID(sig.GetSymbol(), sig.GetSide(), sig.GetSignalType(), clientID)
	traceID := buildExecutionTraceID(sig, clientID, clientID)
	signalReasonJSON := marshalExecutionSignalReasonJSON(sig.GetSignalReason())
	template := extractExecutionTemplate(sig.GetSignalReason())
	w.enqueueExecutionEvent(executionEventFactRow{
		EventTime:     formatExecutionTime(eventTime),
		AccountID:     w.accountID,
		Symbol:        normalizeAnalyticsText(sig.GetSymbol()),
		StrategyID:    strings.TrimSpace(sig.GetStrategyId()),
		SignalID:      buildExecutionSignalID(sig),
		OrderID:       strings.TrimSpace(clientID),
		ClientOrderID: strings.TrimSpace(clientID),
		Action:        normalizeAnalyticsText(sig.GetAction()),
		Side:          normalizeAnalyticsText(sig.GetSide()),
		SignalType:    normalizeAnalyticsText(sig.GetSignalType()),
		RequestedQty:  requestedQty,
		ExecutedQty:   0,
		Price:         sig.GetEntryPrice(),
		Status:        string(exchange.StatusRejected),
		Exchange:      strings.TrimSpace(exchangeName),
		LatencyMs:     computeExecutionLatency(sig.GetTimestamp(), eventTime.UnixMilli()),
		ErrorCode:     "REJECTED",
		ErrorMessage:  strings.TrimSpace(errorMessage),
		TraceID:       traceID,
	})
	w.enqueueOrderEvent(orderEventFactRow{
		EventTime:        formatExecutionTime(eventTime),
		AccountID:        w.accountID,
		Symbol:           normalizeAnalyticsText(sig.GetSymbol()),
		OrderID:          strings.TrimSpace(clientID),
		ClientOrderID:    strings.TrimSpace(clientID),
		PositionCycleID:  cycleID,
		StrategyID:       strings.TrimSpace(sig.GetStrategyId()),
		Template:         template,
		ActionType:       normalizeAnalyticsText(sig.GetSignalType()),
		Side:             normalizeAnalyticsText(sig.GetSide()),
		OrderType:        string(exchange.OrderTypeMarket),
		Price:            sig.GetEntryPrice(),
		OrigQty:          requestedQty,
		ExecutedQty:      0,
		Status:           string(exchange.StatusRejected),
		ReduceOnly:       boolToAnalyticsUInt8(reduceOnly),
		Reason:           reason,
		SignalReasonJSON: signalReasonJSON,
		TraceID:          traceID,
	})
}

// LogPendingOrder 记录处于 NEW 状态的挂单事件。
func (w *executionClickHouseWriter) LogPendingOrder(sig *strategypb.Signal, result *exchange.OrderResult, requestedQty float64, reduceOnly bool, exchangeName, reason string) {
	if w == nil || sig == nil || result == nil {
		return
	}
	cycleID := w.reservePendingCycleID(sig, result.ClientOrderID)
	traceID := buildExecutionTraceID(sig, result.OrderID, result.ClientOrderID)
	signalReasonJSON := marshalExecutionSignalReasonJSON(sig.GetSignalReason())
	template := extractExecutionTemplate(sig.GetSignalReason())
	eventTime := millisToExecutionTime(result.TransactTime)
	w.enqueueExecutionEvent(executionEventFactRow{
		EventTime:     formatExecutionTime(eventTime),
		AccountID:     w.accountID,
		Symbol:        normalizeAnalyticsText(result.Symbol),
		StrategyID:    strings.TrimSpace(sig.GetStrategyId()),
		SignalID:      buildExecutionSignalID(sig),
		OrderID:       strings.TrimSpace(result.OrderID),
		ClientOrderID: strings.TrimSpace(result.ClientOrderID),
		Action:        normalizeAnalyticsText(sig.GetAction()),
		Side:          normalizeAnalyticsText(sig.GetSide()),
		SignalType:    normalizeAnalyticsText(sig.GetSignalType()),
		RequestedQty:  requestedQty,
		ExecutedQty:   result.ExecutedQuantity,
		Price:         result.AvgPrice,
		Status:        string(result.Status),
		Exchange:      strings.TrimSpace(exchangeName),
		LatencyMs:     computeExecutionLatency(sig.GetTimestamp(), result.TransactTime),
		ErrorCode:     "",
		ErrorMessage:  strings.TrimSpace(result.ErrorMessage),
		TraceID:       traceID,
	})
	w.enqueueOrderEvent(orderEventFactRow{
		EventTime:        formatExecutionTime(eventTime),
		AccountID:        w.accountID,
		Symbol:           normalizeAnalyticsText(result.Symbol),
		OrderID:          strings.TrimSpace(result.OrderID),
		ClientOrderID:    strings.TrimSpace(result.ClientOrderID),
		PositionCycleID:  cycleID,
		StrategyID:       strings.TrimSpace(sig.GetStrategyId()),
		Template:         template,
		ActionType:       normalizeAnalyticsText(sig.GetSignalType()),
		Side:             normalizeAnalyticsText(sig.GetSide()),
		OrderType:        string(exchange.OrderTypeMarket),
		Price:            result.AvgPrice,
		OrigQty:          requestedQty,
		ExecutedQty:      result.ExecutedQuantity,
		Status:           string(result.Status),
		ReduceOnly:       boolToAnalyticsUInt8(reduceOnly),
		Reason:           reason,
		SignalReasonJSON: signalReasonJSON,
		TraceID:          traceID,
	})
}

// LogFilledOrder 记录成交完成后的 execution/order 事件，并更新 position_cycle 快照。
func (w *executionClickHouseWriter) LogFilledOrder(sig *strategypb.Signal, result *exchange.OrderResult, requestedQty float64, reduceOnly bool, exchangeName, reason string) {
	if w == nil || sig == nil || result == nil {
		return
	}
	cycleID := w.recordFilledCycle(sig, result)
	traceID := buildExecutionTraceID(sig, result.OrderID, result.ClientOrderID)
	signalReasonJSON := marshalExecutionSignalReasonJSON(sig.GetSignalReason())
	template := extractExecutionTemplate(sig.GetSignalReason())
	eventTime := millisToExecutionTime(result.TransactTime)
	w.enqueueExecutionEvent(executionEventFactRow{
		EventTime:     formatExecutionTime(eventTime),
		AccountID:     w.accountID,
		Symbol:        normalizeAnalyticsText(result.Symbol),
		StrategyID:    strings.TrimSpace(sig.GetStrategyId()),
		SignalID:      buildExecutionSignalID(sig),
		OrderID:       strings.TrimSpace(result.OrderID),
		ClientOrderID: strings.TrimSpace(result.ClientOrderID),
		Action:        normalizeAnalyticsText(sig.GetAction()),
		Side:          normalizeAnalyticsText(sig.GetSide()),
		SignalType:    normalizeAnalyticsText(sig.GetSignalType()),
		RequestedQty:  requestedQty,
		ExecutedQty:   result.ExecutedQuantity,
		Price:         executionEventPrice(sig, result),
		Status:        string(result.Status),
		Exchange:      strings.TrimSpace(exchangeName),
		LatencyMs:     computeExecutionLatency(sig.GetTimestamp(), result.TransactTime),
		ErrorCode:     "",
		ErrorMessage:  strings.TrimSpace(result.ErrorMessage),
		TraceID:       traceID,
	})
	w.enqueueOrderEvent(orderEventFactRow{
		EventTime:        formatExecutionTime(eventTime),
		AccountID:        w.accountID,
		Symbol:           normalizeAnalyticsText(result.Symbol),
		OrderID:          strings.TrimSpace(result.OrderID),
		ClientOrderID:    strings.TrimSpace(result.ClientOrderID),
		PositionCycleID:  cycleID,
		StrategyID:       strings.TrimSpace(sig.GetStrategyId()),
		Template:         template,
		ActionType:       normalizeAnalyticsText(sig.GetSignalType()),
		Side:             normalizeAnalyticsText(sig.GetSide()),
		OrderType:        string(exchange.OrderTypeMarket),
		Price:            executionEventPrice(sig, result),
		OrigQty:          requestedQty,
		ExecutedQty:      result.ExecutedQuantity,
		Status:           string(result.Status),
		ReduceOnly:       boolToAnalyticsUInt8(reduceOnly),
		Reason:           reason,
		SignalReasonJSON: signalReasonJSON,
		TraceID:          traceID,
	})
}

// Close 停止执行分析后台协程并刷出剩余批次。
func (w *executionClickHouseWriter) Close() error {
	if w == nil {
		return nil
	}
	w.cancel()
	w.wg.Wait()
	return nil
}

// reservePendingCycleID 为开仓挂单预留 position_cycle_id，便于后续成交沿用同一周期。
func (w *executionClickHouseWriter) reservePendingCycleID(sig *strategypb.Signal, clientOrderID string) string {
	if w == nil || sig == nil {
		return ""
	}
	key := executionCycleKey(sig.GetSymbol(), sig.GetSide())
	w.mu.Lock()
	defer w.mu.Unlock()
	if state, ok := w.cycles[key]; ok && state != nil && state.CycleStatus != "CLOSED" {
		return state.PositionCycleID
	}
	if existing := strings.TrimSpace(w.pendingCycleIDs[clientOrderID]); existing != "" {
		return existing
	}
	if strings.ToUpper(strings.TrimSpace(sig.GetSignalType())) != "OPEN" {
		return ""
	}
	cycleID := newExecutionCycleID(sig.GetSymbol(), sig.GetSide(), sig.GetTimestamp(), clientOrderID)
	w.pendingCycleIDs[clientOrderID] = cycleID
	return cycleID
}

// lookupCycleID 查询当前已知的 position_cycle_id，用于拒单或平仓事件补充维度。
func (w *executionClickHouseWriter) lookupCycleID(symbol, positionSide, signalType, clientOrderID string) string {
	if w == nil {
		return ""
	}
	key := executionCycleKey(symbol, positionSide)
	w.mu.Lock()
	defer w.mu.Unlock()
	if cycleID := strings.TrimSpace(w.pendingCycleIDs[clientOrderID]); cycleID != "" {
		return cycleID
	}
	if state, ok := w.cycles[key]; ok && state != nil {
		return state.PositionCycleID
	}
	if strings.ToUpper(strings.TrimSpace(signalType)) == "OPEN" && clientOrderID != "" {
		cycleID := newExecutionCycleID(symbol, positionSide, time.Now().UnixMilli(), clientOrderID)
		w.pendingCycleIDs[clientOrderID] = cycleID
		return cycleID
	}
	return ""
}

// recordFilledCycle 根据成交结果推进仓位生命周期，并落 position_cycle_fact 快照。
func (w *executionClickHouseWriter) recordFilledCycle(sig *strategypb.Signal, result *exchange.OrderResult) string {
	if w == nil || sig == nil || result == nil {
		return ""
	}
	key := executionCycleKey(result.Symbol, positionSideForAnalytics(sig, result))
	signalType := strings.ToUpper(strings.TrimSpace(sig.GetSignalType()))
	template := extractExecutionTemplate(sig.GetSignalReason())
	exitReasonKind, exitReasonLabel := extractExecutionExitReason(sig.GetSignalReason())
	eventTime := millisToExecutionTime(result.TransactTime)
	traceID := buildExecutionTraceID(sig, result.OrderID, result.ClientOrderID)

	w.mu.Lock()
	defer w.mu.Unlock()

	state := w.cycles[key]
	switch signalType {
	case "OPEN", "":
		cycleID := strings.TrimSpace(w.pendingCycleIDs[result.ClientOrderID])
		if state != nil && state.CycleStatus != "CLOSED" {
			cycleID = state.PositionCycleID
		}
		if cycleID == "" {
			cycleID = newExecutionCycleID(result.Symbol, positionSideForAnalytics(sig, result), result.TransactTime, result.ClientOrderID)
		}
		if state == nil || state.CycleStatus == "CLOSED" {
			state = &positionCycleState{
				OpenTime:        eventTime,
				AccountID:       w.accountID,
				Symbol:          normalizeAnalyticsText(result.Symbol),
				PositionCycleID: cycleID,
				StrategyID:      strings.TrimSpace(sig.GetStrategyId()),
				Template:        template,
				PositionSide:    positionSideForAnalytics(sig, result),
				CycleStatus:     "OPEN",
				EntryPrice:      executionEventPrice(sig, result),
				EntryQty:        result.ExecutedQuantity,
				TraceID:         traceID,
			}
		} else {
			state.EntryPrice = weightedAverage(state.EntryPrice, state.EntryQty, executionEventPrice(sig, result), result.ExecutedQuantity)
			state.EntryQty += result.ExecutedQuantity
			state.Template = firstExecutionNonEmpty(template, state.Template)
			state.TraceID = traceID
		}
		delete(w.pendingCycleIDs, result.ClientOrderID)
		w.cycles[key] = state
		w.enqueuePositionCycle(w.buildPositionCycleRow(state))
		return state.PositionCycleID
	case "PARTIAL_CLOSE", "CLOSE":
		if state == nil {
			state = &positionCycleState{
				OpenTime:        eventTime,
				AccountID:       w.accountID,
				Symbol:          normalizeAnalyticsText(result.Symbol),
				PositionCycleID: newExecutionCycleID(result.Symbol, positionSideForAnalytics(sig, result), result.TransactTime, result.ClientOrderID),
				StrategyID:      strings.TrimSpace(sig.GetStrategyId()),
				Template:        template,
				PositionSide:    positionSideForAnalytics(sig, result),
				CycleStatus:     "OPEN",
			}
		}
		state.ExitPrice = weightedAverage(state.ExitPrice, state.ExitQty, executionEventPrice(sig, result), result.ExecutedQuantity)
		state.ExitQty += result.ExecutedQuantity
		state.Template = firstExecutionNonEmpty(template, state.Template)
		state.ExitReasonKind = exitReasonKind
		state.ExitReasonLabel = exitReasonLabel
		state.TraceID = traceID
		if signalType == "PARTIAL_CLOSE" {
			state.PartialCloseOrderCount++
			state.CycleStatus = "PARTIAL_CLOSED"
		} else {
			state.FinalCloseOrderCount++
			state.CycleStatus = "CLOSED"
			closeTime := eventTime
			state.CloseTime = &closeTime
		}
		w.cycles[key] = state
		w.enqueuePositionCycle(w.buildPositionCycleRow(state))
		return state.PositionCycleID
	default:
		if state != nil {
			return state.PositionCycleID
		}
		return ""
	}
}

// buildPositionCycleRow 把内存中的生命周期状态转换为 ClickHouse 行结构。
func (w *executionClickHouseWriter) buildPositionCycleRow(state *positionCycleState) positionCycleFactRow {
	var closeTime *string
	if state != nil && state.CloseTime != nil {
		value := formatExecutionTime(*state.CloseTime)
		closeTime = &value
	}
	return positionCycleFactRow{
		OpenTime:               formatExecutionTime(state.OpenTime),
		CloseTime:              closeTime,
		AccountID:              strings.TrimSpace(state.AccountID),
		Symbol:                 normalizeAnalyticsText(state.Symbol),
		PositionCycleID:        strings.TrimSpace(state.PositionCycleID),
		StrategyID:             strings.TrimSpace(state.StrategyID),
		Template:               strings.TrimSpace(state.Template),
		PositionSide:           normalizeAnalyticsText(state.PositionSide),
		CycleStatus:            normalizeAnalyticsText(state.CycleStatus),
		EntryPrice:             state.EntryPrice,
		ExitPrice:              state.ExitPrice,
		EntryQty:               state.EntryQty,
		ExitQty:                state.ExitQty,
		RealizedPnl:            state.RealizedPnl,
		MaxProfit:              state.MaxProfit,
		MaxDrawdown:            state.MaxDrawdown,
		PartialCloseOrderCount: state.PartialCloseOrderCount,
		FinalCloseOrderCount:   state.FinalCloseOrderCount,
		ExitReasonKind:         strings.TrimSpace(state.ExitReasonKind),
		ExitReasonLabel:        strings.TrimSpace(state.ExitReasonLabel),
		TraceID:                strings.TrimSpace(state.TraceID),
	}
}

// run 后台按表聚合事件并批量写入 ClickHouse。
func (w *executionClickHouseWriter) run() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batches := make(map[string][]interface{})
	flush := func() {
		for table, rows := range batches {
			if len(rows) == 0 {
				continue
			}
			if err := w.insertBatch(table, rows); err != nil {
				log.Printf("[execution-clickhouse] insert batch failed table=%s err=%v", table, err)
			}
			batches[table] = batches[table][:0]
		}
	}

	for {
		select {
		case <-w.ctx.Done():
			for {
				select {
				case item := <-w.queue:
					batches[item.table] = append(batches[item.table], item.row)
				default:
					flush()
					return
				}
			}
		case item := <-w.queue:
			batches[item.table] = append(batches[item.table], item.row)
			if len(batches[item.table]) >= 128 {
				if err := w.insertBatch(item.table, batches[item.table]); err != nil {
					log.Printf("[execution-clickhouse] insert batch failed table=%s err=%v", item.table, err)
				}
				batches[item.table] = batches[item.table][:0]
			}
		case <-ticker.C:
			flush()
		}
	}
}

// insertBatch 把同一张表的一批执行分析事件写入 ClickHouse。
func (w *executionClickHouseWriter) insertBatch(table string, rows []interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("encode %s row: %w", table, err)
		}
	}
	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", w.database, table)
	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, w.endpoint+"/?query="+url.QueryEscape(query), &body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if w.username != "" {
		req.SetBasicAuth(w.username, w.password)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// enqueueExecutionEvent 把 execution_event_fact 行放入异步队列。
func (w *executionClickHouseWriter) enqueueExecutionEvent(row executionEventFactRow) {
	w.enqueueExecutionAnalytics(executionAnalyticsEnvelope{table: "execution_event_fact", row: row})
}

// enqueueOrderEvent 把 order_event_fact 行放入异步队列。
func (w *executionClickHouseWriter) enqueueOrderEvent(row orderEventFactRow) {
	w.enqueueExecutionAnalytics(executionAnalyticsEnvelope{table: "order_event_fact", row: row})
}

// enqueuePositionCycle 把 position_cycle_fact 行放入异步队列。
func (w *executionClickHouseWriter) enqueuePositionCycle(row positionCycleFactRow) {
	w.enqueueExecutionAnalytics(executionAnalyticsEnvelope{table: "position_cycle_fact", row: row})
}

// enqueueExecutionAnalytics 把执行分析事件放入队列，满队列时直接丢弃。
func (w *executionClickHouseWriter) enqueueExecutionAnalytics(item executionAnalyticsEnvelope) {
	if w == nil {
		return
	}
	select {
	case w.queue <- item:
	default:
		log.Printf("[execution-clickhouse] queue full, drop table=%s", item.table)
	}
}

// buildExecutionSignalID 生成执行分析中使用的信号ID。
func buildExecutionSignalID(sig *strategypb.Signal) string {
	if sig == nil {
		return ""
	}
	return fmt.Sprintf("%s-%s-%d", strings.TrimSpace(sig.GetStrategyId()), normalizeAnalyticsText(sig.GetSignalType()), sig.GetTimestamp())
}

// buildExecutionTraceID 生成执行链路统一 trace_id。
func buildExecutionTraceID(sig *strategypb.Signal, orderID, clientOrderID string) string {
	if sig == nil {
		return ""
	}
	base := strings.TrimSpace(orderID)
	if base == "" {
		base = strings.TrimSpace(clientOrderID)
	}
	return fmt.Sprintf("%s-%s-%d-%s", strings.TrimSpace(sig.GetStrategyId()), normalizeAnalyticsText(sig.GetSymbol()), sig.GetTimestamp(), base)
}

// marshalExecutionSignalReasonJSON 把 protobuf signal_reason 转成 JSON 字符串。
func marshalExecutionSignalReasonJSON(reason *strategypb.SignalReason) string {
	payload := orderlogSignalReasonMap(reason)
	if payload == nil {
		return "{}"
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// extractExecutionTemplate 从 signal_reason 中提取模板名。
func extractExecutionTemplate(reason *strategypb.SignalReason) string {
	if reason == nil {
		return ""
	}
	if strings.TrimSpace(reason.GetRouteTemplate()) != "" {
		return strings.TrimSpace(reason.GetRouteTemplate())
	}
	if allocator := reason.GetAllocator(); allocator != nil {
		return strings.TrimSpace(allocator.GetTemplate())
	}
	return ""
}

// extractExecutionExitReason 提取结构化出场原因。
func extractExecutionExitReason(reason *strategypb.SignalReason) (string, string) {
	if reason == nil {
		return "", ""
	}
	return strings.TrimSpace(reason.GetExitReasonKind()), strings.TrimSpace(reason.GetExitReasonLabel())
}

// executionEventPrice 优先使用成交价，缺失时回退到信号价格。
func executionEventPrice(sig *strategypb.Signal, result *exchange.OrderResult) float64 {
	if result != nil && result.AvgPrice > 0 {
		return result.AvgPrice
	}
	if sig != nil {
		return sig.GetEntryPrice()
	}
	return 0
}

// computeExecutionLatency 计算从信号产生到执行事件的延迟。
func computeExecutionLatency(signalTimestamp, eventTimestamp int64) uint32 {
	if signalTimestamp <= 0 || eventTimestamp <= 0 || eventTimestamp <= signalTimestamp {
		return 0
	}
	latency := eventTimestamp - signalTimestamp
	if latency < 0 {
		return 0
	}
	return uint32(latency)
}

// millisToExecutionTime 把毫秒时间戳转换为 UTC 时间。
func millisToExecutionTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Now().UTC()
	}
	return time.UnixMilli(ms).UTC()
}

// formatExecutionTime 把时间统一格式化为 ClickHouse DateTime64(3) 字符串。
func formatExecutionTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000")
}

// executionCycleKey 构造仓位生命周期索引键。
func executionCycleKey(symbol, positionSide string) string {
	return normalizeAnalyticsText(symbol) + "|" + normalizeAnalyticsText(positionSide)
}

// positionSideForAnalytics 统一获取生命周期跟踪使用的持仓方向。
func positionSideForAnalytics(sig *strategypb.Signal, result *exchange.OrderResult) string {
	if result != nil && strings.TrimSpace(string(result.PositionSide)) != "" && result.PositionSide != exchange.PosBoth {
		return string(result.PositionSide)
	}
	if sig != nil && strings.TrimSpace(sig.GetSide()) != "" {
		return sig.GetSide()
	}
	return string(exchange.PosBoth)
}

// newExecutionCycleID 生成新的 position_cycle_id。
func newExecutionCycleID(symbol, positionSide string, ts int64, discriminator string) string {
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	return fmt.Sprintf("%s-%s-%d-%s", normalizeAnalyticsText(symbol), normalizeAnalyticsText(positionSide), ts, strings.TrimSpace(discriminator))
}

// weightedAverage 计算加权平均价，便于处理加仓与分批平仓。
func weightedAverage(oldPrice, oldQty, newPrice, newQty float64) float64 {
	if oldQty <= 0 {
		return newPrice
	}
	if newQty <= 0 {
		return oldPrice
	}
	totalQty := oldQty + newQty
	if totalQty <= 0 {
		return newPrice
	}
	return (oldPrice*oldQty + newPrice*newQty) / totalQty
}

// normalizeAnalyticsText 把分析表中的枚举与维度统一成大写去空格形式。
func normalizeAnalyticsText(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

// boolToAnalyticsUInt8 把布尔值转换成 ClickHouse UInt8。
func boolToAnalyticsUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

// firstExecutionNonEmpty 返回第一个非空字符串。
func firstExecutionNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
