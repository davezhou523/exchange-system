package svc

import (
	"context"
	"fmt"
	"log"
	"time"

	"exchange-system/app/execution/rpc/internal/config"
	"exchange-system/app/execution/rpc/internal/exchange"
	"exchange-system/app/execution/rpc/internal/idempotent"
	"exchange-system/app/execution/rpc/internal/kafka"
	"exchange-system/app/execution/rpc/internal/order"
	"exchange-system/app/execution/rpc/internal/orderlog"
	"exchange-system/app/execution/rpc/internal/position"
	"exchange-system/app/execution/rpc/internal/risk"
	marketpb "exchange-system/common/pb/market"
	strategypb "exchange-system/common/pb/strategy"
)

// ---------------------------------------------------------------------------
// ServiceContext 执行服务核心
//
// 职责：
//   1. 接收策略信号 (Kafka signal topic)
//   2. 幂等检查（防重复下单）
//   3. 风控检查（仓位/杠杆/日亏损/敞口）
//   4. 路由下单（Binance / OKX / 模拟撮合）
//   5. 订单管理（状态跟踪）
//   6. 仓位管理（同步更新）
//   7. 发布订单结果 (Kafka order topic)
//   8. 1m K线驱动撮合（模拟撮合模式下）
// ---------------------------------------------------------------------------

// ServiceContext 执行服务上下文
type ServiceContext struct {
	Config config.Config

	// 核心组件
	router       *exchange.Router               // 下单路由器
	riskManager  *risk.Manager                  // 风控管理器
	orderManager *order.Manager                 // 订单管理器
	posManager   *position.Manager              // 仓位管理器
	deduplicator *idempotent.SignalDeduplicator // 幂等去重器
	logger       *orderlog.Logger               // 日志记录器

	// 模拟撮合引擎引用（1m K线模式下使用）
	simExchange *exchange.SimulatedExchange

	// Kafka
	signalConsumer *kafka.Consumer
	klineConsumer  *kafka.KlineConsumer // 1m K线消费者（1m撮合模式）
	orderProducer  *kafka.Producer

	// 生命周期
	cancel context.CancelFunc
}

// NewServiceContext 创建执行服务上下文
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 1. 创建 Kafka 订单生产者
	orderProducer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topics.Order)
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
	)
	router.Register("binance", binanceClient)

	// 注册 OKX 交易所（占位）
	if c.Exchange.OKX.APIKey != "" {
		okxClient := exchange.NewOKXClient(exchange.OKXConfig{
			APIKey:     c.Exchange.OKX.APIKey,
			SecretKey:  c.Exchange.OKX.SecretKey,
			Passphrase: c.Exchange.OKX.Passphrase,
			BaseURL:    c.Exchange.OKX.BaseURL,
			Simulated:  c.Exchange.OKX.Simulated,
		})
		router.Register("okx", okxClient)
	}

	// 注册模拟撮合引擎
	var simExchange *exchange.SimulatedExchange
	if c.Exchange.Simulated.Enabled {
		matchMode := exchange.SimMatchMode(c.Exchange.Simulated.MatchMode)
		simConfig := exchange.SimConfig{
			InitialBalance:  c.Exchange.Simulated.InitialBalance,
			SlippageBPS:     c.Exchange.Simulated.SlippageBPS,
			SlippageModel:   c.Exchange.Simulated.SlippageModel,
			CommissionRate:  c.Exchange.Simulated.CommissionRate,
			CommissionAsset: c.Exchange.Simulated.CommissionAsset,
			FillDelayMs:     c.Exchange.Simulated.FillDelayMs,
			MatchMode:       matchMode,
		}
		simExchange = exchange.NewSimulatedExchange(simConfig)
		router.Register("simulated", simExchange)
		log.Printf("[初始化] 模拟撮合引擎已注册 | 余额=%.0f 滑点=%.0fbps 手续费=%.2f%% 撮合模式=%s",
			simConfig.InitialBalance, simConfig.SlippageBPS, simConfig.CommissionRate*100, simConfig.MatchMode)
	}

	// 4. 创建仓位管理器和订单管理器
	posManager := position.NewManager()
	orderManager := order.NewManager()

	// 5. 创建风控管理器
	riskConfig := risk.RiskConfig{
		MaxPositionSize:     c.Risk.MaxPositionSize,
		MaxLeverage:         c.Risk.MaxLeverage,
		MaxDailyLossPct:     c.Risk.MaxDailyLossPct,
		MaxDrawdownPct:      c.Risk.MaxDrawdownPct,
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

	// 8. 构建服务上下文
	svcCtx := &ServiceContext{
		Config:         c,
		router:         router,
		riskManager:    riskManager,
		orderManager:   orderManager,
		posManager:     posManager,
		deduplicator:   deduplicator,
		logger:         logger,
		simExchange:    simExchange,
		signalConsumer: signalConsumer,
		orderProducer:  orderProducer,
		cancel:         cancel,
	}

	// 9. 1m K线撮合模式下，启动1m K线消费者
	var klineConsumer *kafka.KlineConsumer
	if simExchange != nil && simExchange.GetMatchMode() == exchange.MatchModeKline1m && c.Kafka.Topics.Kline != "" {
		klineGroupID := groupID + "-1m"
		klineConsumer, err = kafka.NewKlineConsumer(c.Kafka.Addrs, klineGroupID, c.Kafka.Topics.Kline)
		if err != nil {
			_ = signalConsumer.Close()
			_ = orderProducer.Close()
			logger.Close()
			cancel()
			return nil, fmt.Errorf("create kline consumer: %v", err)
		}

		// 设置模拟撮合成交回调
		simExchange.SetOnFillCallback(func(result *exchange.OrderResult) {
			svcCtx.handleSimFill(result)
		})

		// 启动1m K线消费
		if err := klineConsumer.StartConsuming(ctx, func(kline *marketpb.Kline) error {
			return svcCtx.handleKline1m(kline)
		}); err != nil {
			_ = klineConsumer.Close()
			_ = signalConsumer.Close()
			_ = orderProducer.Close()
			logger.Close()
			cancel()
			return nil, fmt.Errorf("start kline consumer: %v", err)
		}

		svcCtx.klineConsumer = klineConsumer
		log.Printf("[初始化] 1m K线撮合消费者已启动 | topic=%s group=%s", c.Kafka.Topics.Kline, klineGroupID)
	}

	// 10. 启动信号消费
	if err := signalConsumer.StartConsuming(ctx, func(sig *strategypb.Signal) error {
		return svcCtx.HandleSignal(ctx, sig)
	}); err != nil {
		_ = signalConsumer.Close()
		if klineConsumer != nil {
			_ = klineConsumer.Close()
		}
		_ = orderProducer.Close()
		logger.Close()
		cancel()
		return nil, fmt.Errorf("start signal consumer: %v", err)
	}

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

	// 2. 风控检查（仅开仓信号需要，平仓信号跳过）
	if signalType == "OPEN" || signalType == "" {
		if err := s.performRiskCheck(ctx, symbol, action, quantity, entryPrice); err != nil {
			// 风控拒绝时移除幂等记录，允许后续重试
			s.deduplicator.Remove(sigKey)
			return fmt.Errorf("risk check failed: %v", err)
		}
	}

	// 3. 确定订单参数
	orderSide := mapActionToSide(action)
	posSide := mapSideToPositionSide(side)

	// 平仓信号：使用 reduce_only 参数（交易所会自动平掉对应方向的仓位）
	reduceOnly := signalType == "CLOSE"

	// 4. 路由下单
	orderResult, err := s.router.CreateOrder(ctx, exchange.CreateOrderParam{
		Symbol:       symbol,
		Side:         orderSide,
		PositionSide: posSide,
		Type:         exchange.OrderTypeMarket,
		Quantity:     quantity,
		Price:        entryPrice,
		ClientID:     fmt.Sprintf("sig-%s-%d", strategyID, sig.GetTimestamp()),
		ReduceOnly:   reduceOnly,
	})
	if err != nil {
		// 下单失败时移除幂等记录，允许重试
		s.deduplicator.Remove(sigKey)
		return fmt.Errorf("create order failed: %v", err)
	}

	// 5. 1m K线撮合模式：订单仅挂起，等待1m K线驱动成交
	//    即时模式：订单已成交，直接处理后续逻辑
	if orderResult.Status == exchange.StatusNew {
		// 1m撮合模式，订单挂起中
		log.Printf("[信号] 订单挂起等待撮合 | type=%s ID=%s symbol=%s",
			signalType, orderResult.OrderID, symbol)

		// 标记幂等记录的订单ID
		s.deduplicator.MarkOrderID(sigKey, orderResult.OrderID)

		// 记录到订单管理器（状态为 NEW）
		s.orderManager.AddOrder(&order.OrderState{
			OrderID:      orderResult.OrderID,
			ClientID:     orderResult.ClientOrderID,
			Symbol:       symbol,
			Side:         orderResult.Side,
			PositionSide: orderResult.PositionSide,
			Type:         exchange.OrderTypeMarket,
			Status:       exchange.StatusNew,
			Quantity:     quantity,
			StrategyID:   strategyID,
			SignalKey:    sigKey,
			SignalType:   signalType,
			CreateTime:   time.Now(),
			TransactTime: time.UnixMilli(orderResult.TransactTime),
			StopLoss:     stopLoss,
			TakeProfits:  takeProfits,
			Atr:          atr,
			RiskReward:   riskReward,
		})

		// 1m撮合模式：开仓信号设置止损止盈，等成交后由回调处理后续逻辑
		// 止损止盈会在成交回调中设置
		return nil
	}

	// 即时成交模式：处理已成交订单的后续逻辑
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
	})

	// 更新仓位管理器
	s.posManager.UpdateFromOrder(orderResult, strategyID, stopLoss, takeProfits)

	// 发布订单结果到 Kafka
	orderEvent := s.buildOrderEvent(sig, orderResult)
	if err := s.orderProducer.SendMarketData(context.Background(), orderEvent); err != nil {
		log.Printf("[信号] 发布订单结果失败: %v", err)
	}

	log.Printf("[信号] 订单完成 | type=%s ID=%s 状态=%s 成交=%.4f@%.2f 手续费=%.4f 滑点=%.4f",
		signalType, orderResult.OrderID, orderResult.Status,
		orderResult.ExecutedQuantity, orderResult.AvgPrice,
		orderResult.Commission, orderResult.Slippage)

	// 记录订单执行结果日志
	s.logger.LogOrder(sig, orderResult)

	// 1m撮合模式：开仓成交后设置止损止盈
	if s.simExchange != nil && s.simExchange.GetMatchMode() == exchange.MatchModeKline1m && signalType == "OPEN" {
		posSide := mapSideToPositionSide(sig.GetSide())
		s.simExchange.SetPositionSLTP(symbol, posSide, orderResult.ExecutedQuantity, stopLoss, takeProfits, strategyID)
	}
}

// ---------------------------------------------------------------------------
// 1m K线驱动撮合
// ---------------------------------------------------------------------------

// handleKline1m 处理1m K线，驱动模拟撮合
func (s *ServiceContext) handleKline1m(kline *marketpb.Kline) error {
	if kline == nil || s.simExchange == nil {
		return nil
	}

	// 将1m K线数据传给模拟撮合引擎
	s.simExchange.OnKline1m(
		kline.Symbol,
		kline.Open,
		kline.High,
		kline.Low,
		kline.Close,
		kline.OpenTime,
	)

	return nil
}

// handleSimFill 处理模拟撮合成交回调
//
// 1m K线模式下，订单撮合完成后回调此方法，更新订单管理器、仓位管理器、发布Kafka结果
func (s *ServiceContext) handleSimFill(result *exchange.OrderResult) {
	if result == nil {
		return
	}

	log.Printf("[1m撮合回调] 订单成交 | ID=%s symbol=%s side=%s 成交=%.4f@%.2f",
		result.OrderID, result.Symbol, result.Side,
		result.ExecutedQuantity, result.AvgPrice)

	// 1. 更新订单管理器中的订单状态
	if err := s.orderManager.UpdateOrder(result.OrderID, result); err != nil {
		log.Printf("[1m撮合回调] 更新订单失败: %v", err)
	}

	// 2. 更新仓位管理器
	s.posManager.UpdateFromOrder(result, "", 0, nil)

	// 3. 构建简化信号并记录日志
	sig := &strategypb.Signal{
		Symbol:   result.Symbol,
		Action:   string(result.Side),
		Side:     string(result.PositionSide),
		Quantity: result.ExecutedQuantity,
	}
	s.logger.LogOrder(sig, result)
}

// performRiskCheck 执行风控检查
func (s *ServiceContext) performRiskCheck(ctx context.Context, symbol, action string, quantity, price float64) error {
	// 获取账户信息
	account, err := s.router.GetAccountInfo(ctx)
	if err != nil {
		return fmt.Errorf("get account info: %v", err)
	}

	// 同步仓位到管理器
	s.posManager.UpdateFromExchange(account)

	// 执行风控检查
	result := s.riskManager.CheckPreOrder(account, symbol, action, quantity, price)
	if !result.Passed {
		return fmt.Errorf("risk rejected: %v", result.Reasons)
	}
	return nil
}

// buildOrderEvent 构建订单结果事件，发布到 Kafka order topic
func (s *ServiceContext) buildOrderEvent(sig *strategypb.Signal, result *exchange.OrderResult) map[string]interface{} {
	return map[string]interface{}{
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
		"reason":           sig.GetReason(),
		"atr":              sig.GetAtr(),
		"risk_reward":      sig.GetRiskReward(),
		"indicators":       sig.GetIndicators(),
	}
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
func (s *ServiceContext) GetAccountInfoViaGRPC(ctx context.Context) (*exchange.AccountResult, error) {
	if s == nil || s.router == nil {
		return nil, fmt.Errorf("execution service not initialized")
	}
	return s.router.GetAccountInfo(ctx)
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
	if s.klineConsumer != nil {
		if err := s.klineConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.signalConsumer != nil {
		if err := s.signalConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.orderProducer != nil {
		if err := s.orderProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
