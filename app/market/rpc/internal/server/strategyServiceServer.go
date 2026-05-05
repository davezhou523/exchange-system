package server

import (
	"context"
	"fmt"

	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/svc"
	"exchange-system/common/pb/strategy"
)

// StrategyServiceServer 实现 strategy.StrategyServiceServer，注册在 market 服务的 gRPC server 上。
type StrategyServiceServer struct {
	engine *svc.StrategyEngine
	strategy.UnimplementedStrategyServiceServer
}

// NewStrategyServiceServer 创建策略 gRPC 服务端。
func NewStrategyServiceServer(svcCtx *svc.ServiceContext) *StrategyServiceServer {
	return &StrategyServiceServer{
		engine: svcCtx.StrategyEngine,
	}
}

// StartStrategy 启动策略。
func (s *StrategyServiceServer) StartStrategy(ctx context.Context, in *strategy.StrategyConfig) (*strategy.StrategyStatus, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("strategy engine not initialized")
	}
	err := s.engine.StartStrategy(config.StrategyConfig{
		Name:       in.GetName(),
		Symbol:     in.GetSymbol(),
		Enabled:    in.GetEnabled(),
		Parameters: in.GetParameters(),
	}, nil)
	if err != nil {
		return nil, err
	}
	return &strategy.StrategyStatus{
		StrategyId: in.GetSymbol(),
		Status:     "RUNNING",
	}, nil
}

// StopStrategy 停止策略。
func (s *StrategyServiceServer) StopStrategy(ctx context.Context, in *strategy.StrategyRequest) (*strategy.StrategyStatus, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("strategy engine not initialized")
	}
	err := s.engine.StopStrategy(in.GetStrategyId())
	if err != nil {
		return nil, err
	}
	return &strategy.StrategyStatus{
		StrategyId: in.GetStrategyId(),
		Status:     "STOPPED",
	}, nil
}

// GetStrategyStatus 获取策略状态。
func (s *StrategyServiceServer) GetStrategyStatus(ctx context.Context, in *strategy.StrategyRequest) (*strategy.StrategyStatus, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("strategy engine not initialized")
	}
	return s.engine.GetStrategyStatus(in.GetStrategyId())
}

// StreamSignals 获取策略信号流（暂未实现）。
func (s *StrategyServiceServer) StreamSignals(in *strategy.StrategyRequest, stream strategy.StrategyService_StreamSignalsServer) error {
	return fmt.Errorf("StreamSignals not yet implemented in merged market service")
}
