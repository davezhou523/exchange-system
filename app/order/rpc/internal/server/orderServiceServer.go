package server

import (
	"context"

	"exchange-system/app/order/rpc/internal/logic"
	"exchange-system/app/order/rpc/internal/svc"
	pb "exchange-system/common/pb/order"
)

// ---------------------------------------------------------------------------
// OrderServiceServer — gRPC 服务端
// 实现 OrderService 接口，委托给 logic 层处理
// ---------------------------------------------------------------------------

// OrderServiceServer Order 服务 gRPC 实现
type OrderServiceServer struct {
	svcCtx *svc.ServiceContext
	pb.UnimplementedOrderServiceServer
}

// NewOrderServiceServer 创建 Order 服务实例
func NewOrderServiceServer(svcCtx *svc.ServiceContext) *OrderServiceServer {
	return &OrderServiceServer{
		svcCtx: svcCtx,
	}
}

// GetOpenOrders 查询当前委托（未成交订单）
func (s *OrderServiceServer) GetOpenOrders(ctx context.Context, in *pb.OrderQueryRequest) (*pb.OpenOrderResponse, error) {
	l := logic.NewGetOpenOrdersLogic(ctx, s.svcCtx)
	return l.GetOpenOrders(in)
}

// GetAllOrders 查询历史委托
func (s *OrderServiceServer) GetAllOrders(ctx context.Context, in *pb.OrderQueryRequest) (*pb.AllOrderResponse, error) {
	l := logic.NewGetAllOrdersLogic(ctx, s.svcCtx)
	return l.GetAllOrders(in)
}

// GetUserTrades 查询历史成交
func (s *OrderServiceServer) GetUserTrades(ctx context.Context, in *pb.OrderQueryRequest) (*pb.UserTradeResponse, error) {
	l := logic.NewGetUserTradesLogic(ctx, s.svcCtx)
	return l.GetUserTrades(in)
}

// GetIncomeHistory 查询资金流水
func (s *OrderServiceServer) GetIncomeHistory(ctx context.Context, in *pb.OrderQueryRequest) (*pb.IncomeResponse, error) {
	l := logic.NewGetIncomeHistoryLogic(ctx, s.svcCtx)
	return l.GetIncomeHistory(in)
}

// GetFundingRateHistory 查询资金费率历史
func (s *OrderServiceServer) GetFundingRateHistory(ctx context.Context, in *pb.OrderQueryRequest) (*pb.FundingRateResponse, error) {
	l := logic.NewGetFundingRateHistoryLogic(ctx, s.svcCtx)
	return l.GetFundingRateHistory(in)
}

// GetFundingFees 查询资金费用
func (s *OrderServiceServer) GetFundingFees(ctx context.Context, in *pb.OrderQueryRequest) (*pb.FundingFeeResponse, error) {
	l := logic.NewGetFundingFeesLogic(ctx, s.svcCtx)
	return l.GetFundingFees(in)
}

// FetchAllData 拉取全部数据并保存到本地 JSONL
func (s *OrderServiceServer) FetchAllData(ctx context.Context, in *pb.OrderQueryRequest) (*pb.EmptyRequest, error) {
	l := logic.NewFetchAllDataLogic(ctx, s.svcCtx)
	return l.FetchAllData(in)
}
