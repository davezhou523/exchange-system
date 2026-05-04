package logic

import (
	"context"
	"testing"

	"exchange-system/app/strategy/rpc/internal/svc"
	strategypb "exchange-system/common/pb/strategy"
)

// TestStartStrategyReturnsStatusDesc 验证启动接口会同时返回英文状态码和中文说明。
func TestStartStrategyReturnsStatusDesc(t *testing.T) {
	logic := NewStartStrategyLogic(context.Background(), &svc.ServiceContext{})
	got, err := logic.StartStrategy(&strategypb.StrategyConfig{Name: "demo"})
	if err != nil {
		t.Fatalf("StartStrategy() error = %v", err)
	}
	if got.Status != "RUNNING" || got.StatusDesc != "运行中" {
		t.Fatalf("status = %q/%q, want RUNNING/运行中", got.Status, got.StatusDesc)
	}
	if got.Message != "started" || got.MessageCode != "started" || got.MessageDesc != "策略已启动" {
		t.Fatalf("message fields = %q/%q/%q, want started summary", got.Message, got.MessageCode, got.MessageDesc)
	}
}

// TestStopStrategyReturnsStatusDesc 验证停止接口会同时返回英文状态码和中文说明。
func TestStopStrategyReturnsStatusDesc(t *testing.T) {
	logic := NewStopStrategyLogic(context.Background(), &svc.ServiceContext{})
	got, err := logic.StopStrategy(&strategypb.StrategyRequest{StrategyId: "demo"})
	if err != nil {
		t.Fatalf("StopStrategy() error = %v", err)
	}
	if got.Status != "STOPPED" || got.StatusDesc != "已停止" {
		t.Fatalf("status = %q/%q, want STOPPED/已停止", got.Status, got.StatusDesc)
	}
	if got.Message != "stopped" || got.MessageCode != "stopped" || got.MessageDesc != "策略已停止" {
		t.Fatalf("message fields = %q/%q/%q, want stopped summary", got.Message, got.MessageCode, got.MessageDesc)
	}
}
