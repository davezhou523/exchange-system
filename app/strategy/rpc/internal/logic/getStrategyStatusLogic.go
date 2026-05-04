package logic

import (
	"context"
	"time"

	"exchange-system/app/strategy/rpc/internal/svc"
	"exchange-system/app/strategy/rpc/internal/universe"
	"exchange-system/common/pb/strategy"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetStrategyStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetStrategyStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetStrategyStatusLogic {
	return &GetStrategyStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取策略状态
func (l *GetStrategyStatusLogic) GetStrategyStatus(in *strategy.StrategyRequest) (*strategy.StrategyStatus, error) {
	strategyId := in.GetStrategyId()
	hasStrategy := l.svcCtx.HasStrategy(strategyId)
	status := "STOPPED"
	messageCode := "not_running"
	var allocator *strategy.PositionAllocatorStatus
	var router *strategy.StrategyRouteRuntimeStatus
	if hasStrategy {
		status = "RUNNING"
		messageCode = "running"
	}
	if desired, ok := l.svcCtx.LatestUniverseDesired(strategyId); ok {
		router = &strategy.StrategyRouteRuntimeStatus{
			Enabled:          desired.Enabled,
			Template:         desired.Template,
			RouteBucket:      desired.Bucket,
			TargetReason:     desired.Reason,
			TargetReasonDesc: describeStatusReason(desired.Reason),
			BaseTemplate:     desired.BaseTemplate,
		}
	}
	if result, ok := l.svcCtx.LatestUniverseApplyResult(strategyId); ok {
		if router == nil {
			router = &strategy.StrategyRouteRuntimeStatus{}
		}
		router.RuntimeEnabled = result.Enabled
		router.RuntimeTemplate = result.RuntimeTemplate
		router.ApplyAction = result.Action
		router.ApplyActionDesc = describeStatusAction(result.Action)
		router.ApplyGateReason = result.Reason
		router.ApplyGateReasonDesc = describeStatusReason(result.Reason)
		router.HasStrategy = result.HasStrategy
		router.HasOpenPosition = result.HasOpenPosition
	}
	if snapshot, ok := l.svcCtx.LatestUniverseSnapshot(strategyId); ok {
		if router == nil {
			router = &strategy.StrategyRouteRuntimeStatus{}
		}
		router.RegimeFusion = buildRegimeFusionStatus(snapshot)
	}
	if warmup := buildStrategyWarmupStatus(l.svcCtx.StrategyWarmupStatus(strategyId)); warmup != nil {
		if router == nil {
			router = &strategy.StrategyRouteRuntimeStatus{}
		}
		router.Warmup = warmup
	}
	if latestSecondBar := buildLatestSecondBarStatus(l.svcCtx, strategyId); latestSecondBar != nil {
		if router == nil {
			router = &strategy.StrategyRouteRuntimeStatus{}
		}
		router.LatestSecondBar = latestSecondBar
	}
	if rec, ok := l.svcCtx.LatestWeightRecommendation(strategyId); ok {
		allocator = &strategy.PositionAllocatorStatus{
			Template:        rec.Template,
			RouteBucket:     rec.Bucket,
			RouteReason:     rec.RouteReason,
			RouteReasonDesc: describeStatusReason(rec.RouteReason),
			Score:           rec.Score,
			ScoreSource:     rec.ScoreSource,
			BucketBudget:    rec.BucketBudget,
			StrategyWeight:  rec.StrategyWeight,
			SymbolWeight:    rec.SymbolWeight,
			RiskScale:       rec.RiskScale,
			PositionBudget:  rec.PositionBudget,
			TradingPaused:   rec.TradingPaused,
			PauseReason:     rec.PauseReason,
			PauseReasonDesc: describeStatusReason(rec.PauseReason),
		}
		if rec.TradingPaused {
			messageCode = "allocator_paused"
		} else {
			messageCode = "allocator_ready"
		}
	}
	if router != nil && !router.HasStrategy {
		router.HasStrategy = hasStrategy
	}
	return &strategy.StrategyStatus{
		StrategyId:  strategyId,
		Status:      status,
		StatusDesc:  describeServiceStatus(status),
		Message:     statusMessageSummary(messageCode),
		MessageCode: messageCode,
		MessageDesc: describeStatusMessage(messageCode),
		LastUpdate:  time.Now().UnixMilli(),
		Allocator:   allocator,
		Router:      router,
	}, nil
}

// statusMessageSummary 为状态接口生成稳定且简短的人类可读摘要，避免继续拼接长字符串。
func statusMessageSummary(code string) string {
	switch code {
	case "started":
		return "started"
	case "stopped":
		return "stopped"
	case "not_running":
		return "not running"
	case "allocator_ready":
		return "allocator ready"
	case "allocator_paused":
		return "allocator paused"
	case "running":
		fallthrough
	default:
		return "running"
	}
}

// buildStrategyWarmupStatus 把 ServiceContext 的多周期历史长度快照转换成 RPC 结构，便于状态接口直接暴露 warmup 进度。
func buildStrategyWarmupStatus(status svc.StrategyWarmupStatusView) *strategy.StrategyWarmupStatus {
	return &strategy.StrategyWarmupStatus{
		HistoryLen_4H:     status.HistoryLen4h,
		HistoryLen_1H:     status.HistoryLen1h,
		HistoryLen_15M:    status.HistoryLen15m,
		HistoryLen_1M:     status.HistoryLen1m,
		Source:            status.Source,
		Status:            status.Status,
		IncompleteReasons: append([]string(nil), status.IncompleteReasons...),
	}
}

// buildLatestSecondBarStatus 把最近一条秒级行情视图转换成 RPC 结构，便于外部直接确认 1s fallback 来源。
func buildLatestSecondBarStatus(svcCtx *svc.ServiceContext, symbol string) *strategy.LatestSecondBarStatus {
	if svcCtx == nil || symbol == "" {
		return nil
	}
	view, ok := svcCtx.LatestSecondBarStatus(symbol)
	if !ok {
		return nil
	}
	return &strategy.LatestSecondBarStatus{
		OpenTimeMs:  view.OpenTimeMs,
		CloseTimeMs: view.CloseTimeMs,
		Open:        view.Open,
		High:        view.High,
		Low:         view.Low,
		Close:       view.Close,
		Volume:      view.Volume,
		IsFinal:     view.IsFinal,
		Synthetic:   view.Synthetic,
		Source:      view.Source,
	}
}

// buildRegimeFusionStatus 把 Universe 多周期融合快照转换成 RPC 可直接返回的结构化状态。
func buildRegimeFusionStatus(snapshot universe.Snapshot) *strategy.RegimeFusionStatus {
	if snapshot.Fusion.FusedState == "" &&
		snapshot.Regime1h.Interval == "" &&
		snapshot.Regime15m.Interval == "" {
		return nil
	}
	return &strategy.RegimeFusionStatus{
		H1:              buildRegimeFrameStatus(snapshot.Regime1h),
		M15:             buildRegimeFrameStatus(snapshot.Regime15m),
		FusedState:      string(snapshot.Fusion.FusedState),
		FusedReason:     snapshot.Fusion.FusedReason,
		FusedReasonDesc: describeStatusReason(snapshot.Fusion.FusedReason),
		FusedScore:      snapshot.Fusion.FusedScore,
		PrimaryWeight:   snapshot.Fusion.PrimaryWeight,
		ConfirmWeight:   snapshot.Fusion.ConfirmWeight,
		LastUpdate:      snapshot.Fusion.UpdatedAt.UnixMilli(),
	}
}

// buildRegimeFrameStatus 把单周期状态压缩成 proto 结构，避免 query 层重复理解内部模型。
func buildRegimeFrameStatus(frame universe.RegimeFrame) *strategy.RegimeFrameStatus {
	if frame.Interval == "" &&
		frame.State == "" &&
		frame.Reason == "" &&
		frame.RouteReason == "" &&
		frame.UpdatedAt.IsZero() &&
		!frame.Healthy &&
		!frame.Fresh {
		return nil
	}
	return &strategy.RegimeFrameStatus{
		Interval:        frame.Interval,
		State:           string(frame.State),
		Reason:          frame.Reason,
		ReasonDesc:      describeStatusReason(frame.Reason),
		RouteReason:     frame.RouteReason,
		RouteReasonDesc: describeStatusReason(frame.RouteReason),
		Confidence:      frame.Confidence,
		LastUpdate:      frame.UpdatedAt.UnixMilli(),
		Healthy:         frame.Healthy,
		Fresh:           frame.Fresh,
	}
}
