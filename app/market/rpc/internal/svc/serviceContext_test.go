package svc

import (
	"context"
	"testing"

	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/common/pb/market"
)

type stubSnapshotUpdater struct {
	last *market.Kline
}

type stubReplayableAggregator struct {
	onKlines     []*market.Kline
	replayKlines []*market.Kline
}

type stubHistoryWarmupper struct {
	rows  [][]interface{}
	calls int
}

// UpdateSnapshotFromKline 记录 dispatcher 透传给 UniversePool 的最新闭合 K 线。
func (s *stubSnapshotUpdater) UpdateSnapshotFromKline(k *market.Kline) {
	s.last = k
}

// OnKline 记录 dispatcher 透传给聚合器的实时闭合 1m K 线。
func (s *stubReplayableAggregator) OnKline(_ context.Context, k *market.Kline) {
	s.onKlines = append(s.onKlines, k)
}

// ReplayKline 记录 dispatcher 触发的缺口回放 K 线顺序。
func (s *stubReplayableAggregator) ReplayKline(_ context.Context, k *market.Kline) {
	s.replayKlines = append(s.replayKlines, k)
}

// FetchKlines 返回空结果，占位满足接口。
func (s *stubHistoryWarmupper) FetchKlines(_ context.Context, _, _ string, _ int) ([][]interface{}, error) {
	return nil, nil
}

// FetchKlinesRange 记录在线补档调用，并返回测试注入的 Binance 原始 K 线数组。
func (s *stubHistoryWarmupper) FetchKlinesRange(_ context.Context, _, _ string, _, _ int64, _ int) ([][]interface{}, error) {
	s.calls++
	return s.rows, nil
}

func TestKlineDispatcherOnKlineUpdatesUniverseBeforeAggregator(t *testing.T) {
	updater := &stubSnapshotUpdater{}
	agg := &stubReplayableAggregator{}
	dispatcher := &klineDispatcher{
		agg:         agg,
		universeMgr: updater,
	}

	k := &market.Kline{
		Symbol:   "BTCUSDT",
		Interval: "1m",
		IsClosed: true,
		Close:    95000,
	}
	dispatcher.OnKline(context.Background(), k)

	if updater.last == nil {
		t.Fatal("universe snapshot updater was not called")
	}
	if updater.last != k {
		t.Fatal("dispatcher forwarded unexpected kline instance")
	}
	if len(agg.onKlines) != 1 || agg.onKlines[0] != k {
		t.Fatal("dispatcher did not forward live kline to aggregator")
	}
}

// TestKlineDispatcherOnKlineBackfillsMissing1m 验证实时 1m 跳分钟时会先用 REST 拉取缺口，再顺序 Replay 回灌到聚合器。
func TestKlineDispatcherOnKlineBackfillsMissing1m(t *testing.T) {
	warmupper := &stubHistoryWarmupper{
		rows: [][]interface{}{
			{float64(1778074320000), "100", "101", "99", "100.5", "10", float64(1778074379999), "1005", float64(12), "4", "402"},
			{float64(1778074380000), "100.5", "102", "100", "101.2", "11", float64(1778074439999), "1113.2", float64(13), "5", "506"},
		},
	}
	agg := &stubReplayableAggregator{}
	dispatcher := &klineDispatcher{
		agg:            agg,
		warmupper:      warmupper,
		last1mOpenTime: map[string]int64{"ETHUSDT": 1778074260000},
	}

	live := &market.Kline{
		Symbol:   "ETHUSDT",
		Interval: "1m",
		OpenTime: 1778074440000,
		IsClosed: true,
		Close:    101.8,
	}
	dispatcher.OnKline(context.Background(), live)

	if warmupper.calls != 1 {
		t.Fatalf("FetchKlinesRange() calls = %d, want 1", warmupper.calls)
	}
	if len(agg.replayKlines) != 2 {
		t.Fatalf("ReplayKline() count = %d, want 2", len(agg.replayKlines))
	}
	if agg.replayKlines[0].OpenTime != 1778074320000 || agg.replayKlines[1].OpenTime != 1778074380000 {
		t.Fatalf("ReplayKline() openTimes = [%d %d], want [1778074320000 1778074380000]",
			agg.replayKlines[0].OpenTime, agg.replayKlines[1].OpenTime)
	}
	if len(agg.onKlines) != 1 || agg.onKlines[0] != live {
		t.Fatal("live 1m kline was not forwarded after gap replay")
	}
	if got := dispatcher.latest1mOpenTime("ETHUSDT"); got != live.OpenTime {
		t.Fatalf("latest1mOpenTime = %d, want %d", got, live.OpenTime)
	}
}

// TestKlineDispatcherOnKlineSkipsBackfillWhenContinuous 验证连续 1m K 线不会触发额外 REST 补档。
func TestKlineDispatcherOnKlineSkipsBackfillWhenContinuous(t *testing.T) {
	warmupper := &stubHistoryWarmupper{}
	agg := &stubReplayableAggregator{}
	dispatcher := &klineDispatcher{
		agg:            agg,
		warmupper:      warmupper,
		last1mOpenTime: map[string]int64{"ETHUSDT": 1778074260000},
	}

	live := &market.Kline{
		Symbol:   "ETHUSDT",
		Interval: "1m",
		OpenTime: 1778074320000,
		IsClosed: true,
		Close:    100.5,
	}
	dispatcher.OnKline(context.Background(), live)

	if warmupper.calls != 0 {
		t.Fatalf("FetchKlinesRange() calls = %d, want 0", warmupper.calls)
	}
	if len(agg.replayKlines) != 0 {
		t.Fatalf("ReplayKline() count = %d, want 0", len(agg.replayKlines))
	}
	if len(agg.onKlines) != 1 || agg.onKlines[0] != live {
		t.Fatal("live 1m kline was not forwarded to aggregator")
	}
}

// TestResolveStrategySignalTopic 验证策略信号 topic 优先取显式配置，未配置时回退到 signal。
func TestResolveStrategySignalTopic(t *testing.T) {
	t.Run("configured signal topic", func(t *testing.T) {
		var cfg config.Config
		cfg.Kafka.Topics.Signal = "strategy-signal"
		cfg.Kafka.Topics.Kline = "kline"

		if got := resolveStrategySignalTopic(cfg); got != "strategy-signal" {
			t.Fatalf("resolveStrategySignalTopic() = %q, want strategy-signal", got)
		}
	})

	t.Run("fallback default signal topic", func(t *testing.T) {
		var cfg config.Config
		cfg.Kafka.Topics.Kline = "kline"

		if got := resolveStrategySignalTopic(cfg); got != "signal" {
			t.Fatalf("resolveStrategySignalTopic() = %q, want signal", got)
		}
	})
}
