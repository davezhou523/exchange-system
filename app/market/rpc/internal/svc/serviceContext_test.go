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

// UpdateSnapshotFromKline 记录 dispatcher 透传给 UniversePool 的最新闭合 K 线。
func (s *stubSnapshotUpdater) UpdateSnapshotFromKline(k *market.Kline) {
	s.last = k
}

func TestKlineDispatcherOnKlineUpdatesUniverseBeforeAggregator(t *testing.T) {
	updater := &stubSnapshotUpdater{}
	dispatcher := &klineDispatcher{
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
