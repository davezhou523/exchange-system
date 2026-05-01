package svc

import (
	"context"
	"testing"

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
