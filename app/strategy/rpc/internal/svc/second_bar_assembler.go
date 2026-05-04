package svc

import (
	"strings"
	"sync"

	strategyengine "exchange-system/app/strategy/rpc/internal/strategy"
	marketpb "exchange-system/common/pb/market"
)

// SecondBarAssemblerConfig 控制秒级装配器是否启用深度兜底等可选能力。
type SecondBarAssemblerConfig struct {
	EnableDepthMidFallback bool
}

// SecondBarAssembler 按 symbol 聚合 trade 流，产出给策略 1s 极端保护使用的秒级简化行情。
type SecondBarAssembler struct {
	symbol string

	mu sync.Mutex

	config SecondBarAssemblerConfig

	currentSec int64
	currentBar *strategyengine.SecondBar

	lastTradePrice float64
	lastDepthMid   float64
	lastDepthTs    int64
}

// NewSecondBarAssembler 创建一个按单交易对聚合秒级成交的装配器。
func NewSecondBarAssembler(symbol string, configs ...SecondBarAssemblerConfig) *SecondBarAssembler {
	cfg := SecondBarAssemblerConfig{}
	if len(configs) > 0 {
		cfg = configs[0]
	}
	return &SecondBarAssembler{
		symbol: strings.ToUpper(strings.TrimSpace(symbol)),
		config: cfg,
	}
}

// UpdateConfig 更新秒级装配器配置，让运行时切换是否启用 depth 兜底不需要重建实例。
func (a *SecondBarAssembler) UpdateConfig(cfg SecondBarAssemblerConfig) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.config = cfg
	a.mu.Unlock()
}

// OnTrade 把一条成交合并进当前秒桶；当检测到跨秒时，返回已经封口的 final SecondBar。
func (a *SecondBarAssembler) OnTrade(trade *marketpb.Trade) []*strategyengine.SecondBar {
	if a == nil || trade == nil {
		return nil
	}
	if a.symbol == "" || strings.ToUpper(strings.TrimSpace(trade.GetSymbol())) != a.symbol {
		return nil
	}
	if trade.GetPrice() <= 0 || trade.GetQuantity() <= 0 || trade.GetTimestamp() <= 0 {
		return nil
	}

	sec := trade.GetTimestamp() / 1000

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.currentBar == nil {
		a.currentSec = sec
		a.currentBar = newSecondBarFromTrade(sec, trade)
		a.lastTradePrice = trade.GetPrice()
		return nil
	}
	if sec < a.currentSec {
		// 秒级极端保护优先实时性，迟到成交不回补已封口秒桶，避免重复触发风控。
		return nil
	}
	if sec == a.currentSec {
		if a.currentBar != nil && a.currentBar.Synthetic {
			a.currentBar = newSecondBarFromTrade(sec, trade)
			a.lastTradePrice = trade.GetPrice()
			return nil
		}
		updateSecondBarWithTrade(a.currentBar, trade)
		a.lastTradePrice = trade.GetPrice()
		return nil
	}

	finalized := cloneSecondBar(a.currentBar)
	finalized.IsFinal = true

	a.currentSec = sec
	a.currentBar = newSecondBarFromTrade(sec, trade)
	a.lastTradePrice = trade.GetPrice()

	return []*strategyengine.SecondBar{finalized}
}

// OnDepth 记录最新盘口中间价，并在开启兜底模式时为无成交秒生成基于 mid-price 的 synthetic 秒桶。
func (a *SecondBarAssembler) OnDepth(depth *marketpb.Depth) []*strategyengine.SecondBar {
	if a == nil || depth == nil {
		return nil
	}
	if a.symbol == "" || strings.ToUpper(strings.TrimSpace(depth.GetSymbol())) != a.symbol {
		return nil
	}
	if len(depth.GetBids()) == 0 || len(depth.GetAsks()) == 0 {
		return nil
	}
	bestBid := depth.GetBids()[0]
	bestAsk := depth.GetAsks()[0]
	if bestBid == nil || bestAsk == nil || bestBid.GetPrice() <= 0 || bestAsk.GetPrice() <= 0 {
		return nil
	}
	if depth.GetTimestamp() <= 0 {
		return nil
	}

	mid := (bestBid.GetPrice() + bestAsk.GetPrice()) / 2
	sec := depth.GetTimestamp() / 1000

	a.mu.Lock()
	defer a.mu.Unlock()

	a.lastDepthMid = mid
	a.lastDepthTs = depth.GetTimestamp()
	if !a.config.EnableDepthMidFallback {
		return nil
	}
	if a.currentBar == nil {
		a.currentSec = sec
		a.currentBar = newSyntheticSecondBar(sec, mid)
		return nil
	}
	if sec < a.currentSec {
		return nil
	}
	if sec == a.currentSec {
		if a.currentBar.Synthetic {
			updateSecondBarWithPrice(a.currentBar, mid)
		}
		return nil
	}

	finalized := cloneSecondBar(a.currentBar)
	finalized.IsFinal = true
	a.currentSec = sec
	a.currentBar = newSyntheticSecondBar(sec, mid)
	return []*strategyengine.SecondBar{finalized}
}

// newSecondBarFromTrade 用一笔成交初始化新的秒桶，首笔成交同时作为开高低收。
func newSecondBarFromTrade(sec int64, trade *marketpb.Trade) *strategyengine.SecondBar {
	price := trade.GetPrice()
	return &strategyengine.SecondBar{
		OpenTimeMs:  sec * 1000,
		CloseTimeMs: sec*1000 + 999,
		Open:        price,
		High:        price,
		Low:         price,
		Close:       price,
		Volume:      trade.GetQuantity(),
		IsFinal:     false,
		Synthetic:   false,
	}
}

// newSyntheticSecondBar 基于盘口中间价初始化无成交秒桶，供极端保护在空秒时保持时间连续。
func newSyntheticSecondBar(sec int64, mid float64) *strategyengine.SecondBar {
	return &strategyengine.SecondBar{
		OpenTimeMs:  sec * 1000,
		CloseTimeMs: sec*1000 + 999,
		Open:        mid,
		High:        mid,
		Low:         mid,
		Close:       mid,
		Volume:      0,
		IsFinal:     false,
		Synthetic:   true,
	}
}

// updateSecondBarWithTrade 把同一秒内的新成交折叠进已有秒桶，更新高低收和累计成交量。
func updateSecondBarWithTrade(bar *strategyengine.SecondBar, trade *marketpb.Trade) {
	if bar == nil || trade == nil {
		return
	}
	price := trade.GetPrice()
	if price <= 0 {
		return
	}
	if bar.High == 0 || price > bar.High {
		bar.High = price
	}
	if bar.Low == 0 || price < bar.Low {
		bar.Low = price
	}
	bar.Close = price
	bar.Volume += trade.GetQuantity()
}

// updateSecondBarWithPrice 用最新 mid-price 滚动 synthetic 秒桶，避免同秒内盘口更新被完全忽略。
func updateSecondBarWithPrice(bar *strategyengine.SecondBar, price float64) {
	if bar == nil || price <= 0 {
		return
	}
	if bar.High == 0 || price > bar.High {
		bar.High = price
	}
	if bar.Low == 0 || price < bar.Low {
		bar.Low = price
	}
	bar.Close = price
}

// cloneSecondBar 复制一份秒桶快照，避免上层误改装配器内部状态。
func cloneSecondBar(bar *strategyengine.SecondBar) *strategyengine.SecondBar {
	if bar == nil {
		return nil
	}
	cloned := *bar
	return &cloned
}
