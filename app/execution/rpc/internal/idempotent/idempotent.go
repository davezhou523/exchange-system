package idempotent

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 幂等 & 防重复下单
//
// 基于信号唯一标识（strategy_id + symbol + timestamp）做去重：
//   - 同一信号不会重复下单
//   - 支持 TTL 自动清理过期记录
//   - 线程安全
//
// 生产环境可替换为 Redis 实现，当前使用内存存储
// ---------------------------------------------------------------------------

// SignalDeduplicator 信号去重器
type SignalDeduplicator struct {
	mu      sync.RWMutex
	records map[string]*dedupRecord // key -> 去重记录
	ttl     time.Duration           // 记录过期时间
}

// dedupRecord 去重记录
type dedupRecord struct {
	key       string    // 去重键
	orderID   string    // 已生成的订单ID
	timestamp time.Time // 记录时间
}

// NewSignalDeduplicator 创建信号去重器
// ttl: 记录保留时间，过期自动清理（默认 10 分钟）
func NewSignalDeduplicator(ttl time.Duration) *SignalDeduplicator {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	d := &SignalDeduplicator{
		records: make(map[string]*dedupRecord),
		ttl:     ttl,
	}
	// 启动后台清理协程
	go d.cleanupLoop()
	return d
}

// SignalKey 生成信号去重键
// 格式：strategy_id:symbol:timestamp（同一策略、同一交易对、同一时刻的信号视为重复）
func SignalKey(strategyID, symbol string, timestamp int64) string {
	return fmt.Sprintf("%s:%s:%d", strategyID, symbol, timestamp)
}

// Check 检查信号是否已处理
// 返回：(是否重复, 已存在的订单ID)
// 如果不重复，自动标记为已处理
func (d *SignalDeduplicator) Check(key string) (duplicate bool, existingOrderID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if rec, ok := d.records[key]; ok {
		// 检查是否已过期
		if time.Since(rec.timestamp) > d.ttl {
			// 已过期，删除旧记录，允许重新处理
			delete(d.records, key)
			return false, ""
		}
		// 未过期，属于重复信号
		log.Printf("[幂等] 检测到重复信号 | key=%s 已有订单=%s", key, rec.orderID)
		return true, rec.orderID
	}

	// 新信号，标记为已处理
	d.records[key] = &dedupRecord{
		key:       key,
		timestamp: time.Now(),
	}
	return false, ""
}

// MarkOrderID 记录信号对应的订单ID（下单成功后调用）
func (d *SignalDeduplicator) MarkOrderID(key, orderID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if rec, ok := d.records[key]; ok {
		rec.orderID = orderID
	}
}

// Remove 移除去重记录（订单失败需要重试时调用）
func (d *SignalDeduplicator) Remove(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.records, key)
}

// cleanupLoop 后台清理过期记录
func (d *SignalDeduplicator) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		expired := 0
		for k, rec := range d.records {
			if now.Sub(rec.timestamp) > d.ttl {
				delete(d.records, k)
				expired++
			}
		}
		d.mu.Unlock()

		if expired > 0 {
			log.Printf("[幂等] 清理过期记录 %d 条，剩余 %d 条", expired, len(d.records))
		}
	}
}

// Stats 获取去重器统计信息
func (d *SignalDeduplicator) Stats() (total, withOrder int) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	total = len(d.records)
	for _, rec := range d.records {
		if rec.orderID != "" {
			withOrder++
		}
	}
	return
}
