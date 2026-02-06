// test/load_test.go
package test

import (
	"exchange-system/rpc/matching/exchange/rpc/matching/matching"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestLoad(t *testing.T) {
	// 创建 RPC 客户端
	conn := zrpc.MustNewClient(zrpc.RpcClientConf{
		Etcd: zrpc.EtcdConf{
			Hosts: []string{"127.0.0.1:2379"},
			Key:   "matching.rpc",
		},
	})

	client := matching.NewMatching(conn)

	var success int64
	var failed int64
	var wg sync.WaitGroup

	// 并发测试
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()

			req := &matching.OrderRequest{
				Order: &exchange.Order{
					OrderId:    fmt.Sprintf("test_%d_%d", userID, time.Now().UnixNano()),
					UserId:     fmt.Sprintf("user_%d", userID),
					Symbol:     "BTC/USDT",
					Side:       exchange.OrderSide_BUY,
					Type:       exchange.OrderType_LIMIT,
					Price:      fmt.Sprintf("%.2f", 50000+rand.Float64()*1000),
					Amount:     fmt.Sprintf("%.4f", 0.1+rand.Float64()*0.9),
					CreateTime: time.Now().Unix(),
				},
			}

			_, err := client.SubmitOrder(context.Background(), req)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				logx.Errorf("Order failed: %v", err)
			} else {
				atomic.AddInt64(&success, 1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Load test completed: %d success, %d failed", success, failed)
}
