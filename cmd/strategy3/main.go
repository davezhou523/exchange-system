package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"exchange-system/strategy3"
	"github.com/gorilla/websocket"
	"golang.org/x/net/proxy"
)

func main() {
	symbol := flag.String("symbol", "ETHUSDT", "交易对")
	balance := flag.Float64("balance", 10000, "模拟账户初始余额")
	once := flag.Bool("once", true, "只执行一次评估")
	restURL := flag.String("rest-url", "https://fapi.binance.com", "永续合约 REST 行情地址")
	wsURL := flag.String("ws-url", "wss://fstream.binance.com", "永续合约 WebSocket 地址")
	testnet := flag.Bool("testnet", false, "使用Binance测试网")
	proxyURL := flag.String("proxy", "", "SOCKS5代理地址，格式：socks://127.0.0.1:1080 或 socks5://127.0.0.1:1080")
	apiKey := flag.String("api-key", "Go22trfIcD8q8TUJvJXyt4nMKerXUWUhZzEiArc8W3DfzUnbi8CKrW6nENOD7K1Z", "Binance API Key")
	apiSecret := flag.String("api-secret", "p1LUWlD2PbsPnCq6KlczSnZTIeOtpf5Iu5nQ6bhVhy0iU1CAhmBinONZ7tqrF4NY", "Binance API Secret")
	mode := flag.String("mode", "strategy", "运行模式: strategy(策略), account(账户信息), orders(订单历史)")
	flag.Parse()

	if *testnet {
		*restURL = "https://demo-fapi.binance.com"
		*wsURL = "wss://fstream.binancefuture.com"
		log.Printf("使用Binance测试网，请确保API密钥对应测试网环境")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 创建HTTP客户端和WebSocket拨号器（支持代理）
	var httpClient *http.Client
	var wsDialer *websocket.Dialer

	if *proxyURL != "" {
		// 规范化代理URL：如果缺少://，则添加
		proxyStr := *proxyURL
		if strings.Contains(proxyStr, ":") && !strings.Contains(proxyStr, "://") {
			// 格式为"socks:127.0.0.1:1080"，转换为"socks://127.0.0.1:1080"
			parts := strings.SplitN(proxyStr, ":", 2)
			if len(parts) == 2 {
				proxyStr = parts[0] + "://" + parts[1]
				log.Printf("自动修正代理地址格式: %s -> %s", *proxyURL, proxyStr)
			}
		}

		parsedURL, err := url.Parse(proxyStr)
		if err != nil {
			log.Fatalf("无效的代理地址 %s: %v", proxyStr, err)
		}

		if parsedURL.Scheme == "socks" || parsedURL.Scheme == "socks5" {
			// proxy.FromURL期望的是socks5，所以将socks映射为socks5
			proxyURLForDialer := parsedURL
			if parsedURL.Scheme == "socks" {
				// 创建副本以避免修改原始URL
				proxyURLForDialer = &url.URL{
					Scheme: "socks5",
					Host:   parsedURL.Host,
					User:   parsedURL.User,
				}
				log.Printf("映射代理协议: socks -> socks5")
			}

			socksDialer, err := proxy.FromURL(proxyURLForDialer, proxy.Direct)
			if err != nil {
				log.Fatalf("创建SOCKS5代理拨号器失败: %v", err)
			}

			// 为HTTP客户端创建自定义传输层
			transport := &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return socksDialer.Dial(network, addr)
				},
			}

			httpClient = &http.Client{
				Transport: transport,
				Timeout:   30 * time.Second,
			}

			// 为WebSocket创建自定义拨号器
			wsDialer = &websocket.Dialer{
				NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return socksDialer.Dial(network, addr)
				},
				HandshakeTimeout: 10 * time.Second,
			}

			log.Printf("已启用SOCKS5代理: %s", proxyStr)
		} else {
			log.Fatalf("不支持的代理协议: %s，仅支持socks或socks5", parsedURL.Scheme)
		}
	} else {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
		}
		wsDialer = websocket.DefaultDialer
	}

	service := strategy3.NewRuntimeService(strings.ToUpper(*symbol), strategy3.DefaultParams(), *balance)
	restClient := strategy3.NewBinanceFuturesRESTClient()
	restClient.BaseURL = *restURL
	restClient.APIKey = *apiKey
	restClient.SecretKey = *apiSecret
	restClient.HTTPClient = httpClient // 设置自定义HTTP客户端
	streamClient := strategy3.NewBinanceFuturesWebSocketClient()
	streamClient.BaseURL = *wsURL
	if wsDialer != nil {
		streamClient.Dialer = wsDialer // 设置自定义WebSocket拨号器
	}
	service.SetRESTClient(restClient)
	service.SetStreamClient(streamClient)

	switch *mode {
	case "account":
		accountInfo, err := restClient.GetAccountInfo(ctx)
		if err != nil {
			log.Fatal(err)
		}
		jsonBytes, err := json.MarshalIndent(accountInfo, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(jsonBytes))
		return
	case "orders":
		orders, err := restClient.GetOrders(ctx, *symbol, 10)
		if err != nil {
			log.Fatal(err)
		}
		jsonBytes, err := json.MarshalIndent(orders, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(jsonBytes))
		return
	case "strategy":
		// 继续执行策略逻辑
	default:
		log.Fatalf("未知模式: %s", *mode)
	}

	if *once {
		decision, account, err := service.EvaluateOnce(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(formatDecision(decision, account))
		return
	}

	err := service.Run(ctx, func(decision strategy3.Decision, account strategy3.PaperAccount) {
		fmt.Println(formatDecision(decision, account))
	})
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		log.Fatal(err)
	}
}

func formatDecision(decision strategy3.Decision, account strategy3.PaperAccount) string {
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "action=%s side=%s reason=%s equity=%.2f", decision.Action, decision.Side, decision.Reason, account.Equity)
	if decision.EntryPrice > 0 {
		fmt.Fprintf(builder, " entry=%.4f", decision.EntryPrice)
	}
	if decision.Quantity > 0 {
		fmt.Fprintf(builder, " qty=%.6f", decision.Quantity)
	}
	if decision.StopLoss > 0 {
		fmt.Fprintf(builder, " stop=%.4f", decision.StopLoss)
	}
	if len(decision.TakeProfits) > 0 {
		fmt.Fprintf(builder, " tp=%v", decision.TakeProfits)
	}
	if decision.RealizedPnL != 0 {
		fmt.Fprintf(builder, " pnl=%.2f", decision.RealizedPnL)
	}
	if account.Position != nil {
		fmt.Fprintf(builder, " mark=%.4f unrealized=%.2f", account.Position.MarkPrice, account.UnrealizedPnL)
	}
	return builder.String()
}
