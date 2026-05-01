package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/proxy"
)

func main() {
	baseURL := flag.String("url", "wss://fstream.binance.com", "binance websocket base url")
	proxyURL := flag.String("proxy", "socks5://192.168.10.14:1080", "proxy url, e.g. socks5://127.0.0.1:1080")
	streams := flag.String("streams", "btcusdt@kline_1m/ethusdt@kline_1m", "combined streams path")
	readTimeout := flag.Duration("read-timeout", 20*time.Second, "timeout waiting for first message")
	maxMessages := flag.Int("max-messages", 5, "exit after receiving this many messages")
	flag.Parse()
	log.Printf("[wsproxycheck] base_url=%s proxy_url=%s streams=%s read_timeout=%s max_messages=%d", *baseURL, *proxyURL, *streams, *readTimeout, *maxMessages)
	target := fmt.Sprintf("%s/market/stream?streams=%s", strings.TrimRight(*baseURL, "/"), strings.TrimLeft(*streams, "/"))
	dialer := &websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	if err := configureProxy(dialer, *proxyURL); err != nil {
		log.Fatalf("configure proxy failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("[wsproxycheck] connecting target=%s", target)
	conn, resp, err := dialer.DialContext(ctx, target, nil)
	if err != nil {
		if resp != nil {
			log.Fatalf("[wsproxycheck] dial failed status=%s err=%v", resp.Status, err)
		}
		log.Fatalf("[wsproxycheck] dial failed err=%v", err)
	}
	defer conn.Close()

	log.Printf("[wsproxycheck] connected status=%s", resp.Status)
	if err := conn.SetReadDeadline(time.Now().Add(*readTimeout)); err != nil {
		log.Fatalf("[wsproxycheck] set read deadline failed: %v", err)
	}
	conn.SetPongHandler(func(appData string) error {
		log.Printf("[wsproxycheck] pong payload=%q", appData)
		return conn.SetReadDeadline(time.Now().Add(*readTimeout))
	})

	if err := conn.WriteControl(websocket.PingMessage, []byte("wsproxycheck"), time.Now().Add(3*time.Second)); err != nil {
		log.Fatalf("[wsproxycheck] initial ping failed: %v", err)
	}
	log.Printf("[wsproxycheck] initial ping sent read_timeout=%s", *readTimeout)

	start := time.Now()
	for i := 1; i <= *maxMessages; i++ {
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("[wsproxycheck] read failed after=%s count=%d err=%v", time.Since(start).Round(time.Millisecond), i-1, err)
		}
		preview := string(payload)
		if len(preview) > 240 {
			preview = preview[:240] + "..."
		}
		log.Printf("[wsproxycheck] message #%d type=%d bytes=%d after=%s payload=%s", i, msgType, len(payload), time.Since(start).Round(time.Millisecond), preview)
	}

	log.Printf("[wsproxycheck] success received=%d", *maxMessages)
}

func configureProxy(dialer *websocket.Dialer, raw string) error {
	if raw == "" || dialer == nil {
		return nil
	}
	proxyCfg, err := url.Parse(raw)
	if err != nil {
		return err
	}
	switch strings.ToLower(proxyCfg.Scheme) {
	case "socks5", "socks5h":
		base := &net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}
		socksDialer, err := proxy.FromURL(proxyCfg, base)
		if err != nil {
			return err
		}
		dialer.Proxy = nil
		dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if contextDialer, ok := socksDialer.(proxy.ContextDialer); ok {
				return contextDialer.DialContext(ctx, network, addr)
			}
			return socksDialer.Dial(network, addr)
		}
		log.Printf("[wsproxycheck] using SOCKS5 proxy=%s", proxyCfg.String())
	default:
		dialer.Proxy = func(_ *http.Request) (*url.URL, error) {
			return proxyCfg, nil
		}
		log.Printf("[wsproxycheck] using HTTP proxy=%s", proxyCfg.String())
	}
	return nil
}
