package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"exchange-system/internal/strategy3/engine"
	binancex "exchange-system/internal/strategy3/exchange/binance"
	paperx "exchange-system/internal/strategy3/exchange/paper"
	"exchange-system/internal/strategy3/model"
)

type CandleStore struct {
	mu       sync.RWMutex
	series   map[string][]model.Candle
	maxItems map[string]int
}

type Service struct {
	mu           sync.RWMutex
	symbol       string
	engine       *engine.Engine
	rest         *binancex.FuturesRESTClient
	stream       *binancex.FuturesWebSocketClient
	store        *CandleStore
	paper        *paperx.Exchange
	state        model.State
	lastDecision model.Decision
}

func NewCandleStore() *CandleStore {
	return &CandleStore{
		series:   make(map[string][]model.Candle),
		maxItems: make(map[string]int),
	}
}

func NewService(symbol string, params model.Params, initialBalance float64) *Service {
	if initialBalance <= 0 {
		initialBalance = 10000
	}
	return &Service{
		symbol: strings.ToUpper(symbol),
		engine: engine.New(params),
		rest:   binancex.NewFuturesRESTClient(),
		stream: binancex.NewFuturesWebSocketClient(),
		store:  NewCandleStore(),
		paper:  paperx.New(initialBalance, params.Leverage),
	}
}

func (s *Service) Bootstrap(interval string, candles []model.Candle, maxItems int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store.Bootstrap(interval, candles, maxItems)
}

func (s *CandleStore) Bootstrap(interval string, candles []model.Candle, maxItems int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := append([]model.Candle(nil), candles...)
	if maxItems > 0 && len(cloned) > maxItems {
		cloned = cloned[len(cloned)-maxItems:]
	}
	s.series[interval] = cloned
	s.maxItems[interval] = maxItems
}

func (s *CandleStore) Update(interval string, candle model.Candle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := append([]model.Candle(nil), s.series[interval]...)
	if len(current) == 0 {
		current = append(current, candle)
	} else {
		last := current[len(current)-1]
		switch {
		case last.OpenTime.Equal(candle.OpenTime):
			current[len(current)-1] = candle
		case candle.OpenTime.After(last.OpenTime):
			current = append(current, candle)
		default:
			current[len(current)-1] = candle
		}
	}
	maxItems := s.maxItems[interval]
	if maxItems > 0 && len(current) > maxItems {
		current = current[len(current)-maxItems:]
	}
	s.series[interval] = current
}

func (s *CandleStore) Snapshot() (model.Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h4 := append([]model.Candle(nil), s.series["4h"]...)
	h1 := append([]model.Candle(nil), s.series["1h"]...)
	m15 := append([]model.Candle(nil), s.series["15m"]...)
	if len(h4) == 0 || len(h1) == 0 || len(m15) == 0 {
		return model.Snapshot{}, false
	}
	return model.Snapshot{
		H4:        h4,
		H1:        h1,
		M15:       m15,
		Timestamp: m15[len(m15)-1].CloseTime.UTC(),
	}, true
}

func (s *Service) SetRESTClient(client *binancex.FuturesRESTClient) {
	if client == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rest = client
}

func (s *Service) SetStreamClient(client *binancex.FuturesWebSocketClient) {
	if client == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stream = client
}

func (s *Service) BootstrapHistory(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bootstrapLocked(ctx)
}

func (s *Service) EvaluateOnce(ctx context.Context) (model.Decision, paperx.PaperAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bootstrapLocked(ctx); err != nil {
		return model.Decision{}, paperx.PaperAccount{}, err
	}
	snapshot, ok := s.store.Snapshot()
	if !ok {
		return model.Decision{}, paperx.PaperAccount{}, fmt.Errorf("snapshot not ready")
	}
	snapshot.Symbol = s.symbol
	account := s.paper.Snapshot()
	decision, err := s.engine.Evaluate(snapshot, s.state, model.Account{
		Equity:        account.Equity,
		AvailableCash: account.AvailableBalance,
	})
	if err != nil {
		return model.Decision{}, paperx.PaperAccount{}, err
	}
	s.state = decision.UpdatedState
	account = s.paper.SyncMarkPrice(s.symbol, snapshot.M15[len(snapshot.M15)-1].Close, snapshot.Timestamp)
	if decision.Action != model.ActionHold {
		_, account, err = s.paper.ApplyDecision(s.symbol, decision, snapshot.Timestamp)
		if err != nil {
			return model.Decision{}, paperx.PaperAccount{}, err
		}
	}
	s.lastDecision = decision
	return decision, account, nil
}

func (s *Service) Run(ctx context.Context, handler func(model.Decision, paperx.PaperAccount)) error {
	if err := s.BootstrapHistory(ctx); err != nil {
		return err
	}
	intervals := []string{"4h", "1h", "15m"}
	return s.stream.SubscribeKlines(ctx, s.symbol, intervals, func(event binancex.StreamEvent) error {
		account := s.paper.SyncMarkPrice(s.symbol, event.Candle.Close, event.Candle.CloseTime)
		if handler != nil && !event.Candle.Closed {
			handler(model.Decision{
				Action: model.ActionHold,
				Side:   model.SideNone,
				Reason: fmt.Sprintf("%s 未收盘，已同步模拟账户", event.Interval),
			}, account)
		}
		if !event.Candle.Closed {
			return nil
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.store.Update(event.Interval, event.Candle)
		if event.Interval != "15m" {
			return nil
		}
		snapshot, ok := s.store.Snapshot()
		if !ok {
			return nil
		}
		snapshot.Symbol = s.symbol
		paperAccount := s.paper.Snapshot()
		decision, err := s.engine.Evaluate(snapshot, s.state, model.Account{
			Equity:        paperAccount.Equity,
			AvailableCash: paperAccount.AvailableBalance,
		})
		if err != nil {
			return err
		}
		s.state = decision.UpdatedState
		paperAccount = s.paper.SyncMarkPrice(s.symbol, event.Candle.Close, event.Candle.CloseTime)
		if decision.Action != model.ActionHold {
			_, paperAccount, err = s.paper.ApplyDecision(s.symbol, decision, event.Candle.CloseTime)
			if err != nil {
				return err
			}
		}
		s.lastDecision = decision
		if handler != nil {
			handler(decision, paperAccount)
		}
		return nil
	})
}

func (s *Service) Account() paperx.PaperAccount {
	return s.paper.Snapshot()
}

func (s *Service) Orders() []paperx.PaperOrder {
	return s.paper.Orders()
}

func (s *Service) State() model.State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *Service) LastDecision() model.Decision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastDecision
}

func (s *Service) bootstrapLocked(ctx context.Context) error {
	snapshot, ok := s.store.Snapshot()
	if ok && len(snapshot.H4) >= model.DefaultParams().H4EmaSlow && len(snapshot.H1) >= model.DefaultParams().H1EmaSlow+5 {
		return nil
	}
	h4, err := s.rest.FetchCandles(ctx, s.symbol, "4h", 100)
	if err != nil {
		return err
	}
	h1, err := s.rest.FetchCandles(ctx, s.symbol, "1h", 100)
	if err != nil {
		return err
	}
	m15, err := s.rest.FetchCandles(ctx, s.symbol, "15m", 100)
	if err != nil {
		return err
	}
	s.store.Bootstrap("4h", h4, 100)
	s.store.Bootstrap("1h", h1, 100)
	s.store.Bootstrap("15m", m15, 100)
	if len(m15) > 0 {
		s.paper.SyncMarkPrice(s.symbol, m15[len(m15)-1].Close, m15[len(m15)-1].CloseTime.UTC())
	}
	return nil
}
