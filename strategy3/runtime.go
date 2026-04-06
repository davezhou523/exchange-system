package strategy3

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type CandleStore struct {
	mu       sync.RWMutex
	series   map[string][]Candle
	maxItems map[string]int
}

func NewCandleStore() *CandleStore {
	return &CandleStore{
		series:   make(map[string][]Candle),
		maxItems: make(map[string]int),
	}
}

func (s *CandleStore) Bootstrap(interval string, candles []Candle, maxItems int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := append([]Candle(nil), candles...)
	if maxItems > 0 && len(cloned) > maxItems {
		cloned = cloned[len(cloned)-maxItems:]
	}
	s.series[interval] = cloned
	s.maxItems[interval] = maxItems
}

func (s *CandleStore) Update(interval string, candle Candle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := append([]Candle(nil), s.series[interval]...)
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

func (s *CandleStore) Snapshot() (Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h4 := append([]Candle(nil), s.series["4h"]...)
	h1 := append([]Candle(nil), s.series["1h"]...)
	m15 := append([]Candle(nil), s.series["15m"]...)
	if len(h4) == 0 || len(h1) == 0 || len(m15) == 0 {
		return Snapshot{}, false
	}
	return Snapshot{
		H4:        h4,
		H1:        h1,
		M15:       m15,
		Timestamp: m15[len(m15)-1].CloseTime.UTC(),
	}, true
}

type RuntimeService struct {
	mu           sync.RWMutex
	symbol       string
	engine       *Engine
	rest         *FuturesRESTClient
	stream       *FuturesWebSocketClient
	store        *CandleStore
	paper        *PaperExchange
	state        State
	lastDecision Decision
}

func NewRuntimeService(symbol string, params Params, initialBalance float64) *RuntimeService {
	if initialBalance <= 0 {
		initialBalance = 10000
	}
	return &RuntimeService{
		symbol: strings.ToUpper(symbol),
		engine: NewEngine(params),
		rest:   NewBinanceFuturesRESTClient(),
		stream: NewBinanceFuturesWebSocketClient(),
		store:  NewCandleStore(),
		paper:  NewPaperExchange(initialBalance, params.Leverage),
	}
}

func (s *RuntimeService) SetRESTClient(client *FuturesRESTClient) {
	if client == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rest = client
}

func (s *RuntimeService) SetStreamClient(client *FuturesWebSocketClient) {
	if client == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stream = client
}

func (s *RuntimeService) Bootstrap(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bootstrapLocked(ctx)
}

func (s *RuntimeService) EvaluateOnce(ctx context.Context) (Decision, PaperAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.bootstrapLocked(ctx); err != nil {
		return Decision{}, PaperAccount{}, err
	}
	snapshot, ok := s.store.Snapshot()
	if !ok {
		return Decision{}, PaperAccount{}, fmt.Errorf("snapshot not ready")
	}
	snapshot.Symbol = s.symbol
	account := s.paper.Snapshot()
	decision, err := s.engine.Evaluate(snapshot, s.state, Account{
		Equity:        account.Equity,
		AvailableCash: account.AvailableBalance,
	})
	if err != nil {
		return Decision{}, PaperAccount{}, err
	}
	s.state = decision.UpdatedState
	account = s.paper.SyncMarkPrice(s.symbol, snapshot.M15[len(snapshot.M15)-1].Close, snapshot.Timestamp)
	if decision.Action != ActionHold {
		_, account, err = s.paper.ApplyDecision(s.symbol, decision, snapshot.Timestamp)
		if err != nil {
			return Decision{}, PaperAccount{}, err
		}
	}
	s.lastDecision = decision
	return decision, account, nil
}

func (s *RuntimeService) Run(ctx context.Context, handler func(Decision, PaperAccount)) error {
	if err := s.Bootstrap(ctx); err != nil {
		return err
	}

	intervals := []string{"4h", "1h", "15m"}
	return s.stream.SubscribeKlines(ctx, s.symbol, intervals, func(event StreamEvent) error {
		account := s.paper.SyncMarkPrice(s.symbol, event.Candle.Close, event.Candle.CloseTime)
		if handler != nil && !event.Candle.Closed {
			handler(Decision{
				Action: ActionHold,
				Side:   SideNone,
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
		decision, err := s.engine.Evaluate(snapshot, s.state, Account{
			Equity:        paperAccount.Equity,
			AvailableCash: paperAccount.AvailableBalance,
		})
		if err != nil {
			return err
		}
		s.state = decision.UpdatedState
		paperAccount = s.paper.SyncMarkPrice(s.symbol, event.Candle.Close, event.Candle.CloseTime)
		if decision.Action != ActionHold {
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

func (s *RuntimeService) Account() PaperAccount {
	return s.paper.Snapshot()
}

func (s *RuntimeService) Orders() []PaperOrder {
	return s.paper.Orders()
}

func (s *RuntimeService) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *RuntimeService) LastDecision() Decision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastDecision
}

func (s *RuntimeService) bootstrapLocked(ctx context.Context) error {
	snapshot, ok := s.store.Snapshot()
	if ok && len(snapshot.H4) >= s.engine.params.H4EmaSlow && len(snapshot.H1) >= s.engine.params.H1EmaSlow+5 {
		return nil
	}

	h4, err := s.rest.FetchCandles(ctx, s.symbol, "4h", maxInt(100, s.engine.params.H4EmaSlow+5))
	if err != nil {
		return err
	}
	h1, err := s.rest.FetchCandles(ctx, s.symbol, "1h", maxInt(100, s.engine.params.H1EmaSlow+10))
	if err != nil {
		return err
	}
	m15, err := s.rest.FetchCandles(ctx, s.symbol, "15m", maxInt(100, s.engine.params.M15BreakoutLookback+20))
	if err != nil {
		return err
	}

	s.store.Bootstrap("4h", h4, maxInt(100, s.engine.params.H4EmaSlow+5))
	s.store.Bootstrap("1h", h1, maxInt(100, s.engine.params.H1EmaSlow+10))
	s.store.Bootstrap("15m", m15, maxInt(100, s.engine.params.M15BreakoutLookback+20))
	if len(m15) > 0 {
		s.paper.SyncMarkPrice(s.symbol, m15[len(m15)-1].Close, m15[len(m15)-1].CloseTime.UTC())
	}
	return nil
}
