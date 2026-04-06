package paper

import (
	"fmt"
	"sync"
	"time"

	"exchange-system/internal/strategy3/model"
)

type PaperOrder struct {
	ID        string
	Symbol    string
	Action    model.Action
	Side      model.Side
	Quantity  float64
	Price     float64
	Status    string
	CreatedAt time.Time
}

type PaperPosition struct {
	Symbol        string
	Side          model.Side
	Quantity      float64
	EntryPrice    float64
	MarkPrice     float64
	Margin        float64
	UnrealizedPnL float64
	Leverage      float64
	OpenedAt      time.Time
	UpdatedAt     time.Time
}

type PaperAccount struct {
	WalletBalance    float64
	AvailableBalance float64
	Equity           float64
	RealizedPnL      float64
	UnrealizedPnL    float64
	Leverage         float64
	UpdatedAt        time.Time
	Position         *PaperPosition
}

type Exchange struct {
	mu      sync.Mutex
	account PaperAccount
	orders  []PaperOrder
	nextID  int64
}

func New(initialBalance, leverage float64) *Exchange {
	if leverage <= 0 {
		leverage = model.DefaultParams().Leverage
	}
	account := PaperAccount{
		WalletBalance:    initialBalance,
		AvailableBalance: initialBalance,
		Equity:           initialBalance,
		Leverage:         leverage,
	}
	return &Exchange{account: account}
}

func (p *Exchange) Snapshot() PaperAccount {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clonePaperAccount(p.account)
}

func (p *Exchange) Orders() []PaperOrder {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]PaperOrder, len(p.orders))
	copy(result, p.orders)
	return result
}

func (p *Exchange) SyncMarkPrice(symbol string, price float64, now time.Time) PaperAccount {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.syncMarkPriceLocked(symbol, price, now)
	return clonePaperAccount(p.account)
}

func (p *Exchange) ApplyDecision(symbol string, decision model.Decision, now time.Time) (PaperOrder, PaperAccount, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	executionPrice := decision.ExecutionPrice
	if executionPrice <= 0 {
		executionPrice = decision.EntryPrice
	}
	p.syncMarkPriceLocked(symbol, executionPrice, now)
	var order PaperOrder
	switch decision.Action {
	case model.ActionEnter:
		order = PaperOrder{
			ID:        p.nextOrderIDLocked(),
			Symbol:    symbol,
			Action:    decision.Action,
			Side:      decision.Side,
			Quantity:  decision.Quantity,
			Price:     executionPrice,
			Status:    "filled",
			CreatedAt: now,
		}
		if err := p.openPositionLocked(symbol, decision, now); err != nil {
			return PaperOrder{}, PaperAccount{}, err
		}
	case model.ActionPartialExit:
		order = PaperOrder{
			ID:        p.nextOrderIDLocked(),
			Symbol:    symbol,
			Action:    decision.Action,
			Side:      decision.Side,
			Quantity:  decision.Quantity,
			Price:     executionPrice,
			Status:    "filled",
			CreatedAt: now,
		}
		if err := p.reducePositionLocked(symbol, decision.Quantity, decision.RealizedPnL, now); err != nil {
			return PaperOrder{}, PaperAccount{}, err
		}
	case model.ActionExit:
		order = PaperOrder{
			ID:        p.nextOrderIDLocked(),
			Symbol:    symbol,
			Action:    decision.Action,
			Side:      decision.Side,
			Quantity:  decision.Quantity,
			Price:     executionPrice,
			Status:    "filled",
			CreatedAt: now,
		}
		if err := p.closePositionLocked(symbol, decision.RealizedPnL, now); err != nil {
			return PaperOrder{}, PaperAccount{}, err
		}
	default:
		return PaperOrder{}, clonePaperAccount(p.account), nil
	}
	p.orders = append(p.orders, order)
	p.account.UpdatedAt = now.UTC()
	return order, clonePaperAccount(p.account), nil
}

func (p *Exchange) openPositionLocked(symbol string, decision model.Decision, now time.Time) error {
	if p.account.Position != nil && p.account.Position.Quantity > 0 {
		return fmt.Errorf("paper position already exists for %s", symbol)
	}
	if decision.Quantity <= 0 || decision.EntryPrice <= 0 {
		return fmt.Errorf("invalid entry decision")
	}
	margin := decision.Quantity * decision.EntryPrice / maxFloat(p.account.Leverage, 1)
	if margin > p.account.AvailableBalance {
		return fmt.Errorf("insufficient paper margin: need %.2f have %.2f", margin, p.account.AvailableBalance)
	}
	position := &PaperPosition{
		Symbol:     symbol,
		Side:       decision.Side,
		Quantity:   decision.Quantity,
		EntryPrice: decision.EntryPrice,
		MarkPrice:  decision.EntryPrice,
		Margin:     margin,
		Leverage:   p.account.Leverage,
		OpenedAt:   now.UTC(),
		UpdatedAt:  now.UTC(),
	}
	p.account.AvailableBalance -= margin
	p.account.Position = position
	p.syncMarkPriceLocked(symbol, decision.EntryPrice, now)
	return nil
}

func (p *Exchange) reducePositionLocked(symbol string, quantity, realized float64, now time.Time) error {
	position := p.account.Position
	if position == nil || position.Symbol != symbol {
		return fmt.Errorf("paper position not found for %s", symbol)
	}
	if quantity <= 0 || quantity > position.Quantity {
		return fmt.Errorf("invalid reduce quantity %.6f", quantity)
	}
	releaseRatio := quantity / position.Quantity
	releaseMargin := position.Margin * releaseRatio
	p.account.WalletBalance += realized
	p.account.RealizedPnL += realized
	p.account.AvailableBalance += releaseMargin
	position.Quantity -= quantity
	position.Margin -= releaseMargin
	position.UpdatedAt = now.UTC()
	if position.Quantity <= 0 {
		p.account.Position = nil
	} else {
		p.account.Position = position
	}
	p.syncMarkPriceLocked(symbol, position.MarkPrice, now)
	return nil
}

func (p *Exchange) closePositionLocked(symbol string, realized float64, now time.Time) error {
	position := p.account.Position
	if position == nil || position.Symbol != symbol {
		return fmt.Errorf("paper position not found for %s", symbol)
	}
	p.account.WalletBalance += realized
	p.account.RealizedPnL += realized
	p.account.AvailableBalance += position.Margin
	p.account.Position = nil
	p.syncMarkPriceLocked(symbol, position.MarkPrice, now)
	return nil
}

func (p *Exchange) syncMarkPriceLocked(symbol string, price float64, now time.Time) {
	if price <= 0 {
		price = 0
	}
	p.account.UnrealizedPnL = 0
	if p.account.Position != nil && p.account.Position.Symbol == symbol && price > 0 {
		position := p.account.Position
		position.MarkPrice = price
		switch position.Side {
		case model.SideLong:
			position.UnrealizedPnL = (price - position.EntryPrice) * position.Quantity
		case model.SideShort:
			position.UnrealizedPnL = (position.EntryPrice - price) * position.Quantity
		default:
			position.UnrealizedPnL = 0
		}
		position.UpdatedAt = now.UTC()
		p.account.UnrealizedPnL = position.UnrealizedPnL
		p.account.AvailableBalance = p.account.WalletBalance - position.Margin
		if p.account.AvailableBalance < 0 {
			p.account.AvailableBalance = 0
		}
	}
	if p.account.Position == nil {
		p.account.AvailableBalance = p.account.WalletBalance
	}
	p.account.Equity = p.account.WalletBalance + p.account.UnrealizedPnL
	p.account.UpdatedAt = now.UTC()
}

func (p *Exchange) nextOrderIDLocked() string {
	p.nextID++
	return fmt.Sprintf("paper-%d", p.nextID)
}

func clonePaperAccount(account PaperAccount) PaperAccount {
	cloned := account
	if account.Position != nil {
		position := *account.Position
		cloned.Position = &position
	}
	return cloned
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
