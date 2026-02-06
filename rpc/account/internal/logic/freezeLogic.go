package logic

import (
	"context"
	"errors"
	"math/big"
	"time"

	"exchange-system/rpc/account/account"
	"exchange-system/rpc/account/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type FreezeLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFreezeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FreezeLogic {
	return &FreezeLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *FreezeLogic) Freeze(in *account.FreezeRequest) (*account.FreezeResponse, error) {
	// 使用 Redis 分布式锁保证并发安全
	lockKey := "balance_lock:" + in.UserId + ":" + in.Currency
	mutex := l.svcCtx.Redis.NewMutex(lockKey, redis.WithExpiry(3*time.Second))

	if err := mutex.Lock(); err != nil {
		return nil, err
	}
	defer mutex.Unlock()

	// 获取当前余额
	balanceKey := "balance:" + in.UserId + ":" + in.Currency
	balanceStr, err := l.svcCtx.Redis.Get(balanceKey)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	available := big.NewFloat(0)
	if balanceStr != "" {
		if _, ok := available.SetString(balanceStr); !ok {
			return nil, errors.New("invalid balance format")
		}
	}

	// 检查余额是否足够
	freezeAmount := new(big.Float)
	if _, ok := freezeAmount.SetString(in.Amount); !ok {
		return nil, errors.New("invalid amount")
	}

	if available.Cmp(freezeAmount) < 0 {
		return &account.FreezeResponse{
			Success: false,
			Error:   "insufficient balance",
		}, nil
	}

	// 更新可用余额
	newAvailable := new(big.Float).Sub(available, freezeAmount)
	err = l.svcCtx.Redis.Set(balanceKey, newAvailable.String())
	if err != nil {
		return nil, err
	}

	// 记录冻结记录
	freezeKey := "freeze:" + in.OrderId
	freezeRecord := map[string]string{
		"user_id":  in.UserId,
		"currency": in.Currency,
		"amount":   in.Amount,
		"time":     time.Now().Format(time.RFC3339),
	}

	// 存储到 Redis Hash
	err = l.svcCtx.Redis.Hmset(freezeKey, freezeRecord)
	if err != nil {
		// 回滚余额
		l.svcCtx.Redis.Set(balanceKey, available.String())
		return nil, err
	}

	// 设置过期时间（24小时）
	l.svcCtx.Redis.Expire(freezeKey, 24*3600)

	// 记录余额变更流水
	l.recordBalanceChange(in.UserId, in.Currency, "-"+in.Amount,
		"freeze", in.OrderId, "订单冻结")

	return &account.FreezeResponse{Success: true}, nil
}

func (l *FreezeLogic) recordBalanceChange(userID, currency, amount, changeType, refID, remark string) {
	// 将余额变更记录到数据库
	_, err := l.svcCtx.BalanceChangeModel.Insert(l.ctx, &model.BalanceChange{
		UserID:    userID,
		Currency:  currency,
		Amount:    amount,
		Type:      changeType,
		RefID:     refID,
		Remark:    remark,
		CreatedAt: time.Now(),
	})

	if err != nil {
		l.Errorf("Failed to record balance change: %v", err)
	}
}
