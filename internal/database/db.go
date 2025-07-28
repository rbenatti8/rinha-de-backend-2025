package database

import (
	"context"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

var (
	bufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, 64)
			return &b
		},
	}

	ctx = context.Background()

	keyPaymentsAll = "payments:all"
)

type DB struct {
	redisClient *redis.Client
}

func New(redis *redis.Client) *DB {
	return &DB{
		redisClient: redis,
	}
}

func (db *DB) pushBatchPayment(msgs []messages.PushPayment) {
	if len(msgs) == 0 {
		return
	}

	values := make([]interface{}, 0, len(msgs))

	for _, msg := range msgs {
		bufPtr := bufPool.Get().(*[]byte)
		buf := (*bufPtr)[:0]

		buf = append(buf, msg.Payment.CID...)
		buf = append(buf, '|')
		buf = strconv.AppendFloat(buf, msg.Payment.Amount, 'f', -1, 64)
		buf = append(buf, '|')
		buf = append(buf, msg.Payment.RequestedAt...)
		buf = append(buf, '|')
		buf = append(buf, msg.ProcessedBy...)

		values = append(values, string(buf))

		bufPool.Put(bufPtr)
	}

	start := time.Now()
	err := db.redisClient.RPush(context.Background(), keyPaymentsAll, values...).Err()
	if err != nil {
		slog.Error("Error pushing payments to Redis", slog.String("error", err.Error()))
	}

	if dur := time.Since(start); dur > 50*time.Millisecond {
		slog.Warn("Slow batch push to Redis", slog.Int("count", len(msgs)), slog.Duration("duration", dur))
	}
}
