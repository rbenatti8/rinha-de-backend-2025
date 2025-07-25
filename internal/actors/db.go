package actors

import (
	"context"
	"github.com/anthdm/hollywood/actor"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"strconv"
	"sync"
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

type DBActor struct {
	client *redis.Client
}

func (a *DBActor) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case messages.PushPayment:
		bufPtr := bufPool.Get().(*[]byte)
		buf := (*bufPtr)[:0]

		buf = append(buf, msg.Payment.CID...)
		buf = append(buf, '|')
		buf = strconv.AppendFloat(buf, msg.Payment.Amount, 'f', -1, 64)
		buf = append(buf, '|')
		buf = append(buf, msg.Payment.RequestedAt...)
		buf = append(buf, '|')
		buf = append(buf, msg.ProcessedBy...)

		err := a.client.RPush(ctx, keyPaymentsAll, string(buf)).Err()
		if err != nil {
			slog.Error("Error pushing payments to Redis", slog.String("error", err.Error()))
		}

		bufPool.Put(bufPtr)
	}
}

func NewDBActor(client *redis.Client) actor.Producer {
	return func() actor.Receiver {
		return &DBActor{
			client: client,
		}
	}
}
