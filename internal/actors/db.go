package actors

import (
	"context"
	"github.com/anthdm/hollywood/actor"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"log/slog"
	"strconv"
	"strings"
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

type DBActor struct {
	client   *redis.Client
	mu       sync.Mutex
	batch    []messages.PushPayment
	maxBatch int
}

func (a *DBActor) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case messages.PushPayment:
		a.pushPayment(msg)
	case messages.SummarizePayments:
		a.summarize(c, msg)
	case messages.PurgePayments:
		a.purgePayments(c)
	}
}

func (a *DBActor) purgePayments(c *actor.Context) {
	err := a.client.Del(context.Background(), keyPaymentsAll).Err()
	if err != nil {
		slog.Error("Error purging payments from Redis", slog.String("error", err.Error()))
	}

	c.Respond(struct{}{})
}

func (a *DBActor) pushPayment(msg messages.PushPayment) {
	bufPtr := bufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	buf = append(buf, msg.Payment.CID...)
	buf = append(buf, '|')
	buf = strconv.AppendFloat(buf, msg.Payment.Amount, 'f', -1, 64)
	buf = append(buf, '|')
	buf = append(buf, msg.Payment.RequestedAt...)
	buf = append(buf, '|')
	buf = append(buf, msg.ProcessedBy...)

	timeToArrive := time.Since(msg.ProcessedAt)
	if timeToArrive > 100*time.Millisecond {
		slog.Warn("Payment took too long to arrive", slog.String("ActorID", msg.Payment.CID), slog.Duration("time_to_arrive", timeToArrive))
	}

	t := time.Now()
	err := a.client.RPush(ctx, keyPaymentsAll, string(buf)).Err()
	if err != nil {
		slog.Error("Error pushing payments to Redis", slog.String("error", err.Error()))
	}

	if time.Since(t) > 100*time.Millisecond {
		slog.Warn("Slow push to Redis", slog.String("CID", msg.Payment.CID), slog.Duration("duration", time.Since(t)))
	}

	bufPool.Put(bufPtr)
}

func (a *DBActor) pushBatchPayment(msgs []messages.PushPayment) {
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
	err := a.client.RPush(context.Background(), keyPaymentsAll, values...).Err()
	if err != nil {
		slog.Error("Error pushing payments to Redis", slog.String("error", err.Error()))
	}

	if dur := time.Since(start); dur > 100*time.Millisecond {
		slog.Warn("Slow batch push to Redis", slog.Int("count", len(msgs)), slog.Duration("duration", dur))
	}
}

func (a *DBActor) summarize(c *actor.Context, msg messages.SummarizePayments) {
	summary := messages.SummarizedPayments{}

	lines, err := a.client.LRange(context.Background(), keyPaymentsAll, 0, -1).Result()
	if err != nil {
		slog.Error("Error pulling payments from Redis", slog.String("error", err.Error()))
		c.Respond(struct{}{})
	}

	cidMap := make(map[string]struct{})

	for _, line := range lines {
		fields := strings.Split(line, "|")

		if _, exists := cidMap[fields[0]]; exists {
			slog.Warn("Duplicate CID found, skipping", slog.String("CID", fields[0]))
			continue
		}

		cidMap[fields[0]] = struct{}{}

		requestedAt := fields[2]
		if msg.From != nil {
			timestamp, _ := time.Parse(time.RFC3339Nano, requestedAt)
			if timestamp.UTC().Before(*msg.From) {
				continue
			}
		}

		if msg.To != nil {
			timestamp, _ := time.Parse(time.RFC3339Nano, requestedAt)
			if timestamp.UTC().After(*msg.To) {
				continue
			}
		}

		processedBy := fields[3]

		amount := fields[1]
		value, _ := decimal.NewFromString(amount)

		if processedBy == "default" {
			summary.Default.TotalAmount = summary.Default.TotalAmount.Add(value)
			summary.Default.TotalRequests++
			continue
		}

		summary.Fallback.TotalAmount = summary.Fallback.TotalAmount.Add(value)
		summary.Fallback.TotalRequests++
	}

	c.Respond(summary)
}

func NewDBActor(client *redis.Client) actor.Producer {
	return func() actor.Receiver {
		return &DBActor{
			client: client,
		}
	}
}
