package main

import (
	"fmt"
	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/remote"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/env"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/proto"
	"github.com/shopspring/decimal"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Payment struct {
	CID         string
	Amount      decimal.Decimal
	RequestedAt time.Time
	ProcessedBy string
}
type server struct {
	db []Payment
}

func newServer() actor.Receiver {

	return &server{
		db: make([]Payment, 0, 35000),
	}
}

func (f *server) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case *actor.PID:
		slog.Info("server got pid", "pid", m)
	case *proto.Ping:
		ctx.Respond(&proto.Pong{})
	case *proto.PurgePayments:
		f.db = f.db[:0]
	case *proto.PushPayment:
		d := decimal.NewFromFloat(m.Payment.Amount)
		t, _ := time.Parse(time.RFC3339Nano, m.Payment.RequestedAt)

		slog.Info("Got payment to store: ", slog.String("cid", m.Payment.Cid), slog.String("time taken", time.Since(t).String()))

		f.db = append(f.db, Payment{
			CID:         m.Payment.Cid,
			Amount:      d,
			RequestedAt: t,
			ProcessedBy: m.ProcessedBy,
		})
	case *proto.SummarizePayments:
		slog.Info("summarize payments", slog.String("from", m.From), slog.String("to", m.To))
		defaultTotalAmount := decimal.Zero
		defaultTotalPayments := int64(0)

		fallbackTotalAmount := decimal.Zero
		fallbackTotalPayments := int64(0)

		tFrom, _ := time.Parse(time.RFC3339Nano, m.From)
		tTo, _ := time.Parse(time.RFC3339Nano, m.To)

		for _, p := range f.db {
			if m.From != "" && p.RequestedAt.Before(tFrom) {
				continue
			}

			if m.To != "" && p.RequestedAt.After(tTo) {
				continue
			}

			if p.ProcessedBy == "default" {
				defaultTotalAmount = defaultTotalAmount.Add(p.Amount)
				defaultTotalPayments++
			}

			if p.ProcessedBy == "fallback" {
				fallbackTotalAmount = fallbackTotalAmount.Add(p.Amount)
				fallbackTotalPayments++
			}
		}

		ctx.Respond(&proto.SummarizedPayments{
			Default: &proto.SummarizedProcessor{
				TotalRequests: defaultTotalPayments,
				TotalAmount:   defaultTotalAmount.String(),
			},
			Fallback: &proto.SummarizedProcessor{
				TotalRequests: fallbackTotalPayments,
				TotalAmount:   fallbackTotalAmount.String(),
			},
		})
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
	localURL := env.GetEnvAsString("LOCAL_HOST", "localhost:4000")
	c := remote.NewConfig()
	c.WithBufferSize(524_288)
	r := remote.New(localURL, c)
	e, err := actor.NewEngine(actor.NewEngineConfig().WithRemote(r))
	if err != nil {
		panic(err)
	}

	e.Spawn(newServer, "remote", actor.WithID("database"))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, os.Interrupt)

	slog.Info(fmt.Sprintf("signal %v received", <-quit), slog.Attr{})
}
