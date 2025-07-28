package actors

import (
	"github.com/anthdm/hollywood/actor"
	goJson "github.com/goccy/go-json"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/database"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/healthy"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

var (
	nonePaymentProcessor     = "none"
	defaultPaymentProcessor  = "default"
	fallbackPaymentProcessor = "fallback"
)

type PaymentProcessorActor struct {
	client               *fasthttp.Client
	hcChecker            *healthy.Checker
	defaultProcessorURL  string
	fallbackProcessorURL string
	bestPaymentProcessor string
	dbActor              *actor.PID
	redis                *redis.Client
	retryActorPID        *actor.PID
	integrityActorPool   *Pool
	engine               *actor.Engine
	db                   *database.DB
}

func (a *PaymentProcessorActor) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Started:
		a.engine = c.Engine()
		a.bestPaymentProcessor = defaultPaymentProcessor
	case messages.ProcessPayment:
		paymentProcessor, err := a.hcChecker.GetPaymentProcessor()
		if err != nil {
			a.scheduleRetry(c.PID(), msg)
			return
		}

		if paymentProcessor == fallbackPaymentProcessor {
			a.callProcessor(c, fallbackPaymentProcessor, msg)
			return
		}

		a.callProcessor(c, defaultPaymentProcessor, msg)
	}
}

func (a *PaymentProcessorActor) hasAvailableProcessor() bool {
	if a.bestPaymentProcessor != nonePaymentProcessor {
		return true
	}

	return false
}

func (a *PaymentProcessorActor) callProcessor(c *actor.Context, processor string, msg messages.ProcessPayment) {
	url := a.defaultProcessorURL

	if processor == fallbackPaymentProcessor {
		url = a.fallbackProcessorURL
	}

	msg.Payment.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)

	buf, _ := goJson.Marshal(msg.Payment)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	req.SetBody(buf)

	err := a.client.Do(req, resp)
	if isTimeoutErr(err) {
		slog.Warn("Sending to integrity actor: ", slog.String("cid", msg.Payment.CID), slog.String("RequestedAt", msg.Payment.RequestedAt))
		a.sendToIntegrityActor(msg, processor)
		return
	}

	if isErr(msg.Payment, resp, err) {
		a.scheduleRetry(c.PID(), msg)
		return
	}

	a.pushPayment(messages.PushPayment{
		Payment:     msg.Payment,
		ProcessedBy: processor,
		ProcessedAt: time.Now().UTC(),
	})
}

func (a *PaymentProcessorActor) scheduleRetry(sender *actor.PID, msg messages.ProcessPayment) {
	a.engine.Send(a.retryActorPID, messages.ScheduleRetry{
		Sender:  sender,
		Payment: msg.Payment,
		Tries:   msg.Tries,
	})
}

func (a *PaymentProcessorActor) sendToIntegrityActor(msg messages.ProcessPayment, processor string) {
	integrityActor := a.integrityActorPool.GetActor(msg.Payment.CID)
	a.engine.Send(integrityActor, messages.CheckIntegrity{
		Payment:   msg.Payment,
		Processor: processor,
	})
}

func (a *PaymentProcessorActor) pushPayment(msg messages.PushPayment) {
	t := time.Now()
	bufPtr := bufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	buf = append(buf, msg.Payment.CID...)
	buf = append(buf, '|')
	buf = strconv.AppendFloat(buf, msg.Payment.Amount, 'f', -1, 64)
	buf = append(buf, '|')
	buf = append(buf, msg.Payment.RequestedAt...)
	buf = append(buf, '|')
	buf = append(buf, msg.ProcessedBy...)

	timeToBuildBuf := time.Since(t)
	if timeToBuildBuf > 30*time.Millisecond {
		slog.Warn("Slow buffer creation for payment", slog.String("CID", msg.Payment.CID), slog.Duration("duration", timeToBuildBuf))
	}
	timeToArrive := time.Since(msg.ProcessedAt)
	if timeToArrive > 100*time.Millisecond {
		slog.Warn("Payment took too long to arrive", slog.String("ActorID", msg.Payment.CID), slog.Duration("time_to_arrive", timeToArrive))
	}

	t = time.Now()
	err := a.redis.RPush(ctx, keyPaymentsAll, string(buf)).Err()
	if err != nil {
		slog.Error("Error pushing payments to Redis", slog.String("error", err.Error()))
	}

	if time.Since(t) > 100*time.Millisecond {
		slog.Warn("Slow push to Redis", slog.String("CID", msg.Payment.CID), slog.Duration("duration", time.Since(t)))
	}

	bufPool.Put(bufPtr)
}

func isTimeoutErr(err error) bool {
	if err != nil && err.Error() == "timeout" {
		return true
	}

	return false
}

func isErr(payment messages.Payment, resp *fasthttp.Response, err error) bool {
	if err != nil {
		slog.Error(err.Error())
		return true
	}

	if resp.StatusCode() == http.StatusUnprocessableEntity {
		slog.Warn("Duplicate payment detected", slog.String("cid", payment.CID), slog.String("time", time.Now().UTC().Format(time.RFC3339Nano)))
		return false
	}

	if resp.StatusCode() != http.StatusOK {
		slog.Error("Invalid status code: ", slog.Int("code", resp.StatusCode()))
		return true
	}

	return false
}

func NewPaymentProcessorActor(
	client *fasthttp.Client,
	defaultURL, fallbackURL string,
	redis *redis.Client,
	retryActorPID *actor.PID,
	integrityActorPool *Pool,
	hcChecker *healthy.Checker,
) actor.Producer {
	return func() actor.Receiver {
		return &PaymentProcessorActor{
			client:               client,
			defaultProcessorURL:  defaultURL + "/payments",
			fallbackProcessorURL: fallbackURL + "/payments",
			redis:                redis,
			retryActorPID:        retryActorPID,
			integrityActorPool:   integrityActorPool,
			hcChecker:            hcChecker,
		}
	}
}
