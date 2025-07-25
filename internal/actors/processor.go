package actors

import (
	"github.com/anthdm/hollywood/actor"
	goJson "github.com/goccy/go-json"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"github.com/valyala/fasthttp"
	"net/http"
	"time"
)

var (
	nonePaymentProcessor     = "none"
	defaultPaymentProcessor  = "default"
	fallbackPaymentProcessor = "fallback"
)

type PaymentProcessorActor struct {
	client               *fasthttp.Client
	defaultProcessorURL  string
	fallbackProcessorURL string
	bestPaymentProcessor string
	dbActorPID           *actor.PID
	retryActorPID        *actor.PID
	engine               *actor.Engine
}

func (a *PaymentProcessorActor) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Started:
		a.engine = c.Engine()
		a.bestPaymentProcessor = defaultPaymentProcessor
		c.Engine().Subscribe(c.PID())
	case messages.PaymentProcessorChanged:
		a.bestPaymentProcessor = msg.Processor
	case messages.ProcessPayment:
		if !a.hasAvailableProcessor() {
			a.scheduleRetry(c.PID(), msg)
			return
		}

		if a.bestPaymentProcessor == fallbackPaymentProcessor {
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

	payment := msg.Payment
	payment.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)

	buf, _ := goJson.Marshal(payment)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	req.SetBody(buf)

	err := a.client.Do(req, resp)
	if isErr(resp, err) {
		a.scheduleRetry(c.PID(), msg)
		return
	}

	a.callDB(payment, processor)
}

func (a *PaymentProcessorActor) scheduleRetry(sender *actor.PID, msg messages.ProcessPayment) {
	a.engine.Send(a.retryActorPID, messages.ScheduleRetry{
		Sender:  sender,
		Payment: msg.Payment,
		Tries:   msg.Tries,
	})
}

func (a *PaymentProcessorActor) callDB(p messages.Payment, processor string) {
	a.engine.Send(a.dbActorPID, messages.PushPayment{
		Payment:     p,
		ProcessedBy: processor,
	})
}

func isErr(resp *fasthttp.Response, err error) bool {
	if err != nil {
		return true
	}

	if resp.StatusCode() == http.StatusUnprocessableEntity {
		return false
	}

	if resp.StatusCode() != http.StatusOK {
		return true
	}

	return false
}

func NewPaymentProcessorActor(
	client *fasthttp.Client,
	defaultURL, fallbackURL string,
	dbActorPID *actor.PID,
	retryActorPID *actor.PID,
) actor.Producer {
	return func() actor.Receiver {
		return &PaymentProcessorActor{
			client:               client,
			defaultProcessorURL:  defaultURL + "/payments",
			fallbackProcessorURL: fallbackURL + "/payments",
			dbActorPID:           dbActorPID,
			retryActorPID:        retryActorPID,
		}
	}
}
