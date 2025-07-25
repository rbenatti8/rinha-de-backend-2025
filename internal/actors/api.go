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

type PaymentProcessorAPIActor struct {
	client               *fasthttp.Client
	defaultProcessorURL  string
	fallbackProcessorURL string
	bestPaymentProcessor string
	dbActorPID           *actor.PID
	engine               *actor.Engine
}

func (a *PaymentProcessorAPIActor) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Started:
		a.engine = c.Engine()
		a.bestPaymentProcessor = defaultPaymentProcessor
	case messages.PaymentProcessorChanged:
		a.bestPaymentProcessor = msg.Processor
	case messages.ProcessPayment:
		if !a.hasAvailableProcessor() {
			a.scheduleRetry(c.PID(), msg.Payment)
			return
		}

		if a.bestPaymentProcessor == fallbackPaymentProcessor {
			a.callProcessor(c, fallbackPaymentProcessor, msg)
			return
		}

		a.callProcessor(c, defaultPaymentProcessor, msg)
	}
}

func (a *PaymentProcessorAPIActor) hasAvailableProcessor() bool {
	if a.bestPaymentProcessor != nonePaymentProcessor {
		return true
	}

	return false
}

func (a *PaymentProcessorAPIActor) callProcessor(c *actor.Context, processor string, msg messages.ProcessPayment) {
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
		a.scheduleRetry(c.PID(), payment)
		return
	}

	a.callDB(payment, processor)
}

func (a *PaymentProcessorAPIActor) scheduleRetry(sender *actor.PID, p messages.Payment) {
	// TODO: implement exponential backoff strategy
	// TODO: get retry actor pid
	a.engine.Send(sender, messages.ScheduleRetry{
		Sender:  sender,
		Payment: p,
	})
}

func (a *PaymentProcessorAPIActor) callDB(p messages.Payment, processor string) {
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

func NewPaymentProcessorAPIActor(
	client *fasthttp.Client,
	defaultURL, fallbackURL string,
	dbActorPID *actor.PID,
) actor.Producer {
	return func() actor.Receiver {
		return &PaymentProcessorAPIActor{
			client:               client,
			defaultProcessorURL:  defaultURL,
			fallbackProcessorURL: fallbackURL,
			dbActorPID:           dbActorPID,
		}
	}
}
