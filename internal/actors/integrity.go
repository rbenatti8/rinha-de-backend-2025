package actors

import (
	"fmt"
	"github.com/anthdm/hollywood/actor"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/proto"
	"github.com/valyala/fasthttp"
)

type IntegrityActor struct {
	client               *fasthttp.Client
	defaultProcessorURL  string
	fallbackProcessorURL string
	dbActor              *actor.PID
}

func (a *IntegrityActor) Receive(c *actor.Context) {
	switch m := c.Message().(type) {
	case messages.CheckIntegrity:
		if m.Processor == "fallback" {
			a.checkFallback(c, m)
			return
		}

		a.checkDefault(c, m)
	}
}

func (a *IntegrityActor) checkFallback(c *actor.Context, m messages.CheckIntegrity) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(a.fallbackProcessorURL + fmt.Sprintf("/%s", m.Payment.CID))
	req.Header.SetMethod(fasthttp.MethodGet)

	err := a.client.Do(req, resp)
	if shouldRetry(resp, err) {
		c.Send(c.PID(), m)
		return
	}

	c.Send(a.dbActor, &proto.PushPayment{
		Payment: &proto.Payment{
			Cid:         m.Payment.CID,
			Amount:      m.Payment.Amount,
			RequestedAt: m.Payment.RequestedAt,
		},
		ProcessedBy: m.Processor,
	})
}

func (a *IntegrityActor) checkDefault(c *actor.Context, m messages.CheckIntegrity) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(a.defaultProcessorURL + fmt.Sprintf("/%s", m.Payment.CID))
	req.Header.SetMethod(fasthttp.MethodGet)

	err := a.client.Do(req, resp)
	if shouldRetry(resp, err) {
		c.Send(c.PID(), m)
		return
	}

	c.Send(a.dbActor, &proto.PushPayment{
		Payment: &proto.Payment{
			Cid:         m.Payment.CID,
			Amount:      m.Payment.Amount,
			RequestedAt: m.Payment.RequestedAt,
		},
		ProcessedBy: m.Processor,
	})
}

func shouldRetry(resp *fasthttp.Response, err error) bool {
	if err != nil {
		return true
	}

	return resp.StatusCode() != fasthttp.StatusOK
}

func NewIntegrityActor(
	client *fasthttp.Client,
	defaultURL, fallbackURL string,
	dbActor *actor.PID,
) actor.Producer {
	return func() actor.Receiver {
		return &IntegrityActor{
			client:               client,
			defaultProcessorURL:  defaultURL + "/payments",
			fallbackProcessorURL: fallbackURL + "/payments",
			dbActor:              dbActor,
		}
	}
}
