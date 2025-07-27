package server

import (
	"fmt"
	"github.com/anthdm/hollywood/actor"
	"github.com/buger/jsonparser"
	goJson "github.com/goccy/go-json"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/actors"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/prefork"
	"log/slog"
	"time"
)

var (
	processPaymentPath = "/payments"
	purgePaymentsPath  = "/purge-payments"
	summaryPath        = "/payments-summary"
)

type Handler struct {
	processorActorPool *actors.Pool
	dbActor            *actor.PID
	engine             *actor.Engine
}

func (h *Handler) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Method()) {
	case fasthttp.MethodPost:
		h.handlePost(ctx)
	case fasthttp.MethodGet:
		h.handleGet(ctx)
	default:
		ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
	}
}

func (h *Handler) handlePost(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())

	if path == processPaymentPath {
		h.handleProcessPayment(ctx)
		return
	}

	if path == purgePaymentsPath {
		h.handlePurgePayments(ctx)
		return
	}

	ctx.Error("Not Found", fasthttp.StatusNotFound)
	return
}

func (h *Handler) handleProcessPayment(ctx *fasthttp.RequestCtx) {
	body := ctx.PostBody()

	cid, _ := jsonparser.GetString(body, "correlationId")
	amount, _ := jsonparser.GetFloat(body, "amount")

	pid := h.processorActorPool.GetActor(cid)

	h.engine.Send(pid, messages.ProcessPayment{
		Payment: messages.Payment{
			CID:    cid,
			Amount: amount,
		},
	})

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func (h *Handler) handlePurgePayments(ctx *fasthttp.RequestCtx) {
	h.engine.Send(h.dbActor, messages.PurgePayments{})

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func (h *Handler) handleGet(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())

	if path == summaryPath {
		h.handleGetSummary(ctx)
		return
	}

	ctx.Error("Not Found", fasthttp.StatusNotFound)
	return
}

func (h *Handler) handleGetSummary(ctx *fasthttp.RequestCtx) {
	msg := buildMessage(ctx)

	slog.Info("Summary payments", slog.String("from", msg.From.String()), slog.String("to", msg.To.String()))

	resp := h.engine.Request(h.dbActor, msg, 5*time.Second)

	res, err := resp.Result()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	summaryResp, ok := res.(messages.SummarizedPayments)
	if !ok {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	bodyResp, _ := goJson.Marshal(messages.SummarizedPayments{
		Default: messages.SummarizedProcessor{
			TotalRequests: summaryResp.Default.TotalRequests,
			TotalAmount:   summaryResp.Default.TotalAmount,
		},
		Fallback: messages.SummarizedProcessor{
			TotalRequests: summaryResp.Fallback.TotalRequests,
			TotalAmount:   summaryResp.Fallback.TotalAmount,
		},
	})

	//bodyResp, _ := goJson.Marshal(messages.SummarizedPayments{})

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBody(bodyResp)
}

func buildMessage(c *fasthttp.RequestCtx) messages.SummarizePayments {
	from := c.QueryArgs().Peek("from")
	to := c.QueryArgs().Peek("to")

	summaryReq := messages.SummarizePayments{}

	pFrom, err := time.Parse(time.RFC3339Nano, string(from))
	if err == nil {
		summaryReq.From = &pFrom
	}

	pTo, err := time.Parse(time.RFC3339Nano, string(to))
	if err == nil {
		summaryReq.To = &pTo
	}

	return summaryReq
}

type Server struct {
	preforkServer *prefork.Prefork
	server        *fasthttp.Server
	usePrefork    bool
}

func (s *Server) Start(port int) {
	go func() {

		if s.usePrefork {
			if err := s.server.ListenAndServe(fmt.Sprintf(":%d", port)); err != nil {
				slog.Error("Error starting pre fork server", slog.String("error", err.Error()))
			}
		} else {
			if err := s.server.ListenAndServe(fmt.Sprintf(":%d", port)); err != nil {
				slog.Error("Error starting server", slog.String("error", err.Error()))
			}
		}

	}()
}

func New(
	engine *actor.Engine,
	processorPool *actors.Pool,
	dbActor *actor.PID,
	usePreFork bool,
) *Server {
	h := &Handler{
		processorActorPool: processorPool,
		dbActor:            dbActor,
		engine:             engine,
	}

	s := &fasthttp.Server{
		Handler:               h.handler,
		ReadTimeout:           0,
		WriteTimeout:          0,
		MaxConnsPerIP:         0,
		MaxRequestsPerConn:    0,
		NoDefaultServerHeader: true,
		NoDefaultDate:         true,
		DisableKeepalive:      false,
		ReduceMemoryUsage:     false,
	}

	return &Server{
		server:        s,
		preforkServer: prefork.New(s),
		usePrefork:    usePreFork,
	}
}
