package healthy

import (
	"context"
	"errors"
	"github.com/buger/jsonparser"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
	"log/slog"
	"math"
	"net/http"
	"sync/atomic"
	"time"
)

var statusChannel = "status-update"

type ServiceHealth struct {
	Processor       string
	Failing         bool
	MinResponseTime int64
}

type Checker struct {
	httpClient              *fasthttp.Client
	dpr                     string
	fpr                     string
	client                  *redis.Client
	currentProcessor        atomic.Value
	isPublisher             bool
	maxLatency              int64
	isDefaultFailing        bool
	defaultFailingStartTime time.Time
}

func New(client *redis.Client, httpClient *fasthttp.Client, dpr, fpr string, maxLatency int64, isPublisher bool) *Checker {
	return &Checker{
		client:      client,
		dpr:         dpr + "/payments/service-health",
		fpr:         fpr + "/payments/service-health",
		maxLatency:  maxLatency,
		isPublisher: isPublisher,
		httpClient:  httpClient,
	}
}

func (c *Checker) Start() {
	if c.isPublisher {
		go c.startCheckingServiceHealth()
	} else {
		go c.startListeningServiceHealth()
	}
}

func (c *Checker) startListeningServiceHealth() {
	sub := c.client.Subscribe(context.Background(), statusChannel)

	for msg := range sub.Channel() {
		slog.Info("API received: " + msg.Payload)
		c.currentProcessor.Store(msg.Payload)
	}
}

func (c *Checker) startCheckingServiceHealth() {
	c.checkServiceHealth()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkServiceHealth()
		}
	}
}

func (c *Checker) checkServiceHealth() {
	respChan := make(chan ServiceHealth, 2)

	go func() {
		hcapi, err := c.doDefaultProcessorHC()
		if err != nil {
			respChan <- ServiceHealth{Processor: "default", Failing: true}
			return
		}
		respChan <- hcapi
	}()

	go func() {
		hcapi, err := c.doFallbackProcessorHC()
		if err != nil {
			slog.Error("Fallback processor health check failed", slog.String("error", err.Error()))
			respChan <- ServiceHealth{Processor: "fallback", Failing: true}
			return
		}
		respChan <- hcapi
	}()

	hcs := make(map[string]ServiceHealth, 2)
	for i := 0; i < 2; i++ {
		hcapi := <-respChan
		hcs[hcapi.Processor] = hcapi
	}

	processor := c.chooseProcessor(hcs)
	slog.Info("Checking service health", slog.String("processor", processor))
	c.currentProcessor.Store(processor)

	c.broadcastProcessor(processor)
}

// TODO: improve this
func (c *Checker) chooseProcessor(hcs map[string]ServiceHealth) string {
	def := hcs["default"]
	fbk := hcs["fallback"]

	now := time.Now()
	defaultIsSlow := def.MinResponseTime > c.maxLatency
	defaultUnavailable := def.Failing || defaultIsSlow

	switch {
	case defaultUnavailable && !c.isDefaultFailing:
		c.isDefaultFailing = true
		c.defaultFailingStartTime = now
	case !defaultUnavailable && c.isDefaultFailing:
		c.isDefaultFailing = false
	}

	if !defaultUnavailable {
		return "default"
	}

	if c.isDefaultFailing && time.Since(c.defaultFailingStartTime) < 20*time.Second {
		return "waiting"
	}

	if !fbk.Failing && fbk.MinResponseTime < c.maxLatency {
		return "fallback"
	}

	if def.Failing && fbk.Failing {
		return "none"
	}

	if defaultIsSlow && fbk.MinResponseTime > c.maxLatency {
		return "none"
	}

	if !fbk.Failing && def.MinResponseTime > fbk.MinResponseTime+(fbk.MinResponseTime/2) {
		return "fallback"
	}

	return "default"
}

func computeEffectiveCost(tax float64, latency int64, failing bool) float64 {
	if failing {
		return math.Inf(1)
	}
	return tax + float64(latency)*0.001
}

func (c *Checker) SetValue(s string) {
	c.currentProcessor.Store(s)
}

func (c *Checker) GetPaymentProcessor() (string, error) {
	v := c.currentProcessor.Load()
	if v == nil || v.(string) == "none" || v.(string) == "waiting" {
		return "", errors.New("no payment processor available")
	}

	return v.(string), nil
}

func (c *Checker) HasHealthyProcessors() bool {
	v := c.currentProcessor.Load()
	if v == nil || v.(string) == "none" {
		return false
	}

	return true
}

func (c *Checker) doDefaultProcessorHC() (ServiceHealth, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(c.dpr)
	req.Header.SetMethod(fasthttp.MethodGet)

	err := c.httpClient.Do(req, resp)
	return c.handleHCResponse(err, resp, "default")
}

func (c *Checker) doFallbackProcessorHC() (ServiceHealth, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(c.fpr)
	req.Header.SetMethod(fasthttp.MethodGet)

	err := c.httpClient.Do(req, resp)
	return c.handleHCResponse(err, resp, "fallback")
}

func (c *Checker) handleHCResponse(err error, resp *fasthttp.Response, processor string) (ServiceHealth, error) {
	if err != nil {
		return ServiceHealth{}, err
	}

	if resp.StatusCode() != http.StatusOK {
		slog.Error("failed to do hc", slog.String("error", string(resp.Body())), slog.Int("status", resp.StatusCode()))
		return ServiceHealth{}, errors.New("failed to check health of processor: " + processor)
	}

	resBytes := resp.Body()

	failing, _ := jsonparser.GetBoolean(resBytes, "failing")
	minResponseTime, _ := jsonparser.GetInt(resBytes, "minResponseTime")

	return ServiceHealth{
		Processor:       processor,
		Failing:         failing,
		MinResponseTime: minResponseTime,
	}, nil
}

func (c *Checker) broadcastProcessor(processor string) {
	r := c.client.Publish(context.Background(), statusChannel, processor)
	if r.Err() != nil {
		slog.Error("Error broadcasting status", slog.String("error", r.Err().Error()))
		return
	}
}
