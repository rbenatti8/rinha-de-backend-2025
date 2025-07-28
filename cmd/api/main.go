package main

import (
	"context"
	"crypto/tls"
	"fmt"
	_ "github.com/KimMachineGun/automemlimit"
	"github.com/anthdm/hollywood/actor"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/actors"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/env"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/healthy"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/server"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/valyala/fasthttp"
	_ "go.uber.org/automaxprocs"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	decimal.MarshalJSONWithoutQuotes = true

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})

	logger := slog.New(handler)
	slog.SetDefault(logger)

	paymentProcessorDefaultURL := env.GetEnvAsString("PAYMENT_PROCESSOR_URL_DEFAULT", "http://localhost:8001")
	paymentProcessorFallbackURL := env.GetEnvAsString("PAYMENT_PROCESSOR_URL_FALLBACK", "http://localhost:8002")
	redisURL := env.GetEnvAsString("REDIS_ADDRESS", "localhost:6379")

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisURL,
		DB:           0,
		MinIdleConns: 20,
		PoolSize:     20,
		PoolTimeout:  60 * time.Second,
	})

	warmupAllRedisConns(rdb, 20)

	engine, _ := actor.NewEngine(actor.NewEngineConfig())

	retryTime := env.GetEnvAsInt("RETRY_TIME", 10)
	maxBackoffDelay := env.GetEnvAsInt("MAX_BACKOFF_DELAY", 500)
	heapSize := env.GetEnvAsInt("HEAP_SIZE", 1024)

	readTimeout := env.GetEnvAsInt("READ_TIMEOUT", 500)
	writeTimeout := env.GetEnvAsInt("WRITE_TIMEOUT", 500)

	paymentProcessorPoolSize := env.GetEnvAsInt("ACTOR_POOL_SIZE", 30)

	processorHTTPClient := &fasthttp.Client{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxConnsPerHost:               paymentProcessorPoolSize,
		MaxIdleConnDuration:           120 * time.Second,
		ReadTimeout:                   time.Duration(readTimeout) * time.Millisecond,
		WriteTimeout:                  time.Duration(writeTimeout) * time.Millisecond,
		MaxConnWaitTimeout:            2 * time.Second,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,

		Dial: fasthttp.Dial,
	}

	isPublisher := env.GetEnvAsBool("IS_PUBLISHER", true)

	t := time.Now()

	warmUpConnections(processorHTTPClient, paymentProcessorDefaultURL+"/payments", paymentProcessorPoolSize)
	warmUpConnections(processorHTTPClient, paymentProcessorFallbackURL+"/payments", paymentProcessorPoolSize/2)

	slog.Info("Warm-up connections completed", slog.Duration("duration", time.Since(t)))

	hcHTTPClient := &fasthttp.Client{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxConnsPerHost:               2,
		MaxIdleConnDuration:           120 * time.Second,
		ReadTimeout:                   10 * time.Second,
		WriteTimeout:                  10 * time.Second,
		MaxConnWaitTimeout:            2 * time.Second,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,

		Dial: fasthttp.Dial,
	}

	hc := healthy.New(rdb, hcHTTPClient, paymentProcessorDefaultURL, paymentProcessorFallbackURL, 500, isPublisher)
	hc.Start()

	dbActor := engine.Spawn(actors.NewDBActor(rdb), "db-actor")
	integrityProps := actors.NewIntegrityActor(processorHTTPClient, paymentProcessorDefaultURL, paymentProcessorFallbackURL, dbActor)
	integrityPool := actors.NewPool(engine, integrityProps, "integrity", 1, 512)

	retryActor := engine.Spawn(actors.NewRetryActor(retryTime, maxBackoffDelay, heapSize, hc), "retry-actor", actor.WithInboxSize(heapSize))
	processorProps := actors.NewPaymentProcessorActor(processorHTTPClient, paymentProcessorDefaultURL, paymentProcessorFallbackURL, rdb, retryActor, integrityPool, hc)

	processorActorPool := actors.NewPool(engine, processorProps, "processor", paymentProcessorPoolSize, 2048)

	usePreFork := env.GetEnvAsBool("USE_PREFORK", false)

	s := server.New(engine, processorActorPool, dbActor, usePreFork)
	s.Start(5000)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, os.Interrupt)

	slog.Info(fmt.Sprintf("signal %v received", <-quit), slog.Attr{})
}

func warmUpConnections(client *fasthttp.Client, endpoint string, count int) {
	const maxWorkers = 100

	tasks := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for range tasks {
				req := fasthttp.AcquireRequest()
				resp := fasthttp.AcquireResponse()
				defer fasthttp.ReleaseRequest(req)
				defer fasthttp.ReleaseResponse(resp)

				req.SetRequestURI(endpoint)
				req.Header.SetMethod("GET")

				err := client.Do(req, resp)
				if err != nil {
					log.Fatal("Warmed up connections failed", slog.Attr{}, slog.String("error", err.Error()))
				}

				if resp.StatusCode() != fasthttp.StatusMethodNotAllowed {
					log.Fatal("Warmed up connections failed", slog.Attr{})
				}

				wg.Done()
			}
		}()
	}

	for i := 0; i < count; i++ {
		wg.Add(1)
		tasks <- struct{}{}
	}

	wg.Wait()
	close(tasks)
}

func warmupAllRedisConns(rdb *redis.Client, totalConns int) {
	var wg sync.WaitGroup
	wg.Add(totalConns)

	for i := 0; i < totalConns; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			r := rdb.Ping(ctx)
			if r.Err() != nil {
				log.Fatal(r.Err())
			}
		}()
	}

	wg.Wait()
}
