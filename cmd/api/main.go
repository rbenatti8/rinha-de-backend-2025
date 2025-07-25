package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/anthdm/hollywood/actor"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/actors"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/env"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/server"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})

	logger := slog.New(handler)
	slog.SetDefault(logger)

	paymentProcessorDefaultURL := env.GetEnvAsString("PAYMENT_PROCESSOR_URL_DEFAULT", "http://localhost:8001")
	paymentProcessorFallbackURL := env.GetEnvAsString("PAYMENT_PROCESSOR_URL_FALLBACK", "http://localhost:8002")
	redisURL := env.GetEnvAsString("REDIS_ADDRESS", "localhost:6379")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisURL,
		DB:   0,
	})

	r := rdb.Ping(context.Background())
	if r.Err() != nil {
		log.Fatalf("Could not connect to Redis: %v", r.Err())
	}

	engine, _ := actor.NewEngine(actor.NewEngineConfig())

	retryTime := env.GetEnvAsInt("RETRY_TIME", 10)
	maxBackoffDelay := env.GetEnvAsInt("MAX_BACKOFF_DELAY", 500)
	heapSize := env.GetEnvAsInt("HEAP_SIZE", 1024)

	readTimeout := env.GetEnvAsInt("READ_TIMEOUT", 10)
	writeTimeout := env.GetEnvAsInt("WRITE_TIMEOUT", 500)

	actorPollSize := env.GetEnvAsInt("ACTOR_POOL_SIZE", 20)

	fastHTTPClient := &fasthttp.Client{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxConnsPerHost:               200,
		MaxIdleConnDuration:           90 * time.Second,
		ReadTimeout:                   time.Duration(readTimeout) * time.Second,
		WriteTimeout:                  time.Duration(writeTimeout) * time.Millisecond,
		MaxConnWaitTimeout:            200 * time.Millisecond,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,

		Dial: fasthttp.Dial,
	}

	dbActor := engine.Spawn(actors.NewDBActor(rdb), "db-actor")
	retryActor := engine.Spawn(actors.NewRetryActor(retryTime, maxBackoffDelay, heapSize), "retry-actor", actor.WithInboxSize(heapSize))

	processorProps := actors.NewPaymentProcessorActor(fastHTTPClient, paymentProcessorDefaultURL, paymentProcessorFallbackURL, dbActor, retryActor)

	processorActorPool := actors.NewPool(engine, processorProps, actorPollSize)

	usePreFork := env.GetEnvAsBool("USE_PREFORK", false)

	s := server.New(engine, processorActorPool, dbActor, usePreFork)
	s.Start(5000)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, os.Interrupt)

	slog.Info(fmt.Sprintf("signal %v received", <-quit), slog.Attr{})
}
