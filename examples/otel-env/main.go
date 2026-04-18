package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"resilience/resilience"
)

func main() {
	cfg := loadConfig()
	shutdown, err := setupOTel(cfg)
	if err != nil {
		log.Fatalf("otel setup failed: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()

	cb := resilience.NewCircuitBreaker(cfg.CBName,
		resilience.WithSlidingWindowType(cfg.CBWindowType),
		resilience.WithSlidingWindowSize(cfg.CBWindowSize),
		resilience.WithMinimumNumberOfCalls(cfg.CBMinimumCalls),
		resilience.WithFailureRateThreshold(cfg.CBFailureRateThreshold),
		resilience.WithSlowCallRateThreshold(cfg.CBSlowCallRateThreshold),
		resilience.WithSlowCallDurationThreshold(cfg.CBSlowCallDurationThreshold),
		resilience.WithPermittedNumberOfCallsInHalfOpenState(cfg.CBHalfOpenPermits),
		resilience.WithWaitDurationInOpenState(cfg.CBWaitOpen),
		resilience.WithAutomaticTransitionFromOpenToHalfOpen(cfg.CBAutoTransition),
		resilience.WithIgnoreErrors(func(err error) bool {
			return strings.Contains(strings.ToLower(err.Error()), "validation")
		}),
	)

	retry := resilience.NewRetry(cfg.RetryName,
		resilience.WithMaxAttempts(cfg.RetryMaxAttempts),
		resilience.WithWaitDuration(cfg.RetryWait),
		resilience.WithExponentialBackoff(cfg.RetryBackoffMultiplier, cfg.RetryMaxInterval),
		resilience.WithRetryOn(func(err error) bool {
			return err != nil && !errors.Is(err, context.Canceled)
		}),
	)

	bulkhead := resilience.NewBulkhead(cfg.BulkheadName,
		resilience.WithMaxConcurrentCalls(cfg.BulkheadMaxConcurrent),
		resilience.WithMaxWaitDuration(cfg.BulkheadMaxWait),
	)

	rateLimiter := resilience.NewRateLimiter(cfg.RateLimiterName,
		resilience.WithLimitForPeriod(cfg.RateLimitForPeriod),
		resilience.WithLimitRefreshPeriod(cfg.RateLimitRefreshPeriod),
		resilience.WithTimeoutDuration(cfg.RateLimitTimeout),
	)

	bridge, err := resilience.NewOTelBridge(resilience.OTelBridgeConfig{
		Meter:         otel.Meter("examples/otel-env"),
		MetricsPrefix: cfg.OTelMetricsPrefix,
	})
	if err != nil {
		log.Fatalf("bridge setup failed: %v", err)
	}
	defer func() {
		_ = bridge.Shutdown(context.Background())
	}()

	bridge.RegisterCircuitBreaker(cb)
	bridge.RegisterRetry(retry)
	bridge.RegisterBulkhead(bulkhead)
	bridge.RegisterRateLimiter(rateLimiter)

	log.Printf("running otel-env example for %s", cfg.RunDuration)
	deadline := time.Now().Add(cfg.RunDuration)
	for time.Now().Before(deadline) {
		_, err := resilience.Decorate(func(ctx context.Context) (interface{}, error) {
			roll := rand.Intn(100)
			switch {
			case roll < 20:
				return nil, errors.New("transient timeout")
			case roll < 30:
				time.Sleep(cfg.CBSlowCallDurationThreshold + 30*time.Millisecond)
				return "slow-ok", nil
			default:
				return "ok", nil
			}
		}).
			WithRateLimiter(rateLimiter).
			WithBulkhead(bulkhead).
			WithCircuitBreaker(cb).
			WithRetry(retry).
			Call(context.Background())

		if err != nil {
			log.Printf("call error: %v", err)
		}
		time.Sleep(cfg.LoopDelay)
	}
	log.Println("example completed")
}

type config struct {
	ServiceName string
	RunDuration time.Duration
	LoopDelay   time.Duration

	CBName                      string
	CBWindowType                resilience.SlidingWindowType
	CBWindowSize                int
	CBMinimumCalls              int
	CBFailureRateThreshold      float64
	CBSlowCallRateThreshold     float64
	CBSlowCallDurationThreshold time.Duration
	CBHalfOpenPermits           int
	CBWaitOpen                  time.Duration
	CBAutoTransition            bool

	RetryName              string
	RetryMaxAttempts       int
	RetryWait              time.Duration
	RetryBackoffMultiplier float64
	RetryMaxInterval       time.Duration

	BulkheadName          string
	BulkheadMaxConcurrent int
	BulkheadMaxWait       time.Duration

	RateLimiterName        string
	RateLimitForPeriod     int
	RateLimitRefreshPeriod time.Duration
	RateLimitTimeout       time.Duration

	OTelEndpoint      string
	OTelInsecure      bool
	OTelMetricsPrefix string
}

func loadConfig() config {
	windowType := resilience.CountBased
	if strings.EqualFold(getEnv("RES_CB_SLIDING_WINDOW_TYPE", "COUNT_BASED"), "TIME_BASED") {
		windowType = resilience.TimeBased
	}

	return config{
		ServiceName: getEnv("SERVICE_NAME", "resilience-otel-env"),
		RunDuration: mustDuration("RUN_DURATION", 60*time.Second),
		LoopDelay:   mustDuration("LOOP_DELAY", 250*time.Millisecond),

		CBName:                      getEnv("RES_CB_NAME", "env-cb"),
		CBWindowType:                windowType,
		CBWindowSize:                mustInt("RES_CB_SLIDING_WINDOW_SIZE", 20),
		CBMinimumCalls:              mustInt("RES_CB_MINIMUM_NUMBER_OF_CALLS", 10),
		CBFailureRateThreshold:      mustFloat("RES_CB_FAILURE_RATE_THRESHOLD", 50),
		CBSlowCallRateThreshold:     mustFloat("RES_CB_SLOW_CALL_RATE_THRESHOLD", 80),
		CBSlowCallDurationThreshold: mustDuration("RES_CB_SLOW_CALL_DURATION_THRESHOLD", 500*time.Millisecond),
		CBHalfOpenPermits:           mustInt("RES_CB_PERMITTED_CALLS_HALF_OPEN", 5),
		CBWaitOpen:                  mustDuration("RES_CB_WAIT_DURATION_OPEN_STATE", 15*time.Second),
		CBAutoTransition:            mustBool("RES_CB_AUTO_TRANSITION_OPEN_TO_HALF_OPEN", true),

		RetryName:              getEnv("RES_RETRY_NAME", "env-retry"),
		RetryMaxAttempts:       mustInt("RES_RETRY_MAX_ATTEMPTS", 3),
		RetryWait:              mustDuration("RES_RETRY_WAIT_DURATION", 200*time.Millisecond),
		RetryBackoffMultiplier: mustFloat("RES_RETRY_BACKOFF_MULTIPLIER", 2.0),
		RetryMaxInterval:       mustDuration("RES_RETRY_MAX_INTERVAL", 2*time.Second),

		BulkheadName:          getEnv("RES_BH_NAME", "env-bulkhead"),
		BulkheadMaxConcurrent: mustInt("RES_BH_MAX_CONCURRENT_CALLS", 50),
		BulkheadMaxWait:       mustDuration("RES_BH_MAX_WAIT_DURATION", 25*time.Millisecond),

		RateLimiterName:        getEnv("RES_RL_NAME", "env-rate-limiter"),
		RateLimitForPeriod:     mustInt("RES_RL_LIMIT_FOR_PERIOD", 100),
		RateLimitRefreshPeriod: mustDuration("RES_RL_LIMIT_REFRESH_PERIOD", time.Second),
		RateLimitTimeout:       mustDuration("RES_RL_TIMEOUT_DURATION", 25*time.Millisecond),

		OTelEndpoint:      getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
		OTelInsecure:      mustBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		OTelMetricsPrefix: getEnv("RESILIENCE_OTEL_METRICS_PREFIX", "resilience.demo"),
	}
}

func setupOTel(cfg config) (func(context.Context) error, error) {
	opts := []otlpmetrichttp.Option{}
	endpoint := strings.TrimSpace(cfg.OTelEndpoint)
	if endpoint != "" {
		if strings.Contains(endpoint, "://") {
			u, err := url.Parse(endpoint)
			if err != nil {
				return nil, fmt.Errorf("parse OTEL_EXPORTER_OTLP_ENDPOINT: %w", err)
			}
			opts = append(opts, otlpmetrichttp.WithEndpoint(u.Host))
			if u.Path != "" && u.Path != "/" {
				opts = append(opts, otlpmetrichttp.WithURLPath(u.Path))
			}
		} else {
			opts = append(opts, otlpmetrichttp.WithEndpoint(endpoint))
		}
	}
	if cfg.OTelInsecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}
	exporter, err := otlpmetrichttp.New(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("create otlp metrics exporter: %w", err)
	}

	resource, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter, metric.WithInterval(5*time.Second))),
		metric.WithResource(resource),
	)
	otel.SetMeterProvider(mp)

	return mp.Shutdown, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return fallback
}

func mustFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return n
		}
	}
	return fallback
}

func mustBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}

func mustDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return fallback
}
