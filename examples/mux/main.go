package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"resilience/resilience"
)

func main() {
	cfg := loadConfig()

	cb := resilience.NewCircuitBreaker("mux-cb",
		resilience.WithSlidingWindowType(resilience.TimeBased),
		resilience.WithSlidingWindowSize(cfg.SlidingWindowSize),
		resilience.WithMinimumNumberOfCalls(cfg.MinimumCalls),
		resilience.WithFailureRateThreshold(cfg.FailureRateThreshold),
		resilience.WithSlowCallDurationThreshold(cfg.SlowCallThreshold),
		resilience.WithSlowCallRateThreshold(cfg.SlowCallRateThreshold),
		resilience.WithWaitDurationInOpenState(cfg.OpenWait),
		resilience.WithPermittedNumberOfCallsInHalfOpenState(cfg.HalfOpenPermits),
	)

	retry := resilience.NewRetry("mux-retry",
		resilience.WithMaxAttempts(cfg.RetryMaxAttempts),
		resilience.WithWaitDuration(cfg.RetryWait),
		resilience.WithExponentialBackoff(cfg.RetryBackoffMultiplier, cfg.RetryMaxInterval),
	)

	bulkhead := resilience.NewBulkhead("mux-bulkhead",
		resilience.WithMaxConcurrentCalls(cfg.BulkheadMaxConcurrent),
		resilience.WithMaxWaitDuration(cfg.BulkheadMaxWait),
	)

	rateLimiter := resilience.NewRateLimiter("mux-rl",
		resilience.WithLimitForPeriod(cfg.RateLimitForPeriod),
		resilience.WithLimitRefreshPeriod(cfg.RateLimitRefreshPeriod),
		resilience.WithTimeoutDuration(cfg.RateLimitTimeout),
	)

	r := mux.NewRouter()
	r.Use(resilience.GorillaMuxMiddleware(resilience.MiddlewareConfig{
		CircuitBreaker: cb,
		Retry:          retry,
		Bulkhead:       bulkhead,
		RateLimiter:    rateLimiter,
	}))

	r.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}).Methods(http.MethodGet)

	r.HandleFunc("/demo", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("fail") == "1" {
			http.Error(w, "simulated failure", http.StatusInternalServerError)
			return
		}
		payload := map[string]any{
			"path":      "/demo",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}).Methods(http.MethodGet)

	log.Printf("mux example listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, r); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	ListenAddr             string
	SlidingWindowSize      int
	MinimumCalls           int
	FailureRateThreshold   float64
	SlowCallRateThreshold  float64
	SlowCallThreshold      time.Duration
	OpenWait               time.Duration
	HalfOpenPermits        int
	RetryMaxAttempts       int
	RetryWait              time.Duration
	RetryBackoffMultiplier float64
	RetryMaxInterval       time.Duration
	BulkheadMaxConcurrent  int
	BulkheadMaxWait        time.Duration
	RateLimitForPeriod     int
	RateLimitRefreshPeriod time.Duration
	RateLimitTimeout       time.Duration
}

func loadConfig() config {
	return config{
		ListenAddr:             getEnv("MUX_LISTEN_ADDR", ":8081"),
		SlidingWindowSize:      mustInt("MUX_CB_SLIDING_WINDOW_SIZE", 20),
		MinimumCalls:           mustInt("MUX_CB_MIN_CALLS", 10),
		FailureRateThreshold:   mustFloat("MUX_CB_FAILURE_RATE_THRESHOLD", 50),
		SlowCallRateThreshold:  mustFloat("MUX_CB_SLOW_CALL_RATE_THRESHOLD", 80),
		SlowCallThreshold:      mustDuration("MUX_CB_SLOW_CALL_DURATION_THRESHOLD", 500*time.Millisecond),
		OpenWait:               mustDuration("MUX_CB_OPEN_WAIT", 15*time.Second),
		HalfOpenPermits:        mustInt("MUX_CB_HALF_OPEN_PERMITS", 5),
		RetryMaxAttempts:       mustInt("MUX_RETRY_MAX_ATTEMPTS", 2),
		RetryWait:              mustDuration("MUX_RETRY_WAIT", 100*time.Millisecond),
		RetryBackoffMultiplier: mustFloat("MUX_RETRY_BACKOFF_MULTIPLIER", 2),
		RetryMaxInterval:       mustDuration("MUX_RETRY_MAX_INTERVAL", 2*time.Second),
		BulkheadMaxConcurrent:  mustInt("MUX_BH_MAX_CONCURRENT", 64),
		BulkheadMaxWait:        mustDuration("MUX_BH_MAX_WAIT", 50*time.Millisecond),
		RateLimitForPeriod:     mustInt("MUX_RL_LIMIT_FOR_PERIOD", 200),
		RateLimitRefreshPeriod: mustDuration("MUX_RL_REFRESH_PERIOD", time.Second),
		RateLimitTimeout:       mustDuration("MUX_RL_TIMEOUT", 50*time.Millisecond),
	}
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

func mustDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return fallback
}

var _ = errors.Is
