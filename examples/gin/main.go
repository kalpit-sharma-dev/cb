package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"resilience/resilience"
)

func main() {
	cfg := loadConfig()

	cb := resilience.NewCircuitBreaker("gin-cb",
		resilience.WithSlidingWindowType(resilience.TimeBased),
		resilience.WithSlidingWindowSize(cfg.SlidingWindowSize),
		resilience.WithMinimumNumberOfCalls(cfg.MinimumCalls),
		resilience.WithFailureRateThreshold(cfg.FailureRateThreshold),
		resilience.WithSlowCallDurationThreshold(cfg.SlowCallThreshold),
		resilience.WithSlowCallRateThreshold(cfg.SlowCallRateThreshold),
		resilience.WithWaitDurationInOpenState(cfg.OpenWait),
		resilience.WithPermittedNumberOfCallsInHalfOpenState(cfg.HalfOpenPermits),
	)

	bulkhead := resilience.NewBulkhead("gin-bulkhead",
		resilience.WithMaxConcurrentCalls(cfg.BulkheadMaxConcurrent),
		resilience.WithMaxWaitDuration(cfg.BulkheadMaxWait),
	)

	rateLimiter := resilience.NewRateLimiter("gin-rl",
		resilience.WithLimitForPeriod(cfg.RateLimitForPeriod),
		resilience.WithLimitRefreshPeriod(cfg.RateLimitRefreshPeriod),
		resilience.WithTimeoutDuration(cfg.RateLimitTimeout),
	)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(resilience.GinMiddleware(resilience.MiddlewareConfig{
		CircuitBreaker: cb,
		Bulkhead:       bulkhead,
		RateLimiter:    rateLimiter,
	}))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/demo", func(c *gin.Context) {
		if c.Query("slow") == "1" {
			time.Sleep(cfg.SlowCallThreshold + 50*time.Millisecond)
		}
		if c.Query("fail") == "1" {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "simulated failure"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"path":      "/demo",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})

	log.Printf("gin example listening on %s", cfg.ListenAddr)
	if err := r.Run(cfg.ListenAddr); err != nil {
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
		ListenAddr:             getEnv("GIN_LISTEN_ADDR", ":8082"),
		SlidingWindowSize:      mustInt("GIN_CB_SLIDING_WINDOW_SIZE", 20),
		MinimumCalls:           mustInt("GIN_CB_MIN_CALLS", 10),
		FailureRateThreshold:   mustFloat("GIN_CB_FAILURE_RATE_THRESHOLD", 50),
		SlowCallRateThreshold:  mustFloat("GIN_CB_SLOW_CALL_RATE_THRESHOLD", 80),
		SlowCallThreshold:      mustDuration("GIN_CB_SLOW_CALL_DURATION_THRESHOLD", 500*time.Millisecond),
		OpenWait:               mustDuration("GIN_CB_OPEN_WAIT", 15*time.Second),
		HalfOpenPermits:        mustInt("GIN_CB_HALF_OPEN_PERMITS", 5),
		RetryMaxAttempts:       mustInt("GIN_RETRY_MAX_ATTEMPTS", 2),
		RetryWait:              mustDuration("GIN_RETRY_WAIT", 100*time.Millisecond),
		RetryBackoffMultiplier: mustFloat("GIN_RETRY_BACKOFF_MULTIPLIER", 2),
		RetryMaxInterval:       mustDuration("GIN_RETRY_MAX_INTERVAL", 2*time.Second),
		BulkheadMaxConcurrent:  mustInt("GIN_BH_MAX_CONCURRENT", 64),
		BulkheadMaxWait:        mustDuration("GIN_BH_MAX_WAIT", 50*time.Millisecond),
		RateLimitForPeriod:     mustInt("GIN_RL_LIMIT_FOR_PERIOD", 200),
		RateLimitRefreshPeriod: mustDuration("GIN_RL_REFRESH_PERIOD", time.Second),
		RateLimitTimeout:       mustDuration("GIN_RL_TIMEOUT", 50*time.Millisecond),
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
