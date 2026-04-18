# resilience

Production-grade resilience primitives for Go inspired by Resilience4j.

> Requires Go 1.25.3 or newer.

## Feature comparison vs Resilience4j

| Feature | Resilience4j | `resilience` |
|---|---|---|
| Count-based sliding window | Yes | Yes |
| Time-based sliding window | Yes | Yes |
| Failure-rate threshold trip | Yes | Yes |
| Slow-call rate threshold trip | Yes | Yes |
| Half-open permitted probes | Yes | Yes |
| Open state wait duration | Yes | Yes |
| Automatic open->half-open transition | Yes | Yes |
| Ignore exceptions | Yes | Yes (predicate) |
| Record exceptions (whitelist) | Yes | Yes (predicate) |
| Typed event publishing | Yes | Yes |
| Metrics snapshot API | Yes | Yes |
| Retry decorator | Yes | Yes |
| Bulkhead | Yes | Yes |
| Rate limiter | Yes | Yes |
| Fluent decorator composition | Yes | Yes |

## Installation

```bash
go get github.com/sony/gobreaker
go get golang.org/x/time/rate
go get github.com/gin-gonic/gin
go get go.opentelemetry.io/otel/metric
```

## Quick start

```go
package main

import (
	"context"
	"errors"
	"time"

	"resilience/resilience"
)

var ErrValidation = errors.New("validation error")

func callPaymentAPI(ctx context.Context) (interface{}, error) {
	return "ok", nil
}

func main() {
	cb := resilience.NewCircuitBreaker("payment-service",
		resilience.WithSlidingWindowType(resilience.TimeBased),
		resilience.WithSlidingWindowSize(10),
		resilience.WithFailureRateThreshold(50),
		resilience.WithSlowCallDurationThreshold(2*time.Second),
		resilience.WithSlowCallRateThreshold(80),
		resilience.WithWaitDurationInOpenState(30*time.Second),
		resilience.WithIgnoreErrors(func(err error) bool {
			return errors.Is(err, ErrValidation)
		}),
	)

	retry := resilience.NewRetry("payment-retry",
		resilience.WithMaxAttempts(3),
		resilience.WithWaitDuration(500*time.Millisecond),
		resilience.WithExponentialBackoff(2.0, 5*time.Second),
	)

	bh := resilience.NewBulkhead("payment-bulkhead",
		resilience.WithMaxConcurrentCalls(25),
		resilience.WithMaxWaitDuration(100*time.Millisecond),
	)

	rl := resilience.NewRateLimiter("payment-rate",
		resilience.WithLimitForPeriod(100),
		resilience.WithLimitRefreshPeriod(time.Second),
		resilience.WithTimeoutDuration(100*time.Millisecond),
	)

	_, _ = resilience.Decorate(callPaymentAPI).
		WithRateLimiter(rl).
		WithBulkhead(bh).
		WithCircuitBreaker(cb).
		WithRetry(retry).
		Call(context.Background())
}
```

## Resilience4j parity notes

This package is intentionally modeled after Resilience4j’s core behaviors:

- sliding windows (`COUNT_BASED`, `TIME_BASED`)
- failure-rate and slow-call-rate tripping
- half-open probing limits
- open wait duration and automatic transition
- ignore/record predicates
- event publishing and metrics snapshots
- retry, bulkhead, rate limiter, and decorator composition

The implementation uses Go idioms and `github.com/sony/gobreaker` as the state-machine core, so exact 1:1 bytecode-level parity with Java internals is not meaningful, but behavior and operational patterns are aligned for production usage.

## Integration examples

### 1) Wrapping outbound REST API calls (`net/http`)

```go
package payment

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"resilience/resilience"
)

type Client struct {
	httpClient *http.Client
	cb         *resilience.CircuitBreaker
	retry      *resilience.Retry
	bulkhead   *resilience.Bulkhead
	rate       *resilience.RateLimiter
}

func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 5 * time.Second},
		cb: resilience.NewCircuitBreaker("payment-http",
			resilience.WithSlidingWindowType(resilience.TimeBased),
			resilience.WithSlidingWindowSize(10),
			resilience.WithMinimumNumberOfCalls(20),
			resilience.WithFailureRateThreshold(50),
			resilience.WithSlowCallDurationThreshold(2*time.Second),
			resilience.WithSlowCallRateThreshold(80),
			resilience.WithWaitDurationInOpenState(30*time.Second),
		),
		retry: resilience.NewRetry("payment-retry",
			resilience.WithMaxAttempts(3),
			resilience.WithWaitDuration(200*time.Millisecond),
			resilience.WithExponentialBackoff(2.0, 2*time.Second),
			resilience.WithRetryOn(func(err error) bool {
				return !errors.Is(err, context.Canceled)
			}),
		),
		bulkhead: resilience.NewBulkhead("payment-bulkhead",
			resilience.WithMaxConcurrentCalls(50),
			resilience.WithMaxWaitDuration(50*time.Millisecond),
		),
		rate: resilience.NewRateLimiter("payment-rate",
			resilience.WithLimitForPeriod(200),
			resilience.WithLimitRefreshPeriod(time.Second),
			resilience.WithTimeoutDuration(25*time.Millisecond),
		),
	}
}

func (c *Client) Charge(ctx context.Context, req *http.Request) (map[string]any, error) {
	result, err := resilience.Decorate(func(ctx context.Context) (interface{}, error) {
		resp, err := c.httpClient.Do(req.WithContext(ctx))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			return nil, errors.New("upstream 5xx")
		}
		if resp.StatusCode >= 400 {
			return nil, nil
		}

		var payload map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}).
		WithRateLimiter(c.rate).
		WithBulkhead(c.bulkhead).
		WithCircuitBreaker(c.cb).
		WithRetry(c.retry).
		Call(ctx)
	if err != nil {
		return nil, err
	}
	return result.(map[string]any), nil
}
```

### 2) Wrapping DB calls (`database/sql`)

```go
package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"resilience/resilience"
)

type Repo struct {
	db       *sql.DB
	cb       *resilience.CircuitBreaker
	retry    *resilience.Retry
	bulkhead *resilience.Bulkhead
}

func NewRepo(db *sql.DB) *Repo {
	return &Repo{
		db: db,
		cb: resilience.NewCircuitBreaker("orders-db",
			resilience.WithSlidingWindowType(resilience.CountBased),
			resilience.WithSlidingWindowSize(100),
			resilience.WithMinimumNumberOfCalls(50),
			resilience.WithFailureRateThreshold(40),
			resilience.WithSlowCallDurationThreshold(250*time.Millisecond),
			resilience.WithSlowCallRateThreshold(60),
			resilience.WithWaitDurationInOpenState(15*time.Second),
		),
		retry: resilience.NewRetry("orders-db-retry",
			resilience.WithMaxAttempts(2),
			resilience.WithWaitDuration(50*time.Millisecond),
			resilience.WithRetryOn(func(err error) bool {
				return err != nil && !errors.Is(err, sql.ErrNoRows)
			}),
		),
		bulkhead: resilience.NewBulkhead("orders-db-bulkhead",
			resilience.WithMaxConcurrentCalls(30),
			resilience.WithMaxWaitDuration(20*time.Millisecond),
		),
	}
}

func (r *Repo) GetOrder(ctx context.Context, id int64) (string, error) {
	out, err := resilience.Decorate(func(ctx context.Context) (interface{}, error) {
		var status string
		err := r.db.QueryRowContext(ctx, "SELECT status FROM orders WHERE id = ?", id).Scan(&status)
		if err != nil {
			return "", err
		}
		return status, nil
	}).
		WithBulkhead(r.bulkhead).
		WithCircuitBreaker(r.cb).
		WithRetry(r.retry).
		Call(ctx)
	if err != nil {
		return "", err
	}
	return out.(string), nil
}
```

### 3) Wrapping internal service calls from HTTP handlers

```go
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	resp, err := resilience.Decorate(func(ctx context.Context) (interface{}, error) {
		return h.orderService.Process(ctx, r.URL.Query().Get("id"))
	}).
		WithCircuitBreaker(h.cb).
		WithRetry(h.retry).
		Call(ctx)

	if err != nil {
		if errors.Is(err, resilience.ErrCircuitOpen) {
			http.Error(w, "dependency unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(resp)
}
```

### 4) Copy-paste middleware wrappers

#### Gorilla Mux (`net/http` middleware)

```go
r := mux.NewRouter()

cb := resilience.NewCircuitBreaker("http-cb")
retry := resilience.NewRetry("http-retry", resilience.WithMaxAttempts(2))
bh := resilience.NewBulkhead("http-bh", resilience.WithMaxConcurrentCalls(200))
rl := resilience.NewRateLimiter("http-rl", resilience.WithLimitForPeriod(500), resilience.WithLimitRefreshPeriod(time.Second))

r.Use(resilience.GorillaMuxMiddleware(resilience.MiddlewareConfig{
	CircuitBreaker: cb,
	Retry:          retry,
	Bulkhead:       bh,
	RateLimiter:    rl,
}))

r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})
```

#### Gin middleware

```go
gin.SetMode(gin.ReleaseMode)
router := gin.New()

cb := resilience.NewCircuitBreaker("gin-cb")
retry := resilience.NewRetry("gin-retry", resilience.WithMaxAttempts(2))
bh := resilience.NewBulkhead("gin-bh", resilience.WithMaxConcurrentCalls(200))
rl := resilience.NewRateLimiter("gin-rl", resilience.WithLimitForPeriod(500), resilience.WithLimitRefreshPeriod(time.Second))

router.Use(resilience.GinMiddleware(resilience.MiddlewareConfig{
	CircuitBreaker: cb,
	Retry:          retry,
	Bulkhead:       bh,
	RateLimiter:    rl,
}))

router.GET("/v1/orders/:id", func(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id")})
})
```

### 5) OpenTelemetry metrics export bridge

```go
import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"resilience/resilience"
)

func setupObs(cb *resilience.CircuitBreaker, r *resilience.Retry, bh *resilience.Bulkhead, rl *resilience.RateLimiter) (*resilience.OTelBridge, error) {
	meter := otel.Meter("my-service")
	bridge, err := resilience.NewOTelBridge(resilience.OTelBridgeConfig{
		Meter:         meter,
		MetricsPrefix: "myservice.resilience",
	})
	if err != nil {
		return nil, err
	}

	bridge.RegisterCircuitBreaker(cb)
	bridge.RegisterRetry(r)
	bridge.RegisterBulkhead(bh)
	bridge.RegisterRateLimiter(rl)
	return bridge, nil
}

func shutdownObs(ctx context.Context, bridge *resilience.OTelBridge) {
	if bridge != nil {
		_ = bridge.Shutdown(ctx)
	}
}
```

### 6) Full env-driven configuration example (all parameters)

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"resilience/resilience"
)

func getenvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvFloat(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func getenvBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func getenvString(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func main() {
	// Circuit breaker env vars
	cbName := getenvString("RES_CB_NAME", "orders-cb")
	cbWindowType := strings.ToUpper(getenvString("RES_CB_SLIDING_WINDOW_TYPE", "COUNT_BASED"))
	cbWindowSize := getenvInt("RES_CB_SLIDING_WINDOW_SIZE", 100)
	cbMinCalls := getenvInt("RES_CB_MINIMUM_NUMBER_OF_CALLS", 100)
	cbFailureRate := getenvFloat("RES_CB_FAILURE_RATE_THRESHOLD", 50)
	cbSlowRate := getenvFloat("RES_CB_SLOW_CALL_RATE_THRESHOLD", 100)
	cbSlowDuration := getenvDuration("RES_CB_SLOW_CALL_DURATION_THRESHOLD", 60*time.Second)
	cbHalfOpenPermits := getenvInt("RES_CB_PERMITTED_CALLS_HALF_OPEN", 10)
	cbOpenWait := getenvDuration("RES_CB_WAIT_DURATION_OPEN_STATE", 60*time.Second)
	cbAutoTransition := getenvBool("RES_CB_AUTO_TRANSITION_OPEN_TO_HALF_OPEN", true)

	windowType := resilience.CountBased
	if cbWindowType == "TIME_BASED" {
		windowType = resilience.TimeBased
	}

	cb := resilience.NewCircuitBreaker(cbName,
		resilience.WithSlidingWindowType(windowType),
		resilience.WithSlidingWindowSize(cbWindowSize),
		resilience.WithMinimumNumberOfCalls(cbMinCalls),
		resilience.WithFailureRateThreshold(cbFailureRate),
		resilience.WithSlowCallRateThreshold(cbSlowRate),
		resilience.WithSlowCallDurationThreshold(cbSlowDuration),
		resilience.WithPermittedNumberOfCallsInHalfOpenState(cbHalfOpenPermits),
		resilience.WithWaitDurationInOpenState(cbOpenWait),
		resilience.WithAutomaticTransitionFromOpenToHalfOpen(cbAutoTransition),
		resilience.WithIgnoreErrors(func(err error) bool {
			// example: ignore business validation errors
			return strings.Contains(strings.ToLower(err.Error()), "validation")
		}),
		resilience.WithRecordErrors(func(err error) bool {
			// example: only record timeouts/connection errors
			s := strings.ToLower(err.Error())
			return strings.Contains(s, "timeout") || strings.Contains(s, "connection")
		}),
	)

	// Retry env vars
	retry := resilience.NewRetry(getenvString("RES_RETRY_NAME", "orders-retry"),
		resilience.WithMaxAttempts(getenvInt("RES_RETRY_MAX_ATTEMPTS", 3)),
		resilience.WithWaitDuration(getenvDuration("RES_RETRY_WAIT_DURATION", 200*time.Millisecond)),
		resilience.WithExponentialBackoff(
			getenvFloat("RES_RETRY_BACKOFF_MULTIPLIER", 2.0),
			getenvDuration("RES_RETRY_MAX_INTERVAL", 5*time.Second),
		),
		resilience.WithRetryOn(func(err error) bool {
			if err == nil {
				return false
			}
			return !errors.Is(err, context.Canceled)
		}),
		resilience.WithOnRetry(func(attempt int, err error) {
			fmt.Printf("retry attempt=%d err=%v\n", attempt, err)
		}),
	)

	// Bulkhead env vars
	bh := resilience.NewBulkhead(getenvString("RES_BH_NAME", "orders-bh"),
		resilience.WithMaxConcurrentCalls(getenvInt("RES_BH_MAX_CONCURRENT_CALLS", 50)),
		resilience.WithMaxWaitDuration(getenvDuration("RES_BH_MAX_WAIT_DURATION", 25*time.Millisecond)),
	)

	// Rate limiter env vars
	rl := resilience.NewRateLimiter(getenvString("RES_RL_NAME", "orders-rl"),
		resilience.WithLimitForPeriod(getenvInt("RES_RL_LIMIT_FOR_PERIOD", 200)),
		resilience.WithLimitRefreshPeriod(getenvDuration("RES_RL_LIMIT_REFRESH_PERIOD", time.Second)),
		resilience.WithTimeoutDuration(getenvDuration("RES_RL_TIMEOUT_DURATION", 20*time.Millisecond)),
	)

	// Use all decorators together
	result, err := resilience.Decorate(func(ctx context.Context) (interface{}, error) {
		// place your db/rest logic here
		return "ok", nil
	}).
		WithRateLimiter(rl).
		WithBulkhead(bh).
		WithCircuitBreaker(cb).
		WithRetry(retry).
		Call(context.Background())

	fmt.Printf("result=%v err=%v\n", result, err)
}
```

Example `.env` values:

```bash
RES_CB_NAME=orders-cb
RES_CB_SLIDING_WINDOW_TYPE=TIME_BASED
RES_CB_SLIDING_WINDOW_SIZE=10
RES_CB_MINIMUM_NUMBER_OF_CALLS=20
RES_CB_FAILURE_RATE_THRESHOLD=50
RES_CB_SLOW_CALL_RATE_THRESHOLD=80
RES_CB_SLOW_CALL_DURATION_THRESHOLD=2s
RES_CB_PERMITTED_CALLS_HALF_OPEN=5
RES_CB_WAIT_DURATION_OPEN_STATE=30s
RES_CB_AUTO_TRANSITION_OPEN_TO_HALF_OPEN=true

RES_RETRY_NAME=orders-retry
RES_RETRY_MAX_ATTEMPTS=3
RES_RETRY_WAIT_DURATION=200ms
RES_RETRY_BACKOFF_MULTIPLIER=2.0
RES_RETRY_MAX_INTERVAL=2s

RES_BH_NAME=orders-bh
RES_BH_MAX_CONCURRENT_CALLS=50
RES_BH_MAX_WAIT_DURATION=20ms

RES_RL_NAME=orders-rl
RES_RL_LIMIT_FOR_PERIOD=200
RES_RL_LIMIT_REFRESH_PERIOD=1s
RES_RL_TIMEOUT_DURATION=20ms
```

## Configuration reference

### Circuit breaker (`Config`)

| Field | Type | Default | Description |
|---|---|---|---|
| `SlidingWindowType` | `SlidingWindowType` | `CountBased` | `CountBased` or `TimeBased` |
| `SlidingWindowSize` | `int` | `100` | N calls or N seconds |
| `MinimumNumberOfCalls` | `int` | `100` | Minimum samples before tripping |
| `FailureRateThreshold` | `float64` | `50` | Failure rate percentage threshold |
| `SlowCallRateThreshold` | `float64` | `100` | Slow-call rate percentage threshold |
| `SlowCallDurationThreshold` | `time.Duration` | `60s` | Calls over this duration are slow |
| `PermittedNumberOfCallsInHalfOpen` | `int` | `10` | Allowed probes in half-open |
| `WaitDurationInOpenState` | `time.Duration` | `60s` | Open-state wait before half-open |
| `AutomaticTransitionFromOpenToHalfOpen` | `bool` | `true` | Automatic transition behavior |
| `RecordErrorPredicate` | `func(error) bool` | `nil` | Whitelist of failure errors |
| `IgnoreErrorPredicate` | `func(error) bool` | `nil` | Excluded errors |

### Retry

| Option | Default | Description |
|---|---|---|
| `WithMaxAttempts(int)` | `3` | Max total attempts |
| `WithWaitDuration(time.Duration)` | `0` | Base wait between attempts |
| `WithExponentialBackoff(float64,time.Duration)` | disabled | Multiplier and max interval |
| `WithRetryOn(func(error) bool)` | all non-nil | Predicate for retryable errors |
| `WithOnRetry(func(int,error))` | nil | Retry hook |

### Bulkhead

| Option | Default | Description |
|---|---|---|
| `WithMaxConcurrentCalls(int)` | `25` | Max concurrent calls |
| `WithMaxWaitDuration(time.Duration)` | `0` | Wait timeout to acquire slot |

### Rate limiter

| Option | Default | Description |
|---|---|---|
| `WithLimitForPeriod(int)` | `50` | Tokens per refresh period |
| `WithLimitRefreshPeriod(time.Duration)` | `1s` | Period length |
| `WithTimeoutDuration(time.Duration)` | `0` | Wait timeout for permission |

## Metrics snapshot

`CircuitBreaker.Metrics()` returns:

- `FailureRate float64`
- `SlowCallRate float64`
- `NumberOfBufferedCalls int`
- `NumberOfFailedCalls int`
- `NumberOfSlowCalls int`
- `NumberOfSuccessfulCalls int`
- `State string`
