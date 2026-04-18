# resilience

Production-grade resilience primitives for Go inspired by Resilience4j.

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
				// retry only transport / 5xx mapped errors
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
			// business/client errors are often non-retryable
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
	db      *sql.DB
	cb      *resilience.CircuitBreaker
	retry   *resilience.Retry
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
				// only retry transient DB failures
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

### 4) Typical error handling in callers

```go
result, err := resilience.Decorate(fn).
	WithCircuitBreaker(cb).
	WithRetry(retry).
	WithBulkhead(bh).
	WithRateLimiter(rl).
	Call(ctx)

switch {
case err == nil:
	_ = result
case errors.Is(err, resilience.ErrCircuitOpen):
	// fallback or fast-fail
case errors.Is(err, resilience.ErrBulkheadFull):
	// load-shed
case errors.Is(err, resilience.ErrRateLimitExceeded):
	// backpressure
default:
	// domain/transport error
}
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
