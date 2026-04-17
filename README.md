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
