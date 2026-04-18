# resilience

Production-grade resilience primitives for Go inspired by Resilience4j.

> Go version: **1.25.3+**

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
| Manual open->half-open transition | Yes | Yes (`TransitionToHalfOpen`) |
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

## Runnable examples

The repository includes runnable programs:

- `examples/mux` — Gorilla Mux middleware integration
- `examples/gin` — Gin middleware integration
- `examples/otel-env` — full env-driven configuration + OpenTelemetry bridge
- `examples/manual-half-open` — explicit manual OPEN -> HALF_OPEN transition flow
- `examples/retry` — retry with backoff and retry predicate
- `examples/bulkhead` — semaphore bulkhead behavior under concurrency
- `examples/rate-limiter` — token-bucket rate-limiter behavior
- `examples/decorator` — full chain composition in one place
- `examples/outbound-rest` — wrapping outbound REST client calls
- `examples/db-call` — wrapping repository/DB-style calls

Run them:

```bash
go run ./examples/mux
```

```bash
go run ./examples/gin
```

```bash
go run ./examples/otel-env
```

```bash
go run ./examples/manual-half-open
```

```bash
go run ./examples/retry
```

```bash
go run ./examples/bulkhead
```

```bash
go run ./examples/rate-limiter
```

```bash
go run ./examples/decorator
```

```bash
go run ./examples/outbound-rest
```

```bash
go run ./examples/db-call
```

### Example endpoints

- Mux example: `http://localhost:8081/health`, `http://localhost:8081/demo?fail=1`
- Gin example: `http://localhost:8082/health`, `http://localhost:8082/demo?slow=1`

## Docker Compose observability demo (OTEL -> Prometheus -> Grafana)

Included assets:

- `docker-compose.yml`
- `observability/otelcol/config.yaml`
- `observability/prometheus.yml`
- `observability/grafana/provisioning/...`
- `observability/grafana/dashboards/resilience-overview.json`

Start full stack:

```bash
docker compose up --build
```

Services:

- OTEL Collector: `http://localhost:4318` (OTLP HTTP), `:4317` (OTLP gRPC)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)
- Example producer: `otel-env-example` service in compose

The example app emits resilience events/snapshots through the bridge using metric prefix:

- `resilience.demo.*`

Convenience commands:

```bash
make demo-up
make demo-run-example
make demo-down
```

Example PromQL:

```promql
rate(resilience_demo_retry_events_total[1m])
```

```promql
resilience_demo_cb_failure_rate
```

## Prebuilt Grafana alert rules

Provisioned alert rules are included under:

- `observability/grafana/provisioning/alerting/resilience-alerts.yaml`

Included alerts:

- **Resilience High Failure Rate**
  - Trigger: `max(resilience_demo_cb_failure_rate) > 70` for 2m
- **Resilience High RateLimiter Denied Rate**
  - Trigger: `sum(rate(resilience_demo_rate_limiter_events_total{outcome="denied"}[1m])) > 10` for 2m

Default contact point/policy provisioning files:

- `observability/grafana/provisioning/alerting/contact-points.yaml`
- `observability/grafana/provisioning/alerting/notification-policies.yaml`

You can replace the default contact point with Slack/PagerDuty/Webhook receivers in those files.

## Load generator script

Script: `scripts/load.sh`

Profiles:

- `happy`   -> mostly successful traffic
- `failure` -> mostly `/demo?fail=1`
- `slow`    -> mostly `/demo?slow=1`
- `mixed`   -> blend of success/slow/failure

Examples:

```bash
# against gin example
BASE_URL=http://localhost:8082 PROFILE=mixed DURATION_SECONDS=120 CONCURRENCY=40 ./scripts/load.sh
```

```bash
# aggressive failure profile
BASE_URL=http://localhost:8082 PROFILE=failure DURATION_SECONDS=90 CONCURRENCY=30 ./scripts/load.sh
```

```bash
# gentle happy traffic with per-request delay
BASE_URL=http://localhost:8082 PROFILE=happy DURATION_SECONDS=60 CONCURRENCY=10 REQUEST_DELAY_MS=50 ./scripts/load.sh
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
bh := resilience.NewBulkhead("http-bh", resilience.WithMaxConcurrentCalls(200))
rl := resilience.NewRateLimiter("http-rl", resilience.WithLimitForPeriod(500), resilience.WithLimitRefreshPeriod(time.Second))

r.Use(resilience.GorillaMuxMiddleware(resilience.MiddlewareConfig{
	CircuitBreaker: cb,
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
bh := resilience.NewBulkhead("gin-bh", resilience.WithMaxConcurrentCalls(200))
rl := resilience.NewRateLimiter("gin-rl", resilience.WithLimitForPeriod(500), resilience.WithLimitRefreshPeriod(time.Second))

router.Use(resilience.GinMiddleware(resilience.MiddlewareConfig{
	CircuitBreaker: cb,
	Bulkhead:       bh,
	RateLimiter:    rl,
}))

router.GET("/v1/orders/:id", func(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id")})
})
```

> Middleware note: inbound HTTP middleware intentionally does **not** apply retries to handlers, because retrying after a response is partially/fully written can cause duplicate side effects and invalid HTTP writes.

### 5) OpenTelemetry metrics export bridge

```go
import (
	"context"

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

See runnable `examples/otel-env/main.go` and `examples/otel-env/.env.example`.

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
| `AutomaticTransitionFromOpenToHalfOpen` | `bool` | `true` | If `false`, open state requires explicit `TransitionToHalfOpen()` call before probe requests are allowed |
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

## Manual transition example

When `AutomaticTransitionFromOpenToHalfOpen` is set to `false`, callers remain blocked in OPEN
until you request a probe transition:

```go
cb := resilience.NewCircuitBreaker("manual",
	resilience.WithAutomaticTransitionFromOpenToHalfOpen(false),
)

if cb.State() == resilience.StateOpen {
	ok := cb.TransitionToHalfOpen()
	if ok {
		// next allowed call becomes the HALF_OPEN probe
	}
}
```

### Manual half-open operational guidance

Use manual transition mode when you need external control over recovery probes (for example,
after a deployment rollback, dependency failover, or synthetic health check signal).

Recommended pattern:

1. Keep breaker in OPEN while downstream is unhealthy.
2. Trigger `TransitionToHalfOpen()` only after an explicit health signal (not every request).
3. Allow a small probe budget with `WithPermittedNumberOfCallsInHalfOpenState`.
4. If probes succeed, breaker closes automatically; if probes fail, breaker reopens.

Avoid:

- Calling `TransitionToHalfOpen()` from every request path.
- Using large half-open probe counts during unstable recovery windows.
- Combining manual transition mode with aggressive retry loops that can amplify load spikes.

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

## Benchmarks

Benchmark suite location: `resilience/benchmark_test.go`

Run all benchmarks:

```bash
go test -bench=. -benchmem ./resilience
```

Run a focused benchmark:

```bash
go test -bench=BenchmarkDecorator_Call_FullChain -benchmem ./resilience
```

For deterministic PR-to-PR comparison, use Make targets:

```bash
make bench-core
```

```bash
make bench-contention
```

```bash
make bench
```

You can override benchmark knobs:

```bash
BENCH_COUNT=20 BENCH_TIME=500ms make bench-core
```

## Fuzzing

Fuzz target location: `resilience/fuzz_test.go`

Run fuzzing for predicate interaction invariants:

```bash
go test -fuzz=FuzzCircuitBreaker_ErrorPredicateInteractions -fuzztime=10s ./resilience
```
