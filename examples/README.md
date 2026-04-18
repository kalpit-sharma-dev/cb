# Examples

This folder contains runnable examples for each resilience component and common integration patterns.

| Example | Run command | Demonstrates | Expected behavior |
|---|---|---|---|
| `mux` | `go run ./examples/mux` | Gorilla Mux middleware with circuit breaker + bulkhead + rate limiter | Exposes `/health` and `/demo`; `?fail=1` drives 5xx behavior |
| `gin` | `go run ./examples/gin` | Gin middleware with circuit breaker + bulkhead + rate limiter | Exposes `/health` and `/demo`; `?slow=1` simulates slow calls |
| `otel-env` | `go run ./examples/otel-env` | Full env-driven config and OpenTelemetry bridge | Emits resilience metrics/events for observability stack |
| `manual-half-open` | `go run ./examples/manual-half-open` | Manual `OPEN -> HALF_OPEN` transition flow | Breaker blocks while OPEN until `TransitionToHalfOpen()` is called |
| `retry` | `go run ./examples/retry` | Retry predicate + exponential backoff | Logs retry attempts and final success/failure |
| `bulkhead` | `go run ./examples/bulkhead` | Concurrency limiting with semaphore bulkhead | Some parallel calls are rejected when capacity is saturated |
| `rate-limiter` | `go run ./examples/rate-limiter` | Token bucket limiting and timeout behavior | Calls are allowed/denied based on configured rate |
| `decorator` | `go run ./examples/decorator` | Full decorator chain composition | Shows chaining of rate limiter, bulkhead, circuit breaker, retry |
| `outbound-rest` | `go run ./examples/outbound-rest` | Wrapping outbound HTTP client calls | Simulates resilient remote API calls with retry/circuit handling |
| `db-call` | `go run ./examples/db-call` | Wrapping repository/DB-style operations | Simulates resilient query-style calls with retry and bulkhead |

## Notes

- Middleware examples intentionally avoid inbound handler retries to prevent duplicated side effects after response writes.
- You can run all examples as build checks with:

```bash
go build ./examples/...
```
