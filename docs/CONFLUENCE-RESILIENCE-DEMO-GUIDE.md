# Resilience Library + Observability Demo (Confluence Draft)

## Document purpose

This document explains the end-to-end resilience solution in this repository:

- why it exists
- what the common library provides
- how to use it in applications
- how observability is wired
- what Grafana monitors
- how to run the demo locally
- what developers should do to adopt it safely

This page is intended for engineers onboarding to the project and for teams evaluating resilience controls in production services.

---

## 1) Executive summary

This repo provides a production-grade Go resilience package inspired by Resilience4j, with:

- Circuit Breaker
- Retry
- Bulkhead
- Rate Limiter
- Decorator composition API
- HTTP middleware (Gin + Gorilla Mux)
- OpenTelemetry metrics bridge
- Full demo stack (OTel Collector + Prometheus + Grafana + alerting)

### Business/problem value

The library helps teams:

- reduce cascading failures during downstream outages
- cap latency amplification from slow dependencies
- control concurrency and request rates to protect services
- standardize resilience behavior across services
- monitor resilience behavior with actionable dashboards and alerts

---

## 2) What problem this common library solves

Distributed services commonly fail due to:

- intermittent timeouts and 5xx responses
- dependency brownouts (slow responses)
- retry storms and thread/goroutine exhaustion
- traffic bursts beyond safe capacity
- lack of visibility into resilience behavior

This library addresses those risks by combining defensive patterns:

- **Circuit Breaker**: stop calling unhealthy dependencies temporarily
- **Retry**: recover transient errors with controlled retries/backoff
- **Bulkhead**: cap concurrent in-flight work to protect resources
- **Rate Limiter**: smooth and enforce request throughput

All components are designed to be composable and observable.

---

## 3) What is inside the common library (`/resilience`)

### 3.1 Circuit Breaker

Implemented with `github.com/sony/gobreaker` as state-machine core, with Resilience4j-like behavior layered on top.

Core features:

- count-based and time-based sliding windows
- failure-rate threshold tripping
- slow-call-rate threshold tripping
- configurable slow-call duration threshold
- half-open probe limit
- open-state wait duration
- automatic open -> half-open transition
- manual open -> half-open transition via `TransitionToHalfOpen()`
- ignore/record error predicates
- event publishing (success/failure/slow/ignored/state-change)
- metrics snapshot API

### 3.2 Retry

Features:

- max attempts
- wait duration
- exponential backoff with max interval
- retry predicate (`RetryOn`)
- retry callback (`OnRetry`)
- runtime counters and observer hooks

### 3.3 Bulkhead

Semaphore-based concurrency isolation:

- max concurrent calls
- max wait duration to acquire permit
- explicit rejected-call behavior
- runtime counters and observer hooks

### 3.4 Rate Limiter

Token-bucket style limiter (`x/time/rate`) with:

- tokens per period
- refresh period
- timeout duration for permit acquisition
- allow/deny counters and observer hooks

### 3.5 Decorator API

Fluent composition for consistent wrapping order:

```go
result, err := resilience.Decorate(fn).
  WithRateLimiter(rl).
  WithBulkhead(bh).
  WithCircuitBreaker(cb).
  WithRetry(r).
  Call(ctx)
```

### 3.6 HTTP middleware

- `GorillaMuxMiddleware`
- `GinMiddleware`

Important behavior:

- middleware is designed for safe inbound protection
- inbound handler retries are intentionally not applied to avoid duplicate side-effects after partial response writes

### 3.7 OpenTelemetry bridge

`OTelBridge` exports component events and snapshots to OpenTelemetry metrics instruments.

---

## 4) How observability works

### 4.1 Telemetry flow

`otel-env` app -> OTel HTTP metrics exporter -> OTel Collector -> Prometheus scrape -> Grafana dashboards + alerts

### 4.2 Components and files

- **OTel Collector config**: `observability/otelcol/config.yaml`
- **Prometheus config**: `observability/prometheus.yml`
- **Grafana datasource/dashboard/alert provisioning**:
  - `observability/grafana/provisioning/datasources/...`
  - `observability/grafana/provisioning/dashboards/...`
  - `observability/grafana/provisioning/alerting/...`
- **Dashboard JSON**: `observability/grafana/dashboards/resilience-overview.json`

### 4.3 Metrics exposed by bridge

Representative metric families (prefix configurable; demo uses `resilience.demo`):

- `*.cb.failure_rate`
- `*.cb.slow_call_rate`
- `*.cb.calls_total`
- `*.cb.state_changes_total`
- `*.retry.events_total`
- `*.retry.attempts_total`
- `*.bulkhead.events_total`
- `*.bulkhead.available_concurrency`
- `*.rate_limiter.events_total`
- `*.rate_limiter.allowed_total`

### 4.4 Why this observability matters

It shows:

- whether dependencies are degrading (failure/slow-call trends)
- whether retry behavior is stabilizing or amplifying failures
- whether bulkheads are saturating/rejecting
- whether rate limiting is denying excess traffic
- whether breaker state transitions are healthy and expected

---

## 5) Grafana dashboard: what it shows

Dashboard: **Resilience Overview** (`uid: resilience-overview`)

Panels:

1. **CB Failure Rate** (`resilience_demo_cb_failure_rate`)
2. **CB Slow Call Rate** (`resilience_demo_cb_slow_call_rate`)
3. **Retry Events** (`rate(resilience_demo_retry_events_total[1m])`)
4. **Bulkhead Events** (`rate(resilience_demo_bulkhead_events_total[1m])`)
5. **Rate Limiter Events** (`rate(resilience_demo_rate_limiter_events_total[1m])`)

How to interpret:

- rising CB failure/slow-call rates -> dependency instability
- spike in retry events with failures -> transient or persistent downstream issue
- high bulkhead rejections -> concurrency saturation
- high rate-limiter denied rate -> input traffic above configured safe capacity

---

## 6) Alerting implemented

Configured in: `observability/grafana/provisioning/alerting/resilience-alerts.yaml`

### Alert 1: Resilience High Failure Rate

- query: `max(resilience_demo_cb_failure_rate)`
- condition: `> 70` for `2m`

### Alert 2: Resilience High RateLimiter Denied Rate

- query: `sum(rate(resilience_demo_rate_limiter_events_total{outcome="denied"}[1m]))`
- condition: `> 10` for `2m`

Purpose:

- surface dependency health deterioration early
- detect sustained over-traffic or overly strict limiter config

---

## 7) Repository tour (what developers will find)

### Core package

- `resilience/` -> all reusable resilience primitives + tests + benchmarks + fuzz tests

### Examples

- `examples/mux` -> Gorilla Mux middleware
- `examples/gin` -> Gin middleware
- `examples/otel-env` -> env-driven + OTel end-to-end demo producer
- `examples/manual-half-open` -> manual transition flow
- `examples/retry` -> retry behavior
- `examples/bulkhead` -> concurrency limiting
- `examples/rate-limiter` -> throughput limiting
- `examples/decorator` -> full chain composition
- `examples/outbound-rest` -> external API call pattern
- `examples/db-call` -> repository/DB-call pattern
- `examples/README.md` -> per-example quick guide

### Observability assets

- `observability/` -> collector/prometheus/grafana configs, dashboard, alerts

### Environment/demo tooling

- `docker-compose.yml` -> full stack orchestration
- `Makefile` -> `demo-up/down/logs/restart`, benchmark helpers
- `scripts/load.sh` -> traffic generation profiles

---

## 8) Demo setup runbook

## 8.1 Prerequisites

- Docker + Docker Compose
- Go 1.25.3+

## 8.2 Start full demo stack

```bash
make demo-up
```

or

```bash
docker compose up -d --build
```

## 8.3 Access tools

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)
- OTel Collector OTLP HTTP endpoint: `http://localhost:4318`

## 8.4 Drive load

```bash
BASE_URL=http://localhost:8082 PROFILE=mixed DURATION_SECONDS=120 CONCURRENCY=40 ./scripts/load.sh
```

Profiles:

- `happy`
- `failure`
- `slow`
- `mixed`

## 8.5 Stop demo

```bash
make demo-down
```

---

## 9) How developers should use this library

Recommended rollout pattern:

1. start with outbound dependency calls (HTTP/DB) not everything at once
2. configure conservative thresholds
3. enable OTel bridge and dashboards first
4. tune based on observed real traffic
5. expand to more critical dependency paths

Suggested component choice by need:

- need to protect against hard failures -> Circuit Breaker
- need transient recovery -> Retry
- need to cap in-flight work -> Bulkhead
- need throughput control -> Rate Limiter
- need all of the above -> Decorator chain

---

## 10) Manual half-open mode guidance

When to use:

- when recovery probes must be externally controlled (maintenance windows, failover checks)

Recommended:

- keep breaker OPEN during confirmed outage
- call `TransitionToHalfOpen()` only after external health signal
- allow small probe count (`WithPermittedNumberOfCallsInHalfOpenState`)
- let breaker auto-close on successful probes

Avoid:

- triggering manual transition on every request
- large probe count during unstable dependency recovery

---

## 11) Quality and validation in repo

- unit/integration tests: `go test ./...`
- benchmarks: `resilience/benchmark_test.go`
- fuzz tests: `resilience/fuzz_test.go`
- deterministic benchmark targets:
  - `make bench-core`
  - `make bench-contention`
  - `make bench`

---

## 12) Troubleshooting quick notes

### Symptom: `connection refused` to `otel-collector:4318`

Check:

- collector container is running
- OTLP receiver binds all interfaces in collector config:
  - `endpoint: "0.0.0.0:4318"` (HTTP)
  - `endpoint: "0.0.0.0:4317"` (gRPC)

### Symptom: dashboard empty

Check:

- `otel-env-example` is running and exporting
- Prometheus can scrape collector exporter
- metric prefix in app config matches dashboard queries (`resilience.demo`)

### Symptom: no alerts firing

Check:

- traffic profile is actually crossing configured thresholds
- alert datasource UID and dashboard UID are correct (already provisioned in repo)

---

## 13) Suggested Confluence page metadata

- **Owner:** Platform / Reliability Team
- **Audience:** Backend Engineers, SRE, Service Owners
- **Status:** Production-ready reference implementation
- **Last Updated:** <fill during publish>
- **Related repos/pages:** service integration runbooks, SLO docs, incident playbooks

