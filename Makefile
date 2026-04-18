.PHONY: demo-up demo-down demo-run-example demo-logs demo-restart bench bench-core bench-contention

demo-up:
	docker compose up -d --build

demo-down:
	docker compose down -v

demo-run-example:
	docker compose up --build otel-env-example

demo-logs:
	docker compose logs -f --tail=200 otel-env-example otel-collector prometheus grafana

demo-restart:
	docker compose restart otel-env-example otel-collector prometheus grafana

# Deterministic benchmark suite (fixed count and benchtime for easier PR comparison)
BENCH_PKG ?= ./resilience
BENCH_COUNT ?= 10
BENCH_TIME ?= 200ms

bench:
	go test $(BENCH_PKG) -run '^$$' -bench . -benchmem -count $(BENCH_COUNT) -benchtime $(BENCH_TIME)

bench-core:
	go test $(BENCH_PKG) -run '^$$' -bench 'BenchmarkSlidingWindow|BenchmarkDecorator_Call' -benchmem -count $(BENCH_COUNT) -benchtime $(BENCH_TIME)

bench-contention:
	go test $(BENCH_PKG) -run '^$$' -bench 'Benchmark.*ContentionParallel' -benchmem -count $(BENCH_COUNT) -benchtime $(BENCH_TIME)
