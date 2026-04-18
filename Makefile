.PHONY: demo-up demo-down demo-run-example demo-logs demo-restart

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
