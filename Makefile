.PHONY: test eval e2e perf report report-perf demo chaos api worker mcp reconcile lint load smoke \
	up up-streaming down docker-reset logs compose-smoke stream-up stream-down stream-smoke stream-logs

# the package lives in src/ and is not pip-installed
export PYTHONPATH := $(CURDIR)/src$(if $(PYTHONPATH),:$(PYTHONPATH))
PORT ?= 8000

test:
	pytest -q --cov --cov-report=term-missing --cov-fail-under=100

eval:
	python tools/eval_suite.py

e2e:
	pytest -q -m e2e

perf:
	pytest -q -m perf

report:
	python tools/test_report.py

report-perf:
	python tools/test_report.py --perf

chaos:
	bash tools/chaos.sh

demo:
	bash tools/demo.sh

smoke:
	bash tools/smoke.sh

api:
	uvicorn billing_engine.api:app --host 0.0.0.0 --port $(PORT)

worker:
	python -m billing_engine.worker

mcp:
	python -m billing_engine.mcp_server

reconcile:
	python -m billing_engine.reconcile --period $$(date +%Y-%m)

lint:
	ruff check src tests tools

load:
	locust -f loadtest/locustfile.py --headless -u 50 -r 10 -t 60s --host http://localhost:$(PORT)

# exports dir must exist first: Docker Desktop backs a missing bind source with a tmpfs
up:
	mkdir -p $${DATA_DIR:-data}/exports
	PORT=$(PORT) BILLING_KAFKA_BOOTSTRAP= docker compose up -d --build --wait

# The whole solution in one project: monolith (API + worker) + Kafka + Flink.
# BILLING_KAFKA_BOOTSTRAP is the single switch that turns the SQLite inbox into
# the Kafka queue for the API and the worker; the raw log stays the source of truth.
up-streaming:
	mkdir -p $${DATA_DIR:-data}/exports
	PORT=$(PORT) BILLING_KAFKA_BOOTSTRAP=$${BILLING_KAFKA_BOOTSTRAP:-kafka:9092} \
		docker compose --profile streaming up -d --build
	@for i in $$(seq 1 60); do \
		if python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:$(PORT)/health', timeout=2)" >/dev/null 2>&1; then \
			echo "API healthy on http://localhost:$(PORT) (Flink UI on http://localhost:$${FLINK_PORT:-8081})"; exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "API did not become healthy; check 'make stream-logs'"; exit 1

down:
	docker compose --profile streaming down

docker-reset:
	docker compose --profile streaming down -v

logs:
	docker compose --profile streaming logs -f

compose-smoke:
	bash tools/compose_smoke.sh

# Aliases for the streaming tier of the same stack.
stream-up: up-streaming

stream-down:
	docker compose --profile streaming down -v --remove-orphans

stream-smoke:
	bash tools/kafka_smoke.sh

stream-logs:
	docker compose --profile streaming logs -f
