.PHONY: test eval e2e perf report report-perf demo chaos api worker mcp reconcile lint load smoke \
	up down docker-reset logs compose-smoke stream-up stream-down stream-smoke stream-logs

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
	PORT=$(PORT) docker compose up -d --build --wait

down:
	docker compose down

docker-reset:
	docker compose down -v

logs:
	docker compose logs -f

compose-smoke:
	bash tools/compose_smoke.sh

# Kafka + Flink local runtime (infra/streaming). Kafka is the queue in this mode;
# the raw log remains the durable source of truth.
stream-up:
	docker compose -f infra/streaming/docker-compose.yml up -d --build

stream-down:
	docker compose -f infra/streaming/docker-compose.yml down -v --remove-orphans

stream-smoke:
	bash tools/kafka_smoke.sh

stream-logs:
	docker compose -f infra/streaming/docker-compose.yml logs -f
