.PHONY: test e2e perf report report-perf demo chaos api worker mcp reconcile lint load smoke \
	up down docker-reset logs compose-smoke

# the package lives in src/ and is not pip-installed
export PYTHONPATH := $(CURDIR)/src$(if $(PYTHONPATH),:$(PYTHONPATH))
PORT ?= 8000

test:
	pytest -q --cov --cov-report=term-missing --cov-fail-under=100

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
	ruff check src tests || true

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
