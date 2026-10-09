.PHONY: test demo api worker mcp reconcile lint load smoke

test:
	pytest -q

demo:
	bash tools/demo.sh

smoke:
	bash tools/smoke.sh

api:
	uvicorn billing_engine.api:app --host 0.0.0.0 --port 8000

worker:
	python -m billing_engine.worker

mcp:
	python -m billing_engine.mcp_server

reconcile:
	python -m billing_engine.reconcile --period $$(date +%Y-%m)

lint:
	ruff check src tests || true

load:
	locust -f loadtest/locustfile.py --headless -u 50 -r 10 -t 60s --host http://localhost:8000
