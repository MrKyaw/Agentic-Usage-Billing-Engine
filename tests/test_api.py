import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
os.environ.setdefault("BILLING_DB_PATH", "/tmp/billing-test-api.db")

from fastapi.testclient import TestClient  # noqa: E402

import billing_engine.store as store_mod  # noqa: E402

TMP = Path("/tmp/billing-api-test")
TMP.mkdir(exist_ok=True)


def _client(tmp_path):
    store_mod.reset_store_cache()
    os.environ["BILLING_DB_PATH"] = str(tmp_path / "api.db")
    os.environ["BILLING_RAW_LOG_DIR"] = str(tmp_path / "raw")
    from billing_engine import api as api_mod
    import importlib
    importlib.reload(api_mod)
    return TestClient(api_mod.app)


H = {"X-API-Key": "dev-key-123"}


def test_auth_required(tmp_path):
    c = _client(tmp_path)
    r = c.get("/v1/customers/acme/usage?period=2026-10")
    assert r.status_code == 401


def test_ingest_query_flow(tmp_path):
    c = _client(tmp_path)
    r = c.post("/v1/usage-events?sync=true", headers=H, json={
        "event_id": "api-1", "customer_id": "acme", "product_id": "design-suite",
        "metric": "cloud_credits", "quantity": 10,
        "timestamp": "2026-10-09T10:00:00Z"})
    assert r.status_code == 202
    # duplicate → still 202, no double count
    r = c.post("/v1/usage-events?sync=true", headers=H, json={
        "event_id": "api-1", "customer_id": "acme", "product_id": "design-suite",
        "metric": "cloud_credits", "quantity": 10,
        "timestamp": "2026-10-09T10:00:00Z"})
    assert r.status_code == 202
    u = c.get("/v1/customers/acme/usage?period=2026-10", headers=H).json()
    assert u["lines"][0]["total_quantity"] == 10
    b = c.get("/v1/customers/acme/bill?period=2026-10", headers=H).json()
    assert b["total_amount"] == 1.0


def test_reject_malformed(tmp_path):
    c = _client(tmp_path)
    r = c.post("/v1/usage-events", headers=H, json={"event_id": "x"})
    assert r.status_code == 400


def test_batch_limit(tmp_path):
    c = _client(tmp_path)
    r = c.post("/v1/usage-events:batch", headers=H,
               json={"events": [{"event_id": f"b-{i}", "customer_id": "c",
                                 "product_id": "p", "metric": "cloud_credits",
                                 "quantity": 1,
                                 "timestamp": "2026-10-09T10:00:00Z"} for i in range(101)]})
    assert r.status_code == 400


def test_ops_metrics(tmp_path):
    c = _client(tmp_path)
    r = c.get("/v1/ops/metrics", headers=H)
    assert r.status_code == 200 and "counters" in r.json()
