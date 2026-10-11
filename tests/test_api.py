from pathlib import Path

import pytest
from conftest import make_event
from fastapi.testclient import TestClient

from billing_engine.api import app

pytestmark = pytest.mark.req("FR1", "FR4")

H = {"X-API-Key": "dev-key-123"}


@pytest.fixture()
def c(default_store, tmp_path, monkeypatch):
    monkeypatch.setenv("BILLING_EXPORT_DIR", str(tmp_path / "exports"))
    return TestClient(app)


def test_health(c):
    r = c.get("/health")
    assert r.status_code == 200 and r.json()["ok"] is True


@pytest.mark.req("NFR-SECURITY")
def test_auth_required(c):
    for headers in ({}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}):
        r = c.get("/v1/customers/acme/usage?period=2026-10", headers=headers)
        assert r.status_code == 401 and r.json() == {"detail": "invalid API key"}
    assert c.get("/v1/customers/acme/usage?period=2026-10&api_key=dev-key-123"
                 ).status_code == 401  # never accepted in the URL


@pytest.mark.req("NFR-SECURITY")
def test_openapi_declares_api_key_scheme(c):
    """Drives the /docs "Authorize" button: one key entry for every /v1 route."""
    spec = c.get("/openapi.json").json()
    schemes = spec["components"]["securitySchemes"]
    assert schemes == {"APIKeyHeader": {"type": "apiKey", "in": "header", "name": "X-API-Key"}}
    for path, ops in spec["paths"].items():
        for op in ops.values():
            secured = op.get("security") == [{"APIKeyHeader": []}]
            assert secured == path.startswith("/v1/"), path


def test_ingest_query_flow(c):
    ev = make_event("api-1", quantity=10)
    assert c.post("/v1/usage-events?sync=true", headers=H, json=ev).status_code == 202
    # duplicate → still 202, no double count
    assert c.post("/v1/usage-events?sync=true", headers=H, json=ev).status_code == 202
    u = c.get("/v1/customers/acme/usage?period=2026-10", headers=H).json()
    assert u["lines"][0]["total_quantity"] == 10
    b = c.get("/v1/customers/acme/bill?period=2026-10", headers=H).json()
    assert b["total_amount"] == 1.0 and b["status"] == "open"


def test_async_ingest_leaves_event_pending(c, default_store):
    r = c.post("/v1/usage-events", headers=H, json=make_event("async-1"))
    assert r.status_code == 202 and r.json() == {"event_id": "async-1", "status": "accepted"}
    assert default_store.pending_count() == 1
    m = c.get("/v1/ops/metrics", headers=H).json()
    assert m["queue"]["pending"] == 1


def test_reject_malformed(c):
    r = c.post("/v1/usage-events", headers=H, json={"event_id": "x"})
    assert r.status_code == 400


def test_batch_limit(c):
    r = c.post("/v1/usage-events:batch", headers=H,
               json={"events": [make_event(f"b-{i}") for i in range(101)]})
    assert r.status_code == 400


def test_batch_mixed_valid_and_invalid(c, default_store):
    r = c.post("/v1/usage-events:batch?sync=true", headers=H, json={"events": [
        make_event("mb-1", quantity=3), make_event("mb-2", quantity=4),
        {"event_id": "mb-bad", "quantity": -1}]})
    assert r.status_code == 202
    body = r.json()
    assert body["accepted_count"] == 2 and body["rejected_count"] == 1
    assert default_store.pending_count() == 0
    u = c.get("/v1/customers/acme/usage?period=2026-10", headers=H).json()
    assert u["lines"][0]["total_quantity"] == 7


def test_batch_async(c, default_store):
    r = c.post("/v1/usage-events:batch", headers=H, json={"events": [make_event("ba-1")]})
    assert r.status_code == 202 and default_store.pending_count() == 1


def test_batch_ingest_rejected_maps_to_400(c, monkeypatch):
    from billing_engine import api as api_mod
    from billing_engine.ingest import IngestRejected

    def reject(_store, _events):
        raise IngestRejected("nope")
    monkeypatch.setattr(api_mod, "ingest_batch", reject)
    r = c.post("/v1/usage-events:batch", headers=H, json={"events": [make_event("x")]})
    assert r.status_code == 400 and r.json()["detail"] == "nope"


def test_close_period_endpoint(c, tmp_path):
    c.post("/v1/usage-events?sync=true", headers=H, json=make_event("cl-1", quantity=100))
    r = c.post("/v1/customers/acme/close?period=2026-10", headers=H)
    assert r.status_code == 200
    out = r.json()
    assert out["bill"]["status"] == "closed" and out["bill"]["total_amount"] == 10.0
    assert Path(out["csv"]).parent == tmp_path / "exports" and Path(out["csv"]).exists()
    b = c.get("/v1/customers/acme/bill?period=2026-10", headers=H).json()
    assert b["status"] == "closed" and b["closed_at"] is not None


def test_ops_reconcile(c):
    c.post("/v1/usage-events?sync=true", headers=H, json=make_event("rc-1"))
    r = c.get("/v1/ops/reconcile?customer=acme&period=2026-10", headers=H).json()
    assert r["match"] and r["checked"] == 1
    assert c.get("/v1/ops/reconcile", headers=H).json()["match"]


@pytest.mark.parametrize("period", ["2026-1", "Oct-2026", "2026/10"])
def test_invalid_period_rejected(c, period):
    assert c.get(f"/v1/customers/acme/bill?period={period}", headers=H).status_code == 422
    assert c.get(f"/v1/customers/acme/usage?period={period}", headers=H).status_code == 422


def test_missing_period_rejected(c):
    assert c.get("/v1/customers/acme/usage", headers=H).status_code == 422


def test_ops_metrics(c):
    r = c.get("/v1/ops/metrics", headers=H)
    assert r.status_code == 200
    body = r.json()
    assert "counters" in body and body["queue"]["dlq"] == 0 and body["alarms"] == []


def test_ui_console_self_contained(c):
    """The /ui test console must load with zero external dependencies
    (it exists precisely for networks where the /docs CDN is blocked)."""
    r = c.get("/ui")
    assert r.status_code == 200
    html = r.text
    assert "usage-events" in html and "reconcile" in html
    assert "https://" not in html
    assert "X-API-Key" in html
