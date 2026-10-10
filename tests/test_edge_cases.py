"""Boundary and negative cases: numeric limits, float precision, identifiers,
period boundaries, and HTTP-level malformed / hostile input."""
from pathlib import Path

import pytest
from conftest import make_event
from fastapi.testclient import TestClient
from pydantic import ValidationError

from billing_engine.api import app
from billing_engine.ingest import ingest_one
from billing_engine.models import coerce_event
from billing_engine.period_close import close_period
from billing_engine.processor import drain
from billing_engine.query import get_bill, get_usage
from billing_engine.reconcile import reconcile

H = {"X-API-Key": "dev-key-123"}
FR1 = pytest.mark.req("FR1")


# ------------------------------------------------------------------ quantity

@FR1
@pytest.mark.parametrize("qty,expected", [(1e-9, 1e-9), (1e9, 1e9), ("5", 5.0), ("1e3", 1000.0),
                                          (7, 7.0)])
def test_quantity_accepted_bounds_and_numeric_strings(qty, expected):
    assert coerce_event(make_event("q", quantity=qty)).quantity == expected


@FR1
@pytest.mark.parametrize("qty", [0, -1e-9, 1e9 + 1e-6, float("nan"), float("inf"),
                                 float("-inf"), "abc", "", None, True, False, [1], {"v": 1}])
def test_quantity_rejected(qty):
    with pytest.raises(ValidationError):
        coerce_event(make_event("q", quantity=qty))


@pytest.mark.req("FR3", "FR4")
def test_float_precision_ten_tenths_bill_to_exact_cent(store):
    for i in range(10):
        ingest_one(store, make_event(f"p-{i}", quantity=0.1))
    drain(store)
    line = get_usage(store, "acme", "2026-10")["lines"][0]
    assert line["total_quantity"] == pytest.approx(1.0, abs=1e-12)
    assert get_bill(store, "acme", "2026-10")["total_amount"] == 0.1
    assert reconcile(store)["match"]


@pytest.mark.req("FR3", "FR4")
def test_max_quantity_events_rate_without_overflow(store):
    for i in range(3):
        ingest_one(store, make_event(f"big-{i}", quantity=1e9))
    drain(store)
    assert get_bill(store, "acme", "2026-10")["total_amount"] == 3e8  # 3e9 * $0.10


# --------------------------------------------------------------- identifiers

@FR1
@pytest.mark.parametrize("eid", ["é🚀-ünïcode", "x" * 128, "a/b\\c", "id with spaces",
                                 "550e8400-e29b-41d4-a716-446655440000"])
def test_unusual_event_ids_round_trip(store, eid):
    ingest_one(store, make_event(eid))
    drain(store)
    assert store.get_event(eid.strip())["quantity"] == 10
    assert reconcile(store)["match"]


@FR1
@pytest.mark.parametrize("field", ["event_id", "customer_id", "product_id"])
def test_identifier_too_long_rejected(field):
    with pytest.raises(ValidationError):
        coerce_event({**make_event("x"), field: "x" * 129})


@FR1
@pytest.mark.parametrize("field", ["event_id", "customer_id", "product_id", "metric"])
def test_blank_identifiers_rejected(field):
    with pytest.raises(ValidationError):
        coerce_event({**make_event("x"), field: "   "})


@FR1
def test_identifiers_are_trimmed_so_padding_cannot_split_a_customer(store):
    ingest_one(store, make_event("pad-1", customer_id="acme"))
    ingest_one(store, make_event("pad-2", customer_id="  acme ", metric=" cloud_credits"))
    drain(store)
    assert get_usage(store, "acme", "2026-10")["lines"][0]["event_count"] == 2
    assert get_usage(store, "  acme ", "2026-10")["lines"] == []


@FR1
@pytest.mark.parametrize("field,value", [("customer_id", 5), ("event_id", None),
                                         ("metadata", "not-a-dict")])
def test_wrong_types_rejected(field, value):
    with pytest.raises(ValidationError):
        coerce_event({**make_event("x"), field: value})


@pytest.mark.req("FR2", "FR5")
def test_event_id_collision_across_customers_is_flagged_by_reconcile(store):
    """event_id is a global idempotency key (ADR-001): the second customer's event
    is deduped, and reconciliation surfaces the collision instead of hiding it."""
    ingest_one(store, make_event("shared", customer_id="acme"))
    ingest_one(store, make_event("shared", customer_id="globex"))
    r = drain(store)
    assert r["accepted"] == 1 and r["duplicate"] == 1
    rec = reconcile(store)
    assert not rec["match"]
    assert rec["mismatches"][0]["customer_id"] == "globex"


# ----------------------------------------------------------- period boundaries

@pytest.mark.req("FR2", "FR4")
@pytest.mark.parametrize("ts,period", [
    ("2026-12-31T23:59:59.999999Z", "2026-12"),
    ("2027-01-01T00:00:00Z", "2027-01"),
    ("2026-12-31T20:00:00-05:00", "2027-01"),
    ("2027-01-01T03:00:00+05:00", "2026-12"),
    ("2028-02-29T12:00:00Z", "2028-02"),
    ("2026-10-09", "2026-10"),
])
def test_period_boundaries(store, ts, period):
    ingest_one(store, make_event("b", timestamp=ts))
    drain(store)
    assert store.get_event("b")["period"] == period
    assert get_usage(store, "acme", period)["lines"][0]["event_count"] == 1
    assert reconcile(store)["match"]


@FR1
@pytest.mark.parametrize("ts", ["2026-02-30T00:00:00Z", "2027-02-29T00:00:00Z", "not-a-date",
                                1760000000, "", "2026-13-01T00:00:00Z"])
def test_invalid_timestamps_rejected(ts):
    with pytest.raises(ValidationError):
        coerce_event(make_event("t", timestamp=ts))


@pytest.mark.req("FR4", "FR5")
def test_empty_store_queries(store):
    assert get_usage(store, "nobody", "2026-10")["lines"] == []
    b = get_bill(store, "nobody", "2026-10")
    assert b["total_amount"] == 0.0 and isinstance(b["total_amount"], float)
    assert reconcile(store) == {"checked": 0, "mismatches": [], "match": True,
                                "raw_events_scanned": 0, "dlq_excluded": 0,
                                "pending_excluded": 0}


@pytest.mark.req("FR4")
def test_close_empty_period_and_double_close(store, tmp_path):
    out1 = close_period(store, "nobody", "2026-10", export_dir=tmp_path)
    assert out1["bill"]["total_amount"] == 0.0 and out1["bill"]["lines"] == []
    rows = Path(out1["csv"]).read_text().strip().splitlines()
    assert len(rows) == 2 and rows[1].startswith("nobody,2026-10,TOTAL")
    out2 = close_period(store, "nobody", "2026-10", export_dir=tmp_path)
    assert out2["bill"]["closed_at"] >= out1["bill"]["closed_at"]
    assert get_bill(store, "nobody", "2026-10")["status"] == "closed"


@pytest.mark.req("FR4", "NFR-SECURITY")
@pytest.mark.parametrize("cid", ["..\\..\\pwn", "...", "a b/c", "café", "x:y*z?"])
def test_close_csv_filename_is_sanitized_and_inside_export_dir(store, tmp_path, cid):
    out = close_period(store, cid, "2026-10", export_dir=tmp_path / "exports")
    p = Path(out["csv"])
    assert p.parent == tmp_path / "exports" and p.exists()
    assert not set(p.name) & set("\\/:*?\" <>|")
    assert p.name.endswith("-2026-10.csv")
    assert p.read_text().splitlines()[-1].startswith(f"{cid},2026-10,TOTAL")


# ------------------------------------------------------------------- HTTP

@pytest.fixture()
def c(default_store, tmp_path, monkeypatch):
    monkeypatch.setenv("BILLING_EXPORT_DIR", str(tmp_path / "exports"))
    return TestClient(app)


@FR1
@pytest.mark.parametrize("kwargs", [
    {"content": b"{not json", "headers": {**H, "Content-Type": "application/json"}},
    {"content": b'{"a": 1}', "headers": {**H, "Content-Type": "text/plain"}},
    {"json": [make_event("arr")], "headers": H},
    {"json": "a string", "headers": H},
])
def test_malformed_bodies_rejected_4xx(c, kwargs):
    r = c.post("/v1/usage-events", **kwargs)
    assert 400 <= r.status_code < 500


@FR1
def test_bool_quantity_rejected_over_http(c):
    assert c.post("/v1/usage-events", headers=H,
                  json=make_event("bq", quantity=True)).status_code == 400


@FR1
@pytest.mark.parametrize("override", [
    {"event_id": "   "}, {"customer_id": "\t"}, {"metric": " "},
    {"timestamp": "not-a-date"}, {"timestamp": 1760000000}, {"quantity": False},
])
def test_custom_validator_rejections_are_400_not_500(c, override):
    """Custom validators put the raised exception in errors()[*].ctx; the
    rejection log must not choke on it and turn a client error into a 500."""
    body = {**make_event("cv"), **override}
    r = c.post("/v1/usage-events", headers=H, json=body)
    assert r.status_code == 400
    r = c.post("/v1/usage-events:batch", headers=H,
               json={"events": [body, make_event("cv-ok")]})
    assert r.status_code == 202
    assert (r.json()["accepted_count"], r.json()["rejected_count"]) == (1, 1)


@pytest.mark.req("FR6")
def test_jlog_never_raises_on_unserializable_fields(capsys):
    from billing_engine.observability import jlog
    jlog("INFO", "odd", err=ValueError("x"), obj=object())
    assert '"err": "x"' in capsys.readouterr().out


@FR1
def test_empty_batch_rejected(c):
    assert c.post("/v1/usage-events:batch", headers=H, json={"events": []}).status_code == 400


@FR1
def test_batch_of_exactly_100_accepted(c):
    r = c.post("/v1/usage-events:batch?sync=true", headers=H,
               json={"events": [make_event(f"b-{i}") for i in range(100)]})
    assert r.status_code == 202 and r.json()["accepted_count"] == 100


@FR1
def test_batch_with_non_object_items_rejected(c):
    assert c.post("/v1/usage-events:batch", headers=H,
                  json={"events": [1, "x", None]}).status_code == 422


@pytest.mark.req("NFR-SECURITY")
@pytest.mark.parametrize("headers,status", [
    ({}, 401), ({"X-API-Key": ""}, 401), ({"X-API-Key": "DEV-KEY-123"}, 401),
    ({"X-API-Key": "dev-key-123 "}, 401), ({"x-api-key": "dev-key-123"}, 200),
])
def test_api_key_edge_cases(c, headers, status):
    assert c.get("/v1/customers/a/usage?period=2026-10", headers=headers).status_code == status


@pytest.mark.req("NFR-SECURITY")
@pytest.mark.parametrize("method,path", [
    ("post", "/v1/usage-events"), ("post", "/v1/usage-events:batch"),
    ("get", "/v1/customers/a/bill?period=2026-10"), ("post", "/v1/customers/a/close?period=2026-10"),
    ("get", "/v1/ops/metrics"), ("get", "/v1/ops/reconcile"),
])
def test_every_v1_endpoint_requires_key(c, method, path):
    assert getattr(c, method)(path).status_code == 401


@pytest.mark.req("FR4")
@pytest.mark.parametrize("period", ["2026-13", "2026-00", "0000-1", "2026-10-01", " 2026-10"])
def test_invalid_months_rejected(c, period):
    assert c.get("/v1/customers/a/bill", params={"period": period},
                 headers=H).status_code == 422
    assert c.get("/v1/ops/reconcile", params={"period": period},
                 headers=H).status_code == 422


@pytest.mark.req("FR4", "NFR-SECURITY")
def test_encoded_slash_in_customer_path_does_not_route(c):
    assert c.post("/v1/customers/..%2F..%2Fpwn/close?period=2026-10",
                  headers=H).status_code == 404


@pytest.mark.req("FR4")
def test_unicode_customer_over_http(c):
    c.post("/v1/usage-events?sync=true", headers=H, json=make_event("u-1", customer_id="café"))
    r = c.get("/v1/customers/caf%C3%A9/bill", params={"period": "2026-10"}, headers=H).json()
    assert r["customer_id"] == "café" and r["total_amount"] == 1.0


@pytest.mark.req("FR1")
def test_unknown_route_and_method(c):
    assert c.get("/nope").status_code == 404
    assert c.get("/v1/usage-events", headers=H).status_code == 405


# ------------------------------------------------------------------- batch write path

@FR1
def test_batch_of_only_invalid_events_writes_nothing(store):
    from billing_engine.ingest import ingest_batch
    r = ingest_batch(store, [make_event("x", quantity=-1), make_event("y", metric=" ")])
    assert (r["accepted_count"], r["rejected_count"]) == (0, 2)
    assert store.pending_count() == 0 and store.all_raw_payloads() == []


@pytest.mark.req("FR1", "NFR-CORRECTNESS")
def test_batch_is_enqueued_atomically(store, monkeypatch):
    """A failure on the 3rd event rolls back the whole batch: the producer gets
    an error and retries all of it, never a silently half-written batch."""
    from billing_engine.ingest import ingest_batch
    real, calls = store.append_raw_log, []

    def flaky(p):
        calls.append(p["event_id"])
        if len(calls) == 3:
            raise OSError("disk full")
        real(p)

    monkeypatch.setattr(store, "append_raw_log", flaky)
    with pytest.raises(OSError):
        ingest_batch(store, [make_event(f"b{i}") for i in range(5)])
    assert store.pending_count() == 0
