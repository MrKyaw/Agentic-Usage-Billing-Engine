"""Lambda adapters driven with API Gateway proxy / SQS event shapes."""
import json

import handlers
import pytest
from conftest import make_event

from billing_engine.ingest import _norm

pytestmark = pytest.mark.req("FR1", "FR2", "FR4")


def _api_event(body, path="/v1/usage-events"):
    return {"path": path, "body": json.dumps(body) if body is not None else None}


def test_ingest_single(default_store):
    r = handlers.ingest(_api_event(make_event("h-1")))
    assert r["statusCode"] == 202
    assert json.loads(r["body"]) == {"event_id": "h-1", "status": "accepted"}
    assert r["headers"]["Content-Type"] == "application/json"
    assert default_store.pending_count() == 1


def test_ingest_batch_by_path_and_by_body(default_store):
    r = handlers.ingest(_api_event(make_event("hb-1"), path="/v1/usage-events:batch"))
    assert json.loads(r["body"])["accepted_count"] == 1
    r = handlers.ingest(_api_event({"events": [make_event("hb-2"), make_event("hb-3")]}))
    assert r["statusCode"] == 202 and json.loads(r["body"])["accepted_count"] == 2
    assert default_store.pending_count() == 3


def test_ingest_invalid_returns_400(default_store):
    r = handlers.ingest(_api_event({"event_id": "bad"}))
    assert r["statusCode"] == 400 and "error" in json.loads(r["body"])
    assert handlers.ingest({"path": "/v1/usage-events"})["statusCode"] == 400  # empty body


def _sqs(*payloads):
    return {"Records": [{"messageId": f"m-{i}", "body": json.dumps(_norm(p)),
                         "attributes": {"ApproximateReceiveCount": "1"}}
                        for i, p in enumerate(payloads)]}


def test_processor_handler_accepts_dedupes_and_dlqs(default_store):
    ev = make_event("sq-1", quantity=5)
    out = handlers.processor(_sqs(ev, ev, make_event("sq-bad", metric="nope")))
    assert out == {"accepted": 1, "duplicate": 1, "dlq": 1}
    assert default_store.get_event("sq-1")["quantity"] == 5
    assert default_store.dlq_count() == 1
    assert handlers.processor({}) == {"accepted": 0, "duplicate": 0, "dlq": 0}


def test_query_handler_usage_and_bill(default_store):
    handlers.processor(_sqs(make_event("q-1", quantity=100)))
    ev = {"pathParameters": {"id": "acme"}, "queryStringParameters": {"period": "2026-10"}}
    usage = json.loads(handlers.query({**ev, "path": "/v1/customers/acme/usage"})["body"])
    assert usage["lines"][0]["total_quantity"] == 100
    bill = handlers.query({**ev, "path": "/v1/customers/acme/bill"})
    assert bill["statusCode"] == 200 and json.loads(bill["body"])["total_amount"] == 10.0
    empty = json.loads(handlers.query({"path": "/v1/customers//usage"})["body"])
    assert empty["lines"] == []
