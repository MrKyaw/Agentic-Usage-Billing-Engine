import pytest
from conftest import make_event

from billing_engine import worker
from billing_engine.ingest import ingest_one
from billing_engine.query import get_usage

pytestmark = pytest.mark.req("FR2")


def test_worker_drains_inbox_and_exits_when_idle(default_store):
    for i in range(120):
        ingest_one(default_store, make_event(f"w-{i}", quantity=1))
    ingest_one(default_store, make_event("w-0", quantity=1))  # duplicate
    ingest_one(default_store, make_event("w-bad", metric="nope"))
    worker.main(poll_s=0, batch=50, max_idle_polls=2)
    assert default_store.pending_count() == 0
    assert default_store.dlq_count() == 1
    assert get_usage(default_store, "acme", "2026-10")["lines"][0]["total_quantity"] == 120


def test_worker_exits_immediately_on_empty_inbox(default_store):
    worker.main(poll_s=0, max_idle_polls=1)
    assert default_store.pending_count() == 0
