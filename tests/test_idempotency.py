"""PRD §9: 10k events + 10% duplicates → aggregate equals expected exactly."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from billing_engine.ingest import ingest_one  # noqa: E402
from billing_engine.processor import drain  # noqa: E402
from billing_engine.query import get_usage  # noqa: E402
from billing_engine.store import Store  # noqa: E402

import pytest  # noqa: E402

pytestmark = pytest.mark.req("FR2", "NFR-CORRECTNESS")


def _store(tmp_path):
    return Store(tmp_path / "b.db", tmp_path / "raw")


def _ev(i, cust="acme"):
    return {"event_id": f"evt-{i:06d}", "customer_id": cust, "product_id": "design-suite",
            "metric": "cloud_credits", "quantity": 2.0,
            "timestamp": "2026-10-09T10:00:00Z"}


def test_duplicate_safety_small(tmp_path):
    s = _store(tmp_path)
    for i in range(200):
        ingest_one(s, _ev(i))
    drain(s)
    # redeliver 10% + full replay (at-least-once transport)
    for i in range(0, 200, 10):
        ingest_one(s, _ev(i))
    for i in range(200):
        ingest_one(s, _ev(i))
    r = drain(s)
    assert r["duplicate"] == 220 and r["accepted"] == 0
    u = get_usage(s, "acme", "2026-10")
    assert u["lines"][0]["total_quantity"] == 400.0
    assert s.events_for("acme", "2026-10").__len__() == 200


def test_duplicate_safety_10k(tmp_path):
    s = _store(tmp_path)
    N = 10_000
    for i in range(N):
        ingest_one(s, _ev(i))
    drain(s, batch=500)
    import random
    rng = random.Random(42)
    dupes = rng.sample(range(N), N // 10)
    for i in dupes:
        ingest_one(s, _ev(i))
    r = drain(s, batch=500)
    assert r["duplicate"] == len(dupes)
    u = get_usage(s, "acme", "2026-10")
    assert u["lines"][0]["total_quantity"] == N * 2.0
