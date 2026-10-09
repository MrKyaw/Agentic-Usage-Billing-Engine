"""Loss safety: kill processor mid-run → zero lost events after recovery."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from billing_engine.ingest import ingest_one  # noqa: E402
from billing_engine.processor import process_batch  # noqa: E402
from billing_engine.query import get_usage  # noqa: E402
from billing_engine.store import Store  # noqa: E402


def test_crash_recovery(tmp_path):
    s = Store(tmp_path / "b.db", tmp_path / "raw")
    N = 500
    for i in range(N):
        ingest_one(s, {"event_id": f"c-{i:05d}", "customer_id": "acme",
                       "product_id": "design-suite", "metric": "cloud_credits",
                       "quantity": 1.0, "timestamp": "2026-10-09T10:00:00Z"})
    # simulate crash: claim but never ack (worker dies). claim twice to simulate
    # visibility timeout redelivery, then ack nothing new — rows stay pending.
    claimed = s.claim_batch(limit=500)
    assert len(claimed) == N
    # "crash": do NOT ack. New worker instance claims after visibility expiry.
    s2 = Store(tmp_path / "b.db", tmp_path / "raw")
    # force visibility expiry by resetting claimed_at
    s2._conn().execute("UPDATE inbox SET claimed_at=NULL")
    done = {"accepted": 0, "duplicate": 0, "dlq": 0}
    for _ in range(20):
        r = process_batch(s2, limit=100)
        for k in done:
            done[k] += r[k]
        if s2.pending_count() == 0:
            break
    assert done["accepted"] == N
    assert s2.pending_count() == 0
    u = get_usage(s2, "acme", "2026-10")
    assert u["lines"][0]["total_quantity"] == N

    # second crash variant: process half, die, resume → still exact
    s3 = Store(tmp_path / "b2.db", tmp_path / "raw2")
    for i in range(200):
        ingest_one(s3, {"event_id": f"d-{i:05d}", "customer_id": "globex",
                        "product_id": "render-studio", "metric": "render_minutes",
                        "quantity": 5.0, "timestamp": "2026-10-15T12:00:00Z"})
    r1 = process_batch(s3, limit=100)  # half
    assert r1["accepted"] == 100
    # crash, resume
    for _ in range(10):
        r = process_batch(s3, limit=100)
        if s3.pending_count() == 0:
            break
    u = get_usage(s3, "globex", "2026-10")
    assert u["lines"][0]["total_quantity"] == 1000.0
