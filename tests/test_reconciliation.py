import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from billing_engine.ingest import ingest_batch, ingest_one  # noqa: E402
from billing_engine.period_close import close_period  # noqa: E402
from billing_engine.processor import drain  # noqa: E402
from billing_engine.query import get_bill  # noqa: E402
from billing_engine.reconcile import reconcile  # noqa: E402
from billing_engine.store import Store  # noqa: E402


def _store(tmp_path):
    return Store(tmp_path / "b.db", tmp_path / "raw")


def test_reconcile_clean(tmp_path):
    s = _store(tmp_path)
    evs = [{"event_id": f"r-{i}", "customer_id": "acme", "product_id": "design-suite",
            "metric": "cloud_credits", "quantity": 3.0,
            "timestamp": "2026-10-09T10:00:00Z"} for i in range(50)]
    ingest_batch(s, evs)
    drain(s)
    r = reconcile(s)
    assert r["match"] and r["mismatches"] == [] and r["checked"] >= 1


def test_reconcile_out_of_order_late_events(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, {"event_id": "late-1", "customer_id": "acme", "product_id": "design-suite",
                   "metric": "cloud_credits", "quantity": 10,
                   "timestamp": "2026-10-01T00:00:01Z"})
    ingest_one(s, {"event_id": "late-2", "customer_id": "acme", "product_id": "design-suite",
                   "metric": "cloud_credits", "quantity": 5,
                   "timestamp": "2026-09-30T23:59:59Z"})  # arrives late, buckets to Sep
    drain(s)
    assert reconcile(s, "acme", "2026-10")["match"]
    assert reconcile(s, "acme", "2026-09")["match"]
    assert reconcile(s)["match"]


def test_period_close_csv(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, {"event_id": "c-1", "customer_id": "acme", "product_id": "design-suite",
                   "metric": "cloud_credits", "quantity": 100,
                   "timestamp": "2026-10-09T10:00:00Z"})
    drain(s)
    out = close_period(s, "acme", "2026-10", export_dir=tmp_path / "exports")
    assert out["csv"].endswith(".csv") and Path(out["csv"]).exists()
    b = get_bill(s, "acme", "2026-10")
    assert b["status"] == "closed" and b["total_amount"] == 10.0
