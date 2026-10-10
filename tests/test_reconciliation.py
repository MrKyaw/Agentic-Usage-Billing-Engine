import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from billing_engine.ingest import _norm, ingest_batch, ingest_one  # noqa: E402
from billing_engine.period_close import close_period  # noqa: E402
from billing_engine.processor import drain  # noqa: E402
from billing_engine.query import get_bill  # noqa: E402
from billing_engine.reconcile import reconcile  # noqa: E402
from billing_engine.store import Store  # noqa: E402

import pytest  # noqa: E402

pytestmark = pytest.mark.req("FR4", "FR5")


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


def _ev(eid, metric="cloud_credits", qty=100, ts="2026-10-09T10:00:00Z"):
    return {"event_id": eid, "customer_id": "acme", "product_id": "design-suite",
            "metric": metric, "quantity": qty, "timestamp": ts}


def test_reconcile_ignores_dlq_events(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, _ev("ok-1"))
    ingest_one(s, _ev("bad-1", metric="not_a_metric", qty=5))
    ingest_one(s, _ev("bad-1", metric="not_a_metric", qty=5))  # producer redelivery
    r = drain(s)
    assert r["dlq"] == 2 and s.dlq_count() == 2
    rec = reconcile(s)
    assert rec["match"], rec["mismatches"]
    assert rec["dlq_excluded"] == 1
    scoped = reconcile(s, "acme", "2026-10")
    assert scoped["match"] and scoped["dlq_excluded"] == 1
    assert reconcile(s, "other", "2026-10")["dlq_excluded"] == 0


def test_reconcile_counts_event_that_recovered_after_dlq(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, _ev("flaky-1"))
    item = s.claim_batch(limit=1)[0]
    s.move_to_dlq(item["seq"], item["event_id"], item["payload"], "max_retries_exceeded")
    ingest_one(s, _ev("flaky-1"))  # redriven from DLQ, processed successfully
    drain(s)
    rec = reconcile(s)
    assert rec["match"], rec["mismatches"]
    assert rec["dlq_excluded"] == 0


def test_closed_bill_is_frozen_against_late_events(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, _ev("c-1"))
    drain(s)
    close_period(s, "acme", "2026-10", export_dir=tmp_path / "exports")
    ingest_one(s, _ev("late-after-close"))
    drain(s)
    b = get_bill(s, "acme", "2026-10")
    assert b["status"] == "closed"
    assert b["total_amount"] == 10.0
    assert b["lines"][0]["total_quantity"] == 100
    assert b["post_close_delta"] == 10.0
    # reconciliation still checks the live aggregates against the raw log
    assert reconcile(s)["match"]


def test_reclose_refreezes_live_totals(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, _ev("c-1"))
    drain(s)
    close_period(s, "acme", "2026-10", export_dir=tmp_path / "exports")
    ingest_one(s, _ev("late-after-close"))
    drain(s)
    out = close_period(s, "acme", "2026-10", export_dir=tmp_path / "exports")
    assert out["bill"]["total_amount"] == 20.0
    b = get_bill(s, "acme", "2026-10")
    assert b["total_amount"] == 20.0 and b["post_close_delta"] == 0.0
    assert "20.0" in Path(out["csv"]).read_text()


def test_reconcile_tolerates_float_summation_order(tmp_path):
    """Arrival order != raw-log replay order; the float sums differ by 1 ULP."""
    s = _store(tmp_path)
    qs = [0.0006297235518958485, 0.000333362490174769, 0.0009672430633357533,
          0.00030406514242198475, 549425852.0124679]
    for i, q in enumerate(qs):  # later arrivals carry earlier hours -> reversed replay
        ingest_one(s, _ev(f"f{i}", qty=q, ts=f"2026-10-09T{20 - i:02d}:00:00Z"))
    drain(s)
    rec = reconcile(s)
    assert rec["match"], rec["mismatches"]


def test_reconcile_still_flags_one_unit_drift_on_large_totals(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, _ev("big", qty=549425852.0))
    drain(s)
    s._conn().execute("UPDATE aggregates SET total_quantity = total_quantity + 1")
    assert not reconcile(s)["match"]


def test_open_bill_has_zero_post_close_delta(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, _ev("o-1"))
    drain(s)
    b = get_bill(s, "acme", "2026-10")
    assert b["status"] == "open" and b["closed_at"] is None and b["post_close_delta"] == 0.0


def test_in_flight_events_are_excluded_not_flagged(tmp_path):
    """Live traffic: queued-but-unprocessed events are tracked by the queue-age
    alarm, not reported as missing money."""
    s = _store(tmp_path)
    ingest_one(s, _ev("done-1", qty=3))
    drain(s)
    ingest_one(s, _ev("queued-1", qty=5))
    ingest_one(s, _ev("queued-2", qty=7))
    rec = reconcile(s)
    assert rec["match"], rec["mismatches"]
    assert rec["pending_excluded"] == 2 and rec["raw_events_scanned"] == 3
    drain(s)
    rec = reconcile(s)
    assert rec["match"] and rec["pending_excluded"] == 0


def test_redelivered_copy_of_billed_event_is_not_excluded(tmp_path):
    s = _store(tmp_path)
    ingest_one(s, _ev("r-1", qty=4))
    drain(s)
    ingest_one(s, _ev("r-1", qty=4))  # redelivery still queued
    rec = reconcile(s)
    assert rec["match"] and rec["pending_excluded"] == 0


def test_event_lost_after_raw_log_is_still_flagged(tmp_path):
    """In the raw log but nowhere in the DB (not queued, not DLQ, not billed)."""
    s = _store(tmp_path)
    ingest_one(s, _ev("kept", qty=2))
    drain(s)
    s.append_raw_log(_norm(_ev("lost", qty=9)))
    rec = reconcile(s)
    assert not rec["match"]
    assert rec["mismatches"][0]["diff"] == -9.0
