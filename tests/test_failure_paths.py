"""Failure modes from RUNBOOK: poison messages, retry exhaustion, redelivery,
alarms, tampered aggregates, and timezone bucketing."""
import threading
import time

import pytest
from conftest import make_event

from billing_engine.ingest import IngestRejected, ingest_batch, ingest_one
from billing_engine.observability import Metrics
from billing_engine.processor import drain, process_batch
from billing_engine.query import get_usage
from billing_engine.reconcile import reconcile


FR2 = pytest.mark.req("FR2")
FR2_LOSS = pytest.mark.req("FR2", "NFR-CORRECTNESS")
FR5 = pytest.mark.req("FR5")
FR6 = pytest.mark.req("FR6")


@FR2
def test_unknown_metric_goes_to_dlq(store):
    ingest_one(store, make_event("poison-1", metric="not_a_metric"))
    r = process_batch(store)
    assert r == {"accepted": 0, "duplicate": 0, "dlq": 1}
    assert store.dlq_count() == 1 and store.pending_count() == 0
    row = store._conn().execute("SELECT event_id, reason FROM dlq").fetchone()
    assert row["event_id"] == "poison-1" and row["reason"].startswith("unratable:")
    assert store.get_event("poison-1") is None


@FR2
def test_max_retries_exceeded_goes_to_dlq(store):
    from billing_engine.config import SETTINGS
    ingest_one(store, make_event("retry-1"))
    for _ in range(SETTINGS.max_retries):  # each claim = one failed delivery attempt
        store.claim_batch(limit=1, visibility_s=-1)
    store._conn().execute("UPDATE inbox SET claimed_at=NULL")
    r = process_batch(store)
    assert r["dlq"] == 1 and r["accepted"] == 0
    reason = store._conn().execute("SELECT reason FROM dlq").fetchone()[0]
    assert reason == "max_retries_exceeded"
    assert store.get_event("retry-1") is None


@FR2_LOSS
def test_visibility_timeout_redelivery(store):
    ingest_one(store, make_event("vis-1"))
    first = store.claim_batch(limit=10, visibility_s=60)
    assert len(first) == 1 and first[0]["attempts"] == 1
    assert store.claim_batch(limit=10, visibility_s=60) == []  # still invisible
    time.sleep(0.01)
    again = store.claim_batch(limit=10, visibility_s=0)  # visibility expired
    assert len(again) == 1 and again[0]["attempts"] == 2
    assert drain(store)["accepted"] == 0  # claimed rows invisible to drain's 60s window
    store._conn().execute("UPDATE inbox SET claimed_at=NULL")
    assert drain(store)["accepted"] == 1


class _CrashOnce:
    """Connection proxy that raises the first time a statement matches `needle`,
    simulating a worker killed between two writes."""

    def __init__(self, conn, needle):
        self._c, self._needle, self.fired = conn, needle, False

    def execute(self, sql, *args):
        if not self.fired and self._needle in sql:
            self.fired = True
            raise RuntimeError("worker killed")
        return self._c.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._c, name)


@pytest.mark.parametrize("needle", ["INSERT INTO aggregates", "DELETE FROM inbox"])
@FR2_LOSS
def test_crash_mid_write_never_loses_or_double_counts(store, needle):
    """Kill the worker between the event insert and aggregate update (or the ack)."""
    from billing_engine.processor import process_one
    ingest_one(store, make_event("atom-1", quantity=7))
    proxy = _CrashOnce(store._conn(), needle)
    store._local.conn = proxy
    item = store.claim_batch(limit=1)[0]
    with pytest.raises(RuntimeError):
        process_one(store, item)
    assert proxy.fired
    store._local.conn = proxy._c
    store._conn().execute("UPDATE inbox SET claimed_at=NULL")  # visibility expires
    drain(store)
    assert store.pending_count() == 0
    u = get_usage(store, "acme", "2026-10")["lines"][0]
    assert u["total_quantity"] == 7 and u["event_count"] == 1
    assert reconcile(store)["match"]


@FR2
def test_crash_mid_dlq_move_leaves_single_dlq_row(store):
    ingest_one(store, make_event("p-1", metric="nope"))
    proxy = _CrashOnce(store._conn(), "DELETE FROM inbox")
    store._local.conn = proxy
    with pytest.raises(RuntimeError):
        process_batch(store)
    store._local.conn = proxy._c
    store._conn().execute("UPDATE inbox SET claimed_at=NULL")
    drain(store)
    assert store.dlq_count() == 1 and store.pending_count() == 0


@FR2
def test_drain_stops_at_max_rounds(store):
    for i in range(5):
        ingest_one(store, make_event(f"mr-{i}"))
    r = drain(store, batch=1, max_rounds=2)
    assert r["accepted"] == 2 and store.pending_count() == 3


@FR2_LOSS
def test_enqueue_rolls_back_when_raw_log_write_fails(store, monkeypatch):
    def boom(_payload):
        raise OSError("disk full")
    monkeypatch.setattr(store, "append_raw_log", boom)
    with pytest.raises(OSError):
        ingest_one(store, make_event("rb-1"))
    assert store.pending_count() == 0


@FR2
def test_store_recreates_schema_for_new_thread_connection(store):
    assert store.pending_count() == 0
    store._conn().executescript("DROP TABLE inbox;")
    out = {}
    t = threading.Thread(target=lambda: out.setdefault("n", store.pending_count()))
    t.start()
    t.join()
    assert out["n"] == 0


@FR5
def test_raw_log_reader_skips_blank_lines(store):
    ingest_one(store, make_event("bl-1"))
    fp = next(store.raw_log_dir.rglob("*.jsonl"))
    fp.write_text(fp.read_text() + "\n  \n")
    assert [p["event_id"] for p in store.all_raw_payloads()] == ["bl-1"]


@FR2
def test_get_store_with_explicit_paths_bypasses_singleton(tmp_path, default_store):
    from billing_engine.store import get_store
    s = get_store(db_path=str(tmp_path / "other.db"))
    assert s is not default_store and s.db_path.endswith("other.db")
    assert get_store() is default_store


@FR5
def test_reconcile_detects_tampered_aggregate(store):
    ingest_one(store, make_event("t-1", quantity=5))
    drain(store)
    store._conn().execute("UPDATE aggregates SET total_quantity = total_quantity + 1")
    r = reconcile(store)
    assert not r["match"]
    assert r["mismatches"] == [{"customer_id": "acme", "period": "2026-10",
                                "metric": "cloud_credits", "stored": 6.0,
                                "recomputed": 5.0, "diff": 1.0}]


@FR5
def test_reconcile_dedupes_redelivered_raw_events_and_filters_customer(store):
    ingest_one(store, make_event("rd-1", quantity=4))
    ingest_one(store, make_event("rd-1", quantity=4))
    ingest_one(store, make_event("g-1", customer_id="globex", quantity=1))
    drain(store)
    r = reconcile(store, "acme")
    assert r["match"] and r["checked"] == 1 and r["raw_events_scanned"] == 3


@FR2
def test_non_utc_timestamp_buckets_by_utc_period(store):
    ingest_one(store, make_event("tz-1", timestamp="2026-10-31T23:00:00-05:00"))
    drain(store)
    ev = store.get_event("tz-1")
    assert ev["period"] == "2026-11" and ev["ts"].startswith("2026-11-01T04:00:00")
    assert get_usage(store, "acme", "2026-11")["lines"][0]["total_quantity"] == 10.0
    assert get_usage(store, "acme", "2026-10")["lines"] == []
    assert list(store.raw_log_dir.glob("dt=2026-11-01/*.jsonl"))
    assert reconcile(store)["match"]


@pytest.mark.req("FR1")
def test_ingest_batch_reports_rejected_and_enforces_limit(store):
    res = ingest_batch(store, [make_event("ok-1"), {"event_id": "bad"}])
    assert res["accepted_count"] == 1 and res["rejected_count"] == 1
    assert res["rejected"][0]["payload_event_id"] == "bad"
    with pytest.raises(IngestRejected):
        ingest_batch(store, [make_event(f"x-{i}") for i in range(101)])


@FR6
def test_alarms_fire_on_dlq_queue_age_and_error_rate():
    m = Metrics()
    assert m.check_alarms() == []
    names = {a["alarm"] for a in m.check_alarms(queue_age_s=31, dlq_depth=1)}
    assert names == {"DLQDepthPositive", "QueueAgeHigh"}
    m.inc("processor_accepted_total", 18)
    m.inc("processor_error_total", 2)
    assert [a["alarm"] for a in m.check_alarms()] == ["ErrorRateHigh"]


@FR6
def test_error_rate_alarm_needs_minimum_volume():
    m = Metrics()
    m.inc("processor_error_total", 5)  # 100% errors but < 20 samples
    assert m.check_alarms() == []


@FR6
def test_metrics_snapshot_percentiles_and_gauges():
    m = Metrics()
    assert m.snapshot()["latency_ms"] == {}
    for v in range(1, 101):
        m.observe_latency("ingest_ms", float(v))
    m.set_gauge("queue_pending", 3)
    m.inc("processor_duplicate_total")
    m.inc("processor_accepted_total", 3)
    s = m.snapshot()
    assert s["latency_ms"]["ingest_ms"] == {"p50": 51.0, "p95": 96.0, "p99": 100.0,
                                            "count": 100}
    assert s["gauges"] == {"queue_pending": 3}
    assert s["derived"]["duplicate_rate"] == 0.25
    assert Metrics._pct([], 0.5) is None


@FR6
def test_latency_samples_are_bounded():
    m = Metrics()
    for i in range(5100):
        m.observe_latency("x", float(i))
    assert m.snapshot()["latency_ms"]["x"]["count"] == 5000
