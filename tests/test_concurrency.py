"""Contention: many threads / processes sharing one SQLite DB must never lose,
double-claim, or double-count an event."""
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from conftest import SRC, make_event

from billing_engine.ingest import ingest_one
from billing_engine.processor import drain
from billing_engine.query import get_usage
from billing_engine.reconcile import reconcile
from billing_engine.store import Store

pytestmark = [pytest.mark.concurrency, pytest.mark.req("FR2", "NFR-CORRECTNESS")]

CLAIMER = """
import json, sys
sys.path.insert(0, sys.argv[3])
from billing_engine.store import Store
s = Store(sys.argv[1], sys.argv[2])
seqs = []
while True:
    got = s.claim_batch(limit=25)
    if not got:
        break
    seqs += [g["seq"] for g in got]
print(json.dumps(seqs))
"""


def _fill(store, n, prefix="c", **kw):
    for i in range(n):
        ingest_one(store, make_event(f"{prefix}-{i:05d}", quantity=1, **kw))


def test_concurrent_ingest_threads_lose_nothing(store):
    n_threads, per_thread = 8, 150

    def produce(t):
        for i in range(per_thread):
            ingest_one(store, make_event(f"t{t}-{i:04d}", quantity=1))

    with ThreadPoolExecutor(n_threads) as ex:
        list(ex.map(produce, range(n_threads)))
    total = n_threads * per_thread
    assert store.pending_count() == total
    assert len(store.all_raw_payloads()) == total  # no torn / interleaved raw-log lines
    drain(store)
    assert get_usage(store, "acme", "2026-10")["lines"][0]["total_quantity"] == total
    assert reconcile(store)["match"]


def test_threads_never_claim_the_same_row(store):
    _fill(store, 1500)
    claimed: list[int] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def claimer():
        barrier.wait()
        while True:
            got = store.claim_batch(limit=25)
            if not got:
                return
            with lock:
                claimed.extend(g["seq"] for g in got)

    threads = [threading.Thread(target=claimer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == 1500
    assert len(set(claimed)) == 1500, f"{len(claimed) - len(set(claimed))} rows double-claimed"


def test_processes_never_claim_the_same_row(tmp_path):
    db, raw = tmp_path / "b.db", tmp_path / "raw"
    _fill(Store(db, raw), 3000)
    procs = [subprocess.Popen([sys.executable, "-c", CLAIMER, str(db), str(raw), str(SRC)],
                              stdout=subprocess.PIPE, text=True) for _ in range(6)]
    claimed = []
    for p in procs:
        out, _ = p.communicate(timeout=120)
        assert p.returncode == 0
        claimed += json.loads(out.strip().splitlines()[-1])
    assert len(claimed) == 3000
    assert len(set(claimed)) == 3000, f"{len(claimed) - len(set(claimed))} rows double-claimed"


def test_multiple_worker_processes_bill_exactly_once(tmp_path):
    db, raw = tmp_path / "b.db", tmp_path / "raw"
    s = Store(db, raw)
    _fill(s, 2000)
    _fill(s, 500, prefix="g", customer_id="globex", metric="render_minutes")
    env = {**os.environ, "PYTHONPATH": str(SRC), "BILLING_DB_PATH": str(db),
           "BILLING_RAW_LOG_DIR": str(raw)}
    procs = [subprocess.Popen([sys.executable, "-m", "billing_engine.worker", "--poll-s", "0.02",
                               "--batch", "20", "--max-idle-polls", "5"],
                              env=env, stdout=subprocess.PIPE, text=True) for _ in range(4)]
    logs = [p.communicate(timeout=180)[0] for p in procs]
    assert all(p.returncode == 0 for p in procs)
    msgs = [json.loads(line)["msg"] for out in logs for line in out.splitlines()
            if line.startswith("{")]
    assert msgs.count("processor_accepted") == 2500
    assert msgs.count("processor_duplicate") == 0, "a row was processed by two workers"
    assert s.pending_count() == 0 and s.dlq_count() == 0
    assert get_usage(s, "acme", "2026-10")["lines"][0]["total_quantity"] == 2000
    assert get_usage(s, "globex", "2026-10")["lines"][0]["event_count"] == 500
    assert reconcile(s)["match"]


@pytest.mark.e2e
def test_concurrent_http_ingest_with_three_workers(live_factory):
    live = live_factory(workers=3)
    events = [make_event(f"h-{i:05d}", customer_id=f"cust-{i % 5}", quantity=2)
              for i in range(1200)]
    redeliveries = events[::10]

    def send(ev):
        return live.client.post("/v1/usage-events", json=ev).status_code

    with ThreadPoolExecutor(16) as ex:
        codes = list(ex.map(send, events + redeliveries))
    assert set(codes) == {202}
    live.wait_drained(timeout=120)
    for c in range(5):
        u = live.client.get(f"/v1/customers/cust-{c}/usage", params={"period": "2026-10"}).json()
        assert u["lines"][0]["event_count"] == 240 and u["lines"][0]["total_quantity"] == 480
    assert live.client.get("/v1/ops/reconcile").json()["match"]
    dup_logs = sum(f.read_text().count('"processor_duplicate"')
                   for f in live.tmp_path.glob("worker-*.log"))
    assert dup_logs == len(redeliveries)  # only producer redeliveries, never double-claims


# ------------------------------------------------- write-lock acquisition

def _hold_write_lock(store):
    import sqlite3
    other = sqlite3.connect(store.db_path, isolation_level=None, check_same_thread=False)
    other.execute("BEGIN IMMEDIATE")
    return other


def test_write_lock_waits_for_release_then_succeeds(store):
    store.lock_timeout_s = 5.0
    store._conn()
    other = _hold_write_lock(store)
    threading.Timer(0.1, other.execute, args=("ROLLBACK",)).start()
    ingest_one(store, make_event("after-release"))
    assert store.pending_count() == 1


def test_write_lock_times_out_and_restores_busy_timeout(store):
    import sqlite3
    store.lock_timeout_s = 0.05
    store._conn()
    other = _hold_write_lock(store)
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            store.enqueue(make_event("blocked") | {"metadata": {}})
    finally:
        other.execute("ROLLBACK")
    assert store._conn().execute("PRAGMA busy_timeout").fetchone()[0] == 50
    assert store.pending_count() == 0


def test_non_lock_errors_are_not_retried(store):
    import sqlite3
    c = store._conn()
    c.execute("BEGIN")
    with pytest.raises(sqlite3.OperationalError, match="within a transaction"):
        store._retry_locked(c, "BEGIN IMMEDIATE")
    c.execute("ROLLBACK")


def test_idle_claim_does_not_need_the_write_lock(store):
    """Idle workers poll constantly; they must not contend with ingest."""
    store.lock_timeout_s = 0.05
    store._conn()
    other = _hold_write_lock(store)
    try:
        assert store.claim_batch() == []
    finally:
        other.execute("ROLLBACK")


@pytest.mark.req("NFR-LATENCY")
def test_checkpoints_run_in_processor_not_in_ingest_commit(store):
    c = store._conn()
    assert c.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 0
    _fill(store, 30)
    drain(store)
    busy, log, done = c.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
    assert busy == 0 and log == done


@pytest.mark.req("FR5")
def test_reconcile_never_false_alarms_while_events_are_processing(store):
    """Reconcile runs against live traffic: every snapshot must balance even
    though workers move events from pending to billed underneath it."""
    _fill(store, 1500)
    stop = threading.Event()

    def work():
        while not stop.is_set() and store.pending_count():
            drain(Store(store.db_path, store.raw_log_dir), batch=20, max_rounds=1)

    th = threading.Thread(target=work)
    th.start()
    results = []
    try:
        while th.is_alive():
            results.append(reconcile(store))
    finally:
        stop.set()
        th.join()
    results.append(reconcile(store))
    assert len(results) > 3
    assert all(r["match"] for r in results), next(r for r in results if not r["match"])
    assert results[-1]["pending_excluded"] == 0
    assert any(0 < r["pending_excluded"] < 1500 for r in results)


def test_first_open_retries_wal_switch_while_db_is_busy(tmp_path):
    """Switching to WAL needs an exclusive lock and SQLite reports "locked"
    without consulting the busy handler; several threads/processes opening a
    brand-new DB at once used to fail here intermittently."""
    import sqlite3
    db = tmp_path / "fresh.db"
    holder = sqlite3.connect(db, isolation_level=None, check_same_thread=False)
    holder.execute("CREATE TABLE t(x)")
    holder.execute("BEGIN IMMEDIATE")
    probe = sqlite3.connect(db, timeout=5, isolation_level=None)
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        probe.execute("PRAGMA journal_mode=WAL")  # instant, despite timeout=5
    probe.close()

    threading.Timer(0.1, holder.execute, args=("ROLLBACK",)).start()
    s = Store(db, tmp_path / "raw")
    ingest_one(s, make_event("first"))
    assert s._conn().execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert s.pending_count() == 1
