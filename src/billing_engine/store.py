"""Durable store — local SQLite (+ JSONL raw log) with DynamoDB/S3-shaped API.

Table map (local → AWS):
  inbox       → SQS queue (pending work items; visibility via claimed_at)
  dlq         → SQS DLQ
  events      → DynamoDB `events` (PK event_id — conditional put = idempotency)
  rated       → DynamoDB `rated_records` (one row per accepted event)
  aggregates  → DynamoDB `aggregates` (PK customer#period#metric, atomic add)
  raw log dir → S3 `s3://…/raw/dt=YYYY-MM-DD/*.jsonl` (immutable source of truth)

Crash safety: ingest is transactional (raw-log append + inbox insert). A
processor killed mid-run leaves inbox rows pending → redelivered → dedupe makes
redelivery safe. That is the "zero lost events after recovery" property.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  claimed_at REAL,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_inbox_event ON inbox(event_id);
CREATE TABLE IF NOT EXISTS dlq(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  event_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  quantity REAL NOT NULL,
  ts TEXT NOT NULL,
  period TEXT NOT NULL,
  plan_version TEXT NOT NULL,
  unit_amount REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_cust ON events(customer_id, period);
CREATE TABLE IF NOT EXISTS aggregates(
  customer_id TEXT NOT NULL,
  period TEXT NOT NULL,
  metric TEXT NOT NULL,
  total_quantity REAL NOT NULL DEFAULT 0,
  event_count INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL,
  PRIMARY KEY(customer_id, period, metric)
);
CREATE TABLE IF NOT EXISTS period_close(
  customer_id TEXT NOT NULL,
  period TEXT NOT NULL,
  closed_at REAL NOT NULL,
  summary TEXT NOT NULL,
  PRIMARY KEY(customer_id, period)
);
"""


class DuplicateEvent(Exception):
    pass


class Store:
    def __init__(self, db_path: str | Path, raw_log_dir: str | Path):
        self.db_path = str(db_path)
        self.raw_log_dir = Path(raw_log_dir)
        self.raw_log_dir.mkdir(parents=True, exist_ok=True)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_lock = threading.Lock()
        # Threads of one process queue here instead of in SQLite's busy handler,
        # which sleep-polls with up-to-100ms backoff and convoys under load.
        self._write_lock = threading.Lock()
        self.lock_timeout_s = 30.0
        self._initialized = False

    def _conn(self) -> sqlite3.Connection:
        c: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None,
                                check_same_thread=False)
            c.row_factory = sqlite3.Row
            c.execute(f"PRAGMA busy_timeout={int(self.lock_timeout_s * 1000)};")
            # no checkpoint inside an ingest COMMIT (fsyncs the DB file while the
            # write lock is held); the processor runs checkpoint() instead
            c.execute("PRAGMA wal_autocheckpoint=0;")
            self._local.conn = c
        with self._init_lock:
            if not self._initialized:
                # persistent per DB file; SQLite answers "locked" here without
                # consulting the busy handler, so concurrent first opens retry
                self._retry_locked(c, "PRAGMA journal_mode=WAL")
                c.executescript(SCHEMA)
                self._initialized = True
            else:
                # ensure tables exist for new connections (cheap, idempotent)
                try:
                    c.execute("SELECT 1 FROM inbox LIMIT 1")
                except sqlite3.OperationalError:
                    c.executescript(SCHEMA)
        return c

    # ---- ingest path ----
    def append_raw_log(self, payload: dict[str, Any]) -> None:
        day = payload["timestamp"][:10]  # YYYY-MM-DD from ISO ts
        d = self.raw_log_dir / f"dt={day}"
        d.mkdir(parents=True, exist_ok=True)
        # one file per hour keeps files small and replay splittable
        hour = payload["timestamp"][:13].replace(":", "").replace("T", "-")
        fp = d / f"events-{hour}.jsonl"
        with open(fp, "a") as f:
            f.write(json.dumps(payload, sort_keys=True) + "\n")

    def _retry_locked(self, c: sqlite3.Connection, sql: str) -> None:
        """Run a lock-taking statement with short retries: SQLite's own busy
        handler backs off to 100 ms sleeps, which leaves the lock idle while
        waiters oversleep and blows the ingest p99 under load."""
        deadline = time.monotonic() + self.lock_timeout_s
        delay = 0.0002
        c.execute("PRAGMA busy_timeout=0")
        try:
            while True:
                try:
                    c.execute(sql)
                    return
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e) or time.monotonic() >= deadline:
                        raise
                time.sleep(delay)
                delay = min(delay * 2, 0.002)
        finally:
            c.execute(f"PRAGMA busy_timeout={int(self.lock_timeout_s * 1000)}")

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """Write transaction holding SQLite's RESERVED lock for its whole span, so
        multi-statement writes are atomic and serialized across workers."""
        c = self._conn()
        with self._write_lock:
            self._retry_locked(c, "BEGIN IMMEDIATE")
            try:
                yield c
            except BaseException:
                c.execute("ROLLBACK")
                raise
            c.execute("COMMIT")

    def enqueue(self, payload: dict[str, Any]) -> None:
        self.enqueue_many([payload])

    def enqueue_many(self, payloads: list[dict[str, Any]]) -> None:
        if not payloads:
            return
        with self._tx() as c:
            now = time.time()
            for p in payloads:
                self.append_raw_log(p)
                c.execute("INSERT INTO inbox(event_id, payload, created_at) VALUES(?,?,?)",
                          (p["event_id"], json.dumps(p), now))

    def claim_batch(self, limit: int = 50, visibility_s: float = 60) -> list[dict[str, Any]]:
        """Claim pending inbox rows (SQS receive analogue). Select + mark happen in
        one transaction so two workers can never claim the same row."""
        now = time.time()
        claimable = ("FROM inbox WHERE claimed_at IS NULL OR claimed_at < ? ")
        # read-only peek: idle pollers must not take the write lock from ingest
        if self._conn().execute(f"SELECT 1 {claimable} LIMIT 1",
                                (now - visibility_s,)).fetchone() is None:
            return []
        with self._tx() as c:
            rows = c.execute(
                f"SELECT seq, event_id, payload, attempts {claimable}"
                "ORDER BY seq LIMIT ?", (now - visibility_s, limit)).fetchall()
            for r in rows:
                c.execute("UPDATE inbox SET claimed_at=?, attempts=attempts+1 WHERE seq=?",
                          (now, r["seq"]))
        return [{"seq": r["seq"], "event_id": r["event_id"],
                 "payload": json.loads(r["payload"]), "attempts": r["attempts"] + 1}
                for r in rows]

    def ack(self, seq: int) -> None:
        self._conn().execute("DELETE FROM inbox WHERE seq=?", (seq,))

    def move_to_dlq(self, seq: int, event_id: str, payload: dict[str, Any], reason: str) -> None:
        with self._tx() as c:
            c.execute("INSERT INTO dlq(event_id, payload, reason, created_at) VALUES(?,?,?,?)",
                      (event_id, json.dumps(payload), reason[:500], time.time()))
            c.execute("DELETE FROM inbox WHERE seq=?", (seq,))

    def checkpoint(self) -> None:
        """PASSIVE: copies committed WAL frames without blocking readers or writers."""
        self._conn().execute("PRAGMA wal_checkpoint(PASSIVE)")

    def pending_count(self) -> int:
        return self._conn().execute("SELECT COUNT(*) FROM inbox").fetchone()[0]

    def dlq_count(self) -> int:
        return self._conn().execute("SELECT COUNT(*) FROM dlq").fetchone()[0]

    def reconcile_snapshot(self) -> tuple[set[str], set[str], dict[tuple[str, str, str], float]]:
        """(in-flight ids, DLQ ids, aggregate totals) from ONE read transaction, so
        an event processed mid-reconcile is never seen as both pending and billed.
        Ids already in `events` are neither (a redriven event that later
        succeeded is billable and must still be reconciled)."""
        c = self._conn()
        c.execute("BEGIN")
        try:
            pending = {r[0] for r in c.execute(
                "SELECT DISTINCT event_id FROM inbox "
                "WHERE event_id NOT IN (SELECT event_id FROM events)")}
            dlq = {r[0] for r in c.execute(
                "SELECT DISTINCT event_id FROM dlq "
                "WHERE event_id NOT IN (SELECT event_id FROM events)")}
            aggs = {(r[0], r[1], r[2]): float(r[3]) for r in c.execute(
                "SELECT customer_id, period, metric, total_quantity FROM aggregates")}
        finally:
            c.execute("COMMIT")
        return pending, dlq, aggs

    def oldest_pending_age_s(self) -> float:
        r = self._conn().execute("SELECT MIN(created_at) FROM inbox").fetchone()[0]
        return 0.0 if r is None else time.time() - r

    # ---- idempotent write (DynamoDB conditional-put analogue) ----
    def try_insert_event(self, payload: dict[str, Any], plan_version: str,
                         unit_amount: float) -> bool:
        """True = accepted (first time); False = duplicate (no side effects).
        Event row and aggregate increment commit together or not at all."""
        ts = payload["timestamp"]
        ts = ts if isinstance(ts, str) else ts.isoformat()
        period = ts[:7]
        try:
            with self._tx() as c:
                c.execute(
                    "INSERT INTO events(event_id,customer_id,product_id,metric,quantity,"
                    "ts,period,plan_version,unit_amount) VALUES(?,?,?,?,?,?,?,?,?)",
                    (payload["event_id"], payload["customer_id"], payload["product_id"],
                     payload["metric"], float(payload["quantity"]), ts, period,
                     plan_version, unit_amount))
                c.execute(
                    "INSERT INTO aggregates(customer_id,period,metric,total_quantity,"
                    "event_count,updated_at) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(customer_id,period,metric) DO UPDATE SET "
                    "total_quantity=total_quantity+excluded.total_quantity, "
                    "event_count=event_count+1, updated_at=excluded.updated_at",
                    (payload["customer_id"], period, payload["metric"],
                     float(payload["quantity"]), 1, time.time()))
        except sqlite3.IntegrityError:
            return False
        return True

    # ---- reads ----
    def get_event(self, event_id: str) -> dict[str, Any] | None:
        r = self._conn().execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
        return dict(r) if r else None

    def aggregates_for(self, customer_id: str, period: str) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT * FROM aggregates WHERE customer_id=? AND period=?",
            (customer_id, period)).fetchall()
        return [dict(r) for r in rows]

    def events_for(self, customer_id: str, period: str) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT * FROM events WHERE customer_id=? AND period=? ORDER BY ts",
            (customer_id, period)).fetchall()
        return [dict(r) for r in rows]

    def all_raw_payloads(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for fp in sorted(self.raw_log_dir.rglob("*.jsonl")):
            with open(fp) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        out.append(json.loads(line))
        return out

    def close_period(self, customer_id: str, period: str, summary: dict[str, Any]) -> None:
        self._conn().execute(
            "INSERT INTO period_close(customer_id,period,closed_at,summary)"
            "VALUES(?,?,?,?) ON CONFLICT(customer_id,period) DO UPDATE SET "
            "closed_at=excluded.closed_at, summary=excluded.summary",
            (customer_id, period, time.time(), json.dumps(summary)))

    def get_close(self, customer_id: str, period: str) -> dict[str, Any] | None:
        r = self._conn().execute(
            "SELECT * FROM period_close WHERE customer_id=? AND period=?",
            (customer_id, period)).fetchone()
        if not r:
            return None
        d = dict(r)
        d["summary"] = json.loads(d["summary"])
        return d


_default_store: Store | None = None


def get_store(db_path: str | None = None, raw_log_dir: str | None = None) -> Store:
    import os as _os
    global _default_store
    if db_path is not None or raw_log_dir is not None:
        from billing_engine.config import SETTINGS
        return Store(db_path or SETTINGS.db_path, raw_log_dir or SETTINGS.raw_log_dir)
    if _default_store is None:
        from billing_engine.config import SETTINGS
        _db = _os.environ.get("BILLING_DB_PATH", SETTINGS.db_path)
        _raw = _os.environ.get("BILLING_RAW_LOG_DIR", SETTINGS.raw_log_dir)
        _default_store = Store(_db, _raw)
    return _default_store


def reset_store_cache() -> None:
    global _default_store
    _default_store = None
