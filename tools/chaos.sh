#!/usr/bin/env bash
# Failure injection (PRD §8 Day-2 6-8h block): duplicates, crash, out-of-order.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT/src" BILLING_DB_PATH="$ROOT/data/chaos.db" BILLING_RAW_LOG_DIR="$ROOT/data/chaos-raw"
rm -rf "$ROOT/data/chaos.db" "$ROOT/data/chaos-raw"
python3 - <<'PY'
from billing_engine.store import Store
from billing_engine.ingest import ingest_one
from billing_engine.processor import process_batch, drain
from billing_engine.reconcile import reconcile
s = Store("data/chaos.db", "data/chaos-raw")
for i in range(300):
    ingest_one(s, {"event_id": f"ch-{i:04d}", "customer_id": "acme", "product_id": "design-suite",
                   "metric": "cloud_credits", "quantity": 1, "timestamp": "2026-10-09T10:00:00Z"})
process_batch(s, limit=100)  # partial… then "crash" (just stop calling)
print("mid-crash pending:", s.pending_count())
print("recovery:", drain(s))  # new worker resumes
for i in range(0, 300, 10):  # 10% dupes after recovery
    ingest_one(s, {"event_id": f"ch-{i:04d}", "customer_id": "acme", "product_id": "design-suite",
                   "metric": "cloud_credits", "quantity": 1, "timestamp": "2026-10-09T10:00:00Z"})
ingest_one(s, {"event_id": "ch-late", "customer_id": "acme", "product_id": "design-suite",
               "metric": "cloud_credits", "quantity": 7, "timestamp": "2026-10-01T00:00:01Z"})
print("post-dupe drain:", drain(s))
r = reconcile(s)
print("reconcile match:", r["match"], "checked:", r["checked"])
assert r["match"]
from billing_engine.query import get_usage
assert get_usage(s, "acme", "2026-10")["lines"][0]["total_quantity"] == 307.0
print("CHAOS PASS: 0 double-counts, 0 lost, late event bucketed correctly")
PY
