#!/usr/bin/env bash
# End-to-end demo: ingest → process → duplicate-proof → queries → reconcile → MCP agent.
# PRD §9 Day-2 exit: runs live with zero AWS dependency.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="$ROOT/src"
export BILLING_DB_PATH="$ROOT/data/demo.db"
export BILLING_RAW_LOG_DIR="$ROOT/data/demo-raw"
rm -rf "$BILLING_DB_PATH" "$BILLING_RAW_LOG_DIR" "$ROOT/exports"
mkdir -p "$ROOT/data"

python3 - "$ROOT" <<'PY'
import os, sys
sys.path.insert(0, os.environ.get("PYROOT", "src"))
PY

export PYTHONPATH="$ROOT/src"
python3 <<'PY'
import json
from billing_engine.store import Store, reset_store_cache
from billing_engine.ingest import ingest_batch, ingest_one
from billing_engine.processor import drain
from billing_engine.query import get_bill, get_usage
from billing_engine.reconcile import reconcile
from billing_engine.period_close import close_period
import os
reset_store_cache()
s = Store(os.environ["BILLING_DB_PATH"], os.environ["BILLING_RAW_LOG_DIR"])

# 1. ingest 120 events across 2 customers / 2 metrics (incl. out-of-order)
evs = []
for i in range(100):
    evs.append({"event_id": f"evt-demo-{i:03d}", "customer_id": "acme",
                "product_id": "design-suite", "metric": "cloud_credits", "quantity": 2.0,
                "timestamp": "2026-10-09T10:00:00Z"})
for i in range(20):
    evs.append({"event_id": f"evt-rnd-{i:03d}", "customer_id": "globex",
                "product_id": "render-studio", "metric": "render_minutes", "quantity": 30.0,
                "timestamp": "2026-10-09T09:00:00Z"})
print("ingest:", ingest_batch(s, evs[:100]))
print("ingest:", ingest_batch(s, evs[100:]))
print("process:", drain(s))

# 2. Day-1 exit: duplicate must not change aggregate
before = get_usage(s, "acme", "2026-10")["lines"][0]["total_quantity"]
for e in evs[:12]:  # 10%+ duplicates
    ingest_one(s, e)
print("duplicates processed:", drain(s))
after = get_usage(s, "acme", "2026-10")["lines"][0]["total_quantity"]
assert before == after == 200.0, (before, after)
print(f"PASS duplicate-safety: total_quantity={after}")

# 3. billing + close + CSV stub
print("bill acme:", json.dumps(get_bill(s, "acme", "2026-10"), indent=2))
print("bill globex:", json.dumps(get_bill(s, "globex", "2026-10"), indent=2))
print("close:", close_period(s, "acme", "2026-10", export_dir="exports")["csv"])

# 4. reconciliation: 0 diffs
r = reconcile(s)
print("reconcile:", json.dumps(r, indent=2))
assert r["match"], r["mismatches"]
print("PASS reconciliation: 0 diffs")
PY

echo "--- agent demo (MCP tools, no model key needed) ---"
BILLING_DB_PATH="$ROOT/data/demo.db" BILLING_RAW_LOG_DIR="$ROOT/data/demo-raw" \
  python3 -m billing_engine.agent_demo --customer acme --period 2026-10
echo "=== DEMO COMPLETE ==="
