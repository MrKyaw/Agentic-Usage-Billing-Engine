#!/usr/bin/env bash
# Containerised end-to-end: `docker compose` api + worker sharing one SQLite DB,
# driven over HTTP like a producer and a finance client. Uses its own compose
# project, port and throwaway data dir, so it never touches ./data or a running
# `make up` / `make api`.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-billing-smoke}"
export PORT="${SMOKE_PORT:-18000}"
DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/billing-smoke.XXXXXX")"
export DATA_DIR
mkdir -p "$DATA_DIR/exports"
PERIOD="$(date -u +%Y-%m)"
KEY="${BILLING_API_KEY:-dev-key-123}"

passed=0
cleanup() {
  if [ "$passed" != 1 ]; then
    echo "--- container logs (last 60 lines)"
    docker compose logs --no-color --tail 60 || true
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "--- docker compose up (project=$COMPOSE_PROJECT_NAME port=$PORT data=$DATA_DIR)"
docker compose up -d --build --wait

python3 - "http://localhost:$PORT" "$KEY" "$PERIOD" "$DATA_DIR" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

base, key, period, data_dir = sys.argv[1:5]


def call(method, path, body=None, auth=True):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    if auth:
        req.add_header("X-API-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def jcall(method, path, body=None):
    status, raw = call(method, path, body)
    return status, json.loads(raw)


def check(cond, msg):
    if not cond:
        sys.exit(f"FAIL {msg}")
    print(f"PASS {msg}")


def drained(timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if jcall("GET", "/v1/ops/metrics")[1]["queue"]["pending"] == 0:
            return True
        time.sleep(0.25)
    return False


def event(eid, qty):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"event_id": eid, "customer_id": "acme", "product_id": "design-suite",
            "metric": "cloud_credits", "quantity": qty, "timestamp": ts}


check(call("GET", "/health", auth=False)[0] == 200, "GET /health -> 200")
check(call("GET", "/docs", auth=False)[0] == 200, "GET /docs -> 200")
check(call("GET", f"/v1/customers/acme/bill?period={period}", auth=False)[0] == 401,
      "missing X-API-Key -> 401")

ev = event("smoke-001", 10)
codes = [call("POST", "/v1/usage-events", ev)[0] for _ in range(3)]
check(codes == [202, 202, 202], "POST event + 2 duplicates -> 202 x3")
check(call("POST", "/v1/usage-events", event("smoke-bad", 1) | {"metric": ""})[0] == 400,
      "invalid event -> 400")

check(drained(), "worker container drained the queue")
_, bill = jcall("GET", f"/v1/customers/acme/bill?period={period}")
line = bill["lines"][0] if len(bill["lines"]) == 1 else {}
check(line.get("event_count") == 1 and line.get("total_quantity") == 10.0,
      f"billed exactly once (event_count={line.get('event_count')}, "
      f"quantity={line.get('total_quantity')})")
check(bill["total_amount"] == 1.0, f"bill total_amount == 1.0 (got {bill['total_amount']})")

_, rec = jcall("GET", f"/v1/ops/reconcile?period={period}")
check(rec["match"] is True, "GET /v1/ops/reconcile -> match")

status, closed = jcall("POST", f"/v1/customers/acme/close?period={period}")
check(status == 200 and closed["bill"]["status"] == "closed", "period close -> closed")
csv = Path(data_dir) / "exports" / f"bill-acme-{period}.csv"
check(csv.is_file() and "TOTAL" in csv.read_text(), f"CSV export on host volume ({csv.name})")

check(call("POST", "/v1/usage-events", event("smoke-late", 10))[0] == 202, "late event -> 202")
check(drained(), "worker drained late event")
_, bill = jcall("GET", f"/v1/customers/acme/bill?period={period}")
check(bill["status"] == "closed" and bill["total_amount"] == 1.0
      and bill["post_close_delta"] == 1.0,
      f"closed bill frozen, late usage in post_close_delta ({bill['post_close_delta']})")
PY

echo "--- reconcile CLI inside the api container"
out="$(docker compose exec -T api python -m billing_engine.reconcile --period "$PERIOD")"
echo "$out" | python3 -c 'import json,sys; r=json.load(sys.stdin); assert r["match"], r; print("PASS reconcile CLI -> match (raw_events_scanned=%d)" % r["raw_events_scanned"])'

echo "--- SQLite write lock is enforced across the api and worker containers"
docker compose exec -d api python -c "
import sqlite3, time
c = sqlite3.connect('/data/.lockcheck.db', isolation_level=None)
c.execute('pragma journal_mode=wal'); c.execute('create table if not exists t(x)')
c.execute('begin exclusive'); open('/data/.lockcheck.held', 'w').close(); time.sleep(6)
c.execute('commit')"
docker compose exec -T worker python -c "
import os, sqlite3, sys, time
deadline = time.monotonic() + 5
while not os.path.exists('/data/.lockcheck.held'):
    if time.monotonic() > deadline: sys.exit('FAIL lock holder never started')
    time.sleep(0.1)
c = sqlite3.connect('/data/.lockcheck.db', timeout=0.5, isolation_level=None)
try:
    c.execute('begin immediate')
except sqlite3.OperationalError as e:
    print('PASS second writer blocked:', e)
else:
    sys.exit('FAIL worker took a write lock the api holds: /data must not be a bind mount')
"

passed=1
echo "=== COMPOSE SMOKE OK ==="
