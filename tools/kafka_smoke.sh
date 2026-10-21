#!/usr/bin/env bash
# End-to-end Kafka + Flink smoke test for the whole solution.
#
# Starts the unified stack (docker-compose.yml, streaming profile) — the same
# monolith that `make up` runs, with Kafka as the queue — publishes 10 unique
# events plus 2 retries, then proves three independent things:
#   1. Kafka delivered every event to the billing worker (bill + reconcile).
#   2. Flink deduped the retries into one record per event_id.
#   3. Flink emitted a 10-second operational aggregate.
#
# It uses its own Compose project, ports and data dir, and tears the stack down
# on exit, so it never touches a running `make up` / `make up-streaming`.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-billing-stream-smoke}"
export PORT="${STREAM_PORT:-18000}"
export KAFKA_PORT="${KAFKA_PORT:-29092}"
export FLINK_PORT="${FLINK_PORT:-8081}"
# Kafka mode: the API publishes to Kafka and the worker consumes the topic.
export BILLING_KAFKA_BOOTSTRAP="${BILLING_KAFKA_BOOTSTRAP:-kafka:9092}"
DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/billing-stream-smoke.XXXXXX")"
export DATA_DIR
mkdir -p "$DATA_DIR/exports"
KEY="${BILLING_API_KEY:-dev-key-123}"
BASE="http://localhost:${PORT}"
FLINK_UI="http://localhost:${FLINK_PORT}"
COMPOSE=(docker compose --profile streaming)
passed=0

cleanup() {
  if [ "$passed" != 1 ]; then
    echo "--- streaming logs (last 120 lines)"
    "${COMPOSE[@]}" logs --no-color --tail 120 || true
  fi
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "--- docker compose up (project=$COMPOSE_PROJECT_NAME port=$PORT kafka=$KAFKA_PORT flink=$FLINK_PORT)"
"${COMPOSE[@]}" up -d --build

echo "--- wait for API health"
for _ in $(seq 1 90); do
  if curl -fsS "$BASE/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "$BASE/health" >/dev/null

echo "--- wait for both Flink jobs to reach RUNNING"
flink_ready=0
for _ in $(seq 1 120); do
  if curl -fsS "$FLINK_UI/jobs/overview" 2>/dev/null | python3 -c '
import json
import sys

try:
    data = json.load(sys.stdin)
except Exception:
    raise SystemExit(1)
running = {j.get("name", "") for j in data.get("jobs", [])
           if j.get("state") == "RUNNING"}
ok = (any("usage_events_deduped" in n for n in running)
      and any("usage_window_aggregates" in n for n in running))
raise SystemExit(0 if ok else 1)
'; then
    flink_ready=1
    break
  fi
  sleep 2
done
if [ "$flink_ready" != 1 ]; then
  echo "FAIL Flink jobs did not reach RUNNING"
  curl -sS "$FLINK_UI/jobs/overview" || true
  exit 1
fi
echo "PASS both Flink jobs are RUNNING"

echo "--- publish 10 unique events + 2 duplicate retries"
PERIOD="$(python3 - "$BASE" "$KEY" <<'PY'
import json
import sys
import urllib.request
from datetime import datetime, timezone

base, key = sys.argv[1:3]
# Pin every event to one minute and the current billing period.
ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
stamp = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
period = ts.strftime("%Y-%m")

def post(event):
    body = json.dumps(event).encode()
    req = urllib.request.Request(base + "/v1/usage-events", data=body,
                                 method="POST",
                                 headers={"Content-Type": "application/json",
                                          "X-API-Key": key})
    with urllib.request.urlopen(req, timeout=10) as r:
        assert r.status == 202, r.status

for i in range(1, 11):
    post({"event_id": f"stream-{i:03d}", "customer_id": "acme",
          "product_id": "design-suite", "metric": "cloud_credits",
          "quantity": 1.0, "timestamp": stamp})
for _ in range(2):
    post({"event_id": "stream-001", "customer_id": "acme",
          "product_id": "design-suite", "metric": "cloud_credits",
          "quantity": 1.0, "timestamp": stamp})

print(period)
PY
)"
echo "published 12 events in period $PERIOD"

echo "--- wait for the Kafka worker to rate all 10 unique events"
python3 - "$BASE" "$KEY" "$PERIOD" <<'PY'
import json
import sys
import time
import urllib.request

base, key, period = sys.argv[1:4]
deadline = time.monotonic() + 120
last = None
while time.monotonic() < deadline:
    req = urllib.request.Request(
        f"{base}/v1/customers/acme/bill?period={period}",
        headers={"X-API-Key": key})
    with urllib.request.urlopen(req, timeout=10) as r:
        bill = json.load(r)
    lines = bill.get("lines", [])
    last = (bill.get("total_amount"), lines)
    if (bill.get("total_amount") == 1.0 and len(lines) == 1
            and lines[0].get("event_count") == 10):
        print(f"PASS bill total=1.0 event_count=10")
        break
    time.sleep(1)
else:
    sys.exit(f"FAIL bill never converged: {last}")

req = urllib.request.Request(f"{base}/v1/ops/reconcile?period={period}",
                             headers={"X-API-Key": key})
with urllib.request.urlopen(req, timeout=10) as r:
    rec = json.load(r)
assert rec["match"] is True, rec
print("PASS reconcile match (raw log -> Kafka worker -> aggregates)")
PY

echo "--- wait for Flink dedupe of the two retries"
DEDUPE_COUNT="$("${COMPOSE[@]}" exec -T api python - <<'PY'
import json
import sys
import time
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": f"smoke-dedupe-{time.time_ns()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["usage-events-deduped"])
seen = {}
deadline = time.monotonic() + 120
try:
    while time.monotonic() < deadline and len(seen) < 10:
        msg = consumer.poll(1.0)
        if msg is None or msg.error() or msg.value() is None:
            continue
        event = json.loads(msg.value())
        seen[event["event_id"]] = event
finally:
    consumer.close()
print(len(seen))
PY
)"
if [ "$DEDUPE_COUNT" != "10" ]; then
  echo "FAIL Flink deduped topic has $DEDUPE_COUNT unique event ids (want 10)"
  exit 1
fi
echo "PASS Flink deduped topic has 10 unique event ids (12 raw events)"

echo "--- wait for Flink 10-second operational aggregate"
AGG_TOTAL="$("${COMPOSE[@]}" exec -T api python - <<'PY'
import json
import time
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": f"smoke-agg-{time.time_ns()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["usage-window-aggregates"])
total = 0.0
deadline = time.monotonic() + 120
try:
    while time.monotonic() < deadline and total < 12.0:
        msg = consumer.poll(1.0)
        if msg is None or msg.error() or msg.value() is None:
            continue
        agg = json.loads(msg.value())
        total += float(agg.get("total_quantity", 0))
finally:
    consumer.close()
print(total)
PY
)"
if ! python3 -c "import sys; sys.exit(0 if float('$AGG_TOTAL') >= 12.0 else 1)"; then
  echo "FAIL Flink aggregate total is $AGG_TOTAL (want >= 12.0 for 12 raw events)"
  exit 1
fi
echo "PASS Flink aggregate saw all 12 raw events (total_quantity=$AGG_TOTAL)"

echo "--- Flink jobs"
"${COMPOSE[@]}" exec -T jobmanager /opt/flink/bin/flink list

passed=1
echo "=== KAFKA + FLINK SMOKE OK ==="
