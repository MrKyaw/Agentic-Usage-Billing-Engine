# Runbook — top 3 failure modes (one page)

## 1. Reconciliation mismatch (stored ≠ recomputed)
**Signal:** `GET /v1/ops/reconcile` returns `match:false`; alarm `ReconMismatch`.
**Triage (5 min):**
1. Scope it: `reconcile?customer=X&period=YYYY-MM` — one customer or global?
2. Check deploys: did `plan_version` change mid-period? (`events.plan_version` mixed = expected; re-rate at close.)
3. Check DLQ depth — unprocessed messages explain shortfalls, not overcounts.
**Fix:** aggregates are derived — rebuild from raw log:
`python -m billing_engine.reconcile --period YYYY-MM` confirms; re-run processor
replay from S3 prefix `dt=...`. Never hand-edit aggregates.
**Escalate:** overcount (stored > recomputed) = correctness incident → page.

## 2. Consumer lag / queue age rising
**Signal:** `queue_age > 30s` alarm; dashboard `event_to_aggregate p95 > 5s`.
**Triage:** `GET /v1/ops/metrics` → `queue.pending` growing? DLQ growing?
- Pending growing, no errors → throughput: raise Processor reserved concurrency
  (CDK `reserved_concurrent_executions`) or batch size; check DDB throttles.
- Errors growing → see mode 3.
**Fix:** queue buffers hours of ingest; scale consumer, drain, verify lag → 0,
then reconcile. Ingest stays up (degraded, not down).

## 3. DLQ filling (poison messages)
**Signal:** `DLQDepthPositive` pages immediately.
**Triage:** inspect DLQ `reason`: `unratable: unknown metric` = producer sent a
metric with no price plan (most common); `max_retries_exceeded` = transient DDB
throttle or bug.
**Fix:** unknown metric → add price plan (new `plan_version`), then redrive:
`redrive_dlq(store)` (processor) moves DLQ rows back with a fresh retry budget —
idempotent, so already-billed redeliveries drop as duplicates and still-poison
events re-DLQ bounded-ly. Throttle → back off, redrive. Then drain + reconcile.
**Never:** delete DLQ without redrive + reconcile.
