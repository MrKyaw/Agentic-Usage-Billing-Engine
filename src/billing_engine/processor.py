"""Idempotent, resilient processor — FR2 + FR3 + FR4.

At-least-once delivery + conditional-put dedupe = exactly-once *effect*.
Order of operations per message:
  1. rate unit (pure function; unknown metric → DLQ)
  2. try_insert_event (conditional put on event_id; False → duplicate, ack, done)
  3. ack inbox row.

Key resilience features:
 - Partition key (event_id) ordering: redelivered events are bucketed by
   event_id hash to avoid hot-key contention on DynamoDB.
 - Deterministic rating: rate_unit is pure (no side effects); rate_period
   consumes allowances/tiers at period boundaries.
 - Idempotent state reconciliation: reconcile() replays the raw log and
   compares to aggregates; any nonzero diff is a correctness incident.
 - Poison (attempts > max_retries or un-ratable) → DLQ + alarm.
 - Batching with checkpoint (WAL passive checkpoint) so mid-process crash
   never loses committed state.
"""
from __future__ import annotations

import time
from typing import Any

from billing_engine.observability import METRICS, jlog
from billing_engine.rating import RatingError, get_book
from billing_engine.store import Store


def process_one(store: Store, item: dict[str, Any], max_retries: int = 5) -> str:
    """Returns 'accepted' | 'duplicate' | 'dlq'. Never raises on poison."""
    from billing_engine.config import SETTINGS
    max_retries = max_retries or SETTINGS.max_retries
    t0 = time.perf_counter()
    payload: dict[str, Any] = item["payload"]
    event_id = payload.get("event_id", "?")
    book = get_book()
    try:
        unit_amount, plan_version = book.rate_unit(payload["metric"], float(payload["quantity"]))
    except (RatingError, KeyError, TypeError, ValueError) as e:
        store.move_to_dlq(item["seq"], event_id, payload, f"unratable: {e}")
        METRICS.inc("processor_dlq_total")
        METRICS.inc("processor_error_total")
        jlog("ERROR", "processor_dlq", event_id=event_id, reason=str(e)[:200])
        return "dlq"
    ok = store.try_insert_event(payload, plan_version, unit_amount)
    ms = (time.perf_counter() - t0) * 1000
    METRICS.observe_latency("event_to_aggregate_ms", ms)
    if not ok:
        store.ack(item["seq"])
        METRICS.inc("processor_duplicate_total")
        jlog("INFO", "processor_duplicate", event_id=event_id)
        return "duplicate"
    store.ack(item["seq"])
    METRICS.inc("processor_accepted_total")
    jlog("INFO", "processor_accepted", event_id=event_id,
         customer_id=payload.get("customer_id"), amount=unit_amount,
         latency_ms=round(ms, 2))
    return "accepted"


def process_event(store: Store, payload: dict[str, Any]) -> tuple[str, str | None]:
    """Process one event from a stream (Kafka) where there is no inbox row to ack.

    Returns ``(status, reason)``: ``accepted``/``duplicate`` with ``None``, or
    ``dlq`` with the unratable reason. The caller publishes poison messages to
    the Kafka DLQ topic; the ledger insert remains the idempotency guard.
    """
    t0 = time.perf_counter()
    event_id = payload.get("event_id", "?")
    book = get_book()
    try:
        unit_amount, plan_version = book.rate_unit(payload["metric"],
                                                   float(payload["quantity"]))
    except (RatingError, KeyError, TypeError, ValueError) as e:
        METRICS.inc("processor_dlq_total")
        METRICS.inc("processor_error_total")
        jlog("ERROR", "processor_dlq", event_id=event_id, reason=str(e)[:200])
        return "dlq", f"unratable: {e}"
    ok = store.try_insert_event(payload, plan_version, unit_amount)
    ms = (time.perf_counter() - t0) * 1000
    METRICS.observe_latency("event_to_aggregate_ms", ms)
    if not ok:
        METRICS.inc("processor_duplicate_total")
        jlog("INFO", "processor_duplicate", event_id=event_id)
        return "duplicate", None
    METRICS.inc("processor_accepted_total")
    jlog("INFO", "processor_accepted", event_id=event_id,
         customer_id=payload.get("customer_id"), amount=unit_amount,
         latency_ms=round(ms, 2))
    return "accepted", None


def process_batch(store: Store, limit: int = 50) -> dict[str, int]:
    from billing_engine.config import SETTINGS
    items = store.claim_batch(limit=limit)
    out = {"accepted": 0, "duplicate": 0, "dlq": 0}
    for it in items:
        if it["attempts"] > SETTINGS.max_retries:
            store.move_to_dlq(it["seq"], it["event_id"], it["payload"], "max_retries_exceeded")
            METRICS.inc("processor_dlq_total")
            out["dlq"] += 1
            continue
        out[process_one(store, it)] += 1
    if items:
        store.checkpoint()
    METRICS.set_gauge("queue_pending", store.pending_count())
    METRICS.set_gauge("dlq_depth", store.dlq_count())
    return out


def drain(store: Store, batch: int = 200, max_rounds: int = 1000) -> dict[str, int]:
    """Process until inbox empty (used by API inline path, tests, demo)."""
    total = {"accepted": 0, "duplicate": 0, "dlq": 0}
    for _ in range(max_rounds):
        r = process_batch(store, limit=batch)
        for k in total:
            total[k] += r[k]
        if store.pending_count() == 0:
            break
    return total


def redrive_dlq(store: Store, limit: int = 500) -> int:
    """Ops recovery (RUNBOOK mode #3): replay DLQ back to the inbox after the
    cause is fixed. Crash-looped valid events bill on this pass; still-poison
    events re-DLQ boundedly."""
    n = store.replay_dlq(limit=limit)
    if n:
        METRICS.inc("processor_dlq_replay_total", n)
        METRICS.set_gauge("dlq_depth", store.dlq_count())
        jlog("INFO", "dlq_redrive", replayed=n)
    return n
