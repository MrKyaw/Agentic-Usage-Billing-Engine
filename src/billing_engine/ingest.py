"""Ingest service — FR1 (API Gateway → Lambda → SQS analogue).

Validation errors → caller gets 4xx (no queue write). Accepted events are
durably logged + enqueued in ONE transaction, then 202 is returned. Ingest never
rates or aggregates (thin, fast, p99 < 200ms target).
"""
from __future__ import annotations

import time
from typing import Any

from pydantic import ValidationError

from billing_engine.models import UsageEvent
from billing_engine.observability import METRICS, jlog
from billing_engine.store import Store


class IngestRejected(ValueError):
    pass


def _norm(payload: dict[str, Any]) -> dict[str, Any]:
    ev = UsageEvent.model_validate(payload)  # may raise ValidationError
    # mode="json" renders the UTC-normalized timestamp as a canonical ISO string
    return ev.model_dump(mode="json")


def _validate(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return _norm(payload)
    except ValidationError as e:
        METRICS.inc("ingest_rejected_total")
        jlog("WARN", "ingest_rejected",
             errors=e.errors(include_url=False, include_context=False,
                             include_input=False)[:3])
        raise IngestRejected(e.json())


def _accepted(norm: dict[str, Any], ms: float) -> dict[str, Any]:
    METRICS.inc("ingest_accepted_total")
    jlog("INFO", "ingest_accepted", event_id=norm["event_id"],
         customer_id=norm["customer_id"], latency_ms=round(ms, 2))
    return {"event_id": norm["event_id"], "status": "accepted"}


def ingest_one(store: Store, payload: dict[str, Any]) -> dict[str, Any]:
    t0 = time.perf_counter()
    norm = _validate(payload)
    store.enqueue(norm)
    ms = (time.perf_counter() - t0) * 1000
    METRICS.observe_latency("ingest_ms", ms)
    return _accepted(norm, ms)


def ingest_batch(store: Store, events: list[dict[str, Any]]) -> dict[str, Any]:
    """Each event is validated on its own (partial accept); all valid events are
    then logged + enqueued in one transaction, i.e. one write-lock acquisition."""
    from billing_engine.config import SETTINGS
    if len(events) > SETTINGS.max_batch:
        raise IngestRejected(f"batch exceeds {SETTINGS.max_batch}")
    t0 = time.perf_counter()
    valid, rejected = [], []
    for p in events:
        try:
            valid.append(_validate(p))
        except IngestRejected as e:
            rejected.append({"payload_event_id": p.get("event_id"), "error": str(e)[:300]})
    store.enqueue_many(valid)
    ms = (time.perf_counter() - t0) * 1000
    METRICS.observe_latency("ingest_batch_ms", ms)
    accepted = [_accepted(n, ms) for n in valid]
    return {"accepted": accepted, "rejected": rejected,
            "accepted_count": len(accepted), "rejected_count": len(rejected)}
