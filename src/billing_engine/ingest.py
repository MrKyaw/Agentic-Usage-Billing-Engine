"""Ingest service — FR1 (API Gateway → Lambda → SQS analogue).

Validation errors → caller gets 4xx (no queue write). Accepted events are
durably logged + enqueued in ONE transaction, then 202 is returned. Ingest never
rates or aggregates (thin, fast, p99 < 200ms target).
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from billing_engine.models import UsageEvent
from billing_engine.observability import METRICS, jlog
from billing_engine.store import Store

if TYPE_CHECKING:  # pragma: no cover - typing-only protocol
    from typing import Protocol

    class EventPublisher(Protocol):
        """Minimal publisher contract (KafkaEventBus satisfies it)."""

        def publish(self, event: dict[str, Any]) -> bool: ...


class IngestRejected(ValueError):
    pass


def _publish(publisher: EventPublisher | None, event: dict[str, Any]) -> None:
    """Publish after the event is durable; a broker failure must never fail ingest."""
    if publisher is None:
        return
    try:
        publisher.publish(event)
    except Exception as e:  # noqa: BLE001 - raw log is already durable
        METRICS.inc("ingest_publish_failed_total")
        jlog("ERROR", "ingest_publish_failed", event_id=event.get("event_id"),
             error=str(e)[:200])


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


def ingest_one(store: Store, payload: dict[str, Any],
               publisher: EventPublisher | None = None,
               enqueue: bool = True) -> dict[str, Any]:
    t0 = time.perf_counter()
    norm = _validate(payload)
    if enqueue:
        store.enqueue(norm)
    else:
        # Kafka mode: Kafka is the queue; the raw log remains the durable source
        # of truth for audit/replay. No inbox row means reconcile sees the event
        # as expected rather than pending.
        store.append_raw_log(norm)
    ms = (time.perf_counter() - t0) * 1000
    METRICS.observe_latency("ingest_ms", ms)
    _publish(publisher, norm)
    return _accepted(norm, ms)


def ingest_batch(store: Store, events: list[dict[str, Any]],
                 publisher: EventPublisher | None = None,
                 enqueue: bool = True) -> dict[str, Any]:
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
    if enqueue:
        store.enqueue_many(valid)
    else:
        for norm in valid:
            store.append_raw_log(norm)
    ms = (time.perf_counter() - t0) * 1000
    METRICS.observe_latency("ingest_batch_ms", ms)
    for norm in valid:
        _publish(publisher, norm)
    accepted = [_accepted(n, ms) for n in valid]
    return {"accepted": accepted, "rejected": rejected,
            "accepted_count": len(accepted), "rejected_count": len(rejected)}
