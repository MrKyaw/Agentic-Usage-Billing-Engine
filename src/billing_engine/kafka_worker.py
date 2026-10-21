"""Kafka consumer worker — consumes the stream and applies the idempotent processor.

In the streaming runtime the API writes the raw log and publishes to
``usage-events``; this worker consumes that raw topic and writes rated
events/aggregates to the same store the HTTP API queries. The ledger's
conditional insert on ``event_id`` is the authoritative dedupe; Flink maintains a
separate deduped analytics read model. Offsets are committed only after the
ledger insert, so a crash re-delivers the message and the conditional insert
absorbs the duplicate.

Run locally:  python -m billing_engine.kafka_worker
              python -m billing_engine.worker  (same loop once
              BILLING_KAFKA_BOOTSTRAP is set — the unified worker entry point)
Docker:       docker-compose.yml, streaming profile (services ``api`` +
              ``worker``; the worker consumes Kafka because the deployment sets
              BILLING_KAFKA_BOOTSTRAP)
"""
from __future__ import annotations

import argparse
from typing import Any

from billing_engine.kafka_transport import KafkaEventBus, get_event_bus
from billing_engine.observability import jlog
from billing_engine.processor import process_event
from billing_engine.store import Store, get_store


def handle_event(store: Store, payload: dict[str, Any]) -> str:
    """Return ``accepted``/``duplicate``/``dlq`` for the transport to commit."""
    status, reason = process_event(store, payload)
    if status == "dlq":
        jlog("ERROR", "kafka_event_dlq", event_id=payload.get("event_id"),
             reason=reason)
    return status


def run(bus: KafkaEventBus | None = None, store: Store | None = None,
        max_messages: int | None = None, max_idle_polls: int | None = None,
        timeout_s: float = 1.0) -> dict[str, int]:
    bus = bus if bus is not None else get_event_bus()
    store = store if store is not None else get_store()
    jlog("INFO", "kafka_worker_started", topic=bus.config.topic,
         group=bus.config.group_id)
    counts = bus.consume(lambda payload: handle_event(store, payload),
                         max_messages=max_messages,
                         max_idle_polls=max_idle_polls,
                         timeout_s=timeout_s)
    jlog("INFO", "kafka_worker_stopped", **counts)
    return counts


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Kafka billing worker")
    ap.add_argument("--max-messages", type=int, default=None,
                    help="stop after N messages (smoke tests)")
    ap.add_argument("--max-idle-polls", type=int, default=None,
                    help="stop after N empty polls (smoke tests)")
    ap.add_argument("--timeout-s", type=float, default=1.0)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    bus = get_event_bus()
    try:
        run(bus, max_messages=args.max_messages,
            max_idle_polls=args.max_idle_polls, timeout_s=args.timeout_s)
    except KeyboardInterrupt:  # pragma: no cover - interactive shutdown
        jlog("INFO", "kafka_worker_interrupted")
    finally:
        bus.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
