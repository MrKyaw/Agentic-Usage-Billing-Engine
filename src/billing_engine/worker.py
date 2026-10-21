"""Queue worker — one entry point for both transports.

Default (SQLite inbox, the monolith path): poll the inbox, like SQS → Lambda
event source mapping.

Streaming tier (Kafka): with ``BILLING_KAFKA_BOOTSTRAP`` set, the API publishes
to ``usage-events`` instead of writing an inbox row, so the worker consumes that
topic and applies the same idempotent processor. The API switches on the very
same variable, so a deployment only has to set it once.

Both paths write to the same ledger, and the ledger's conditional insert on
``event_id`` stays the authoritative dedupe.
"""
from __future__ import annotations

import argparse
import os
import time

from billing_engine.observability import jlog
from billing_engine.processor import process_batch
from billing_engine.store import get_store


def kafka_enabled() -> bool:
    """True when this deployment is wired to Kafka instead of the SQLite inbox."""
    return bool(os.environ.get("BILLING_KAFKA_BOOTSTRAP", "").strip())


def main(poll_s: float = 0.5, batch: int = 50, max_idle_polls: int | None = None) -> None:
    """Poll forever by default; with `max_idle_polls`, exit after that many
    consecutive empty polls (used by tests and one-shot backfills).

    In Kafka mode the same bounds apply to the consumer loop: it stops after
    `max_idle_polls` empty polls.
    """
    if kafka_enabled():
        from billing_engine import kafka_worker
        kafka_worker.run(max_idle_polls=max_idle_polls)
        return
    store = get_store()
    jlog("INFO", "worker_started")
    idle = 0
    while max_idle_polls is None or idle < max_idle_polls:
        r = process_batch(store, limit=batch)
        if r["accepted"] + r["duplicate"] + r["dlq"] == 0:
            idle += 1
            time.sleep(poll_s)
        else:
            idle = 0
            if store.pending_count() == 0:
                time.sleep(poll_s)
    jlog("INFO", "worker_stopped", idle_polls=idle)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--poll-s", type=float, default=0.5)
    ap.add_argument("--batch", type=int, default=50)
    ap.add_argument("--max-idle-polls", type=int, default=None)
    a = ap.parse_args()
    main(poll_s=a.poll_s, batch=a.batch, max_idle_polls=a.max_idle_polls)
