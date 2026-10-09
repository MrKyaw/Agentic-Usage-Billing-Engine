"""Queue worker — polls inbox like SQS → Lambda event source mapping."""
from __future__ import annotations

import time

from billing_engine.observability import jlog
from billing_engine.processor import process_batch
from billing_engine.store import get_store


def main(poll_s: float = 0.5, batch: int = 50) -> None:
    store = get_store()
    jlog("INFO", "worker_started")
    while True:
        r = process_batch(store, limit=batch)
        if r["accepted"] + r["duplicate"] + r["dlq"] == 0:
            time.sleep(poll_s)
        elif store.pending_count() == 0:
            time.sleep(poll_s)


if __name__ == "__main__":
    main()
