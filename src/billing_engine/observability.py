"""Structured JSON logs + in-memory SLI counters — FR6.

Locally this is in-process; in AWS each function emits the same JSON schema to
CloudWatch Logs and Embedded Metric Format to CloudWatch Metrics (see
infra/cdk dashboard + alarms). Correlation ID is always `event_id`.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from collections import defaultdict
from typing import Any


def jlog(level: str, msg: str, **fields: Any) -> None:
    rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "level": level, "msg": msg, **fields}
    sys.stdout.write(json.dumps(rec) + "\n")
    sys.stdout.flush()


class Metrics:
    """Thread-safe counters + latency samples (p50/p95/p99 computed on read)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counters: dict[str, int] = defaultdict(int)
        self.latencies_ms: dict[str, list[float]] = defaultdict(list)
        self.gauges: dict[str, float] = {}
        self.start = time.time()

    def inc(self, name: str, n: int = 1) -> None:
        with self._lock:
            self.counters[name] += n

    def observe_latency(self, name: str, ms: float) -> None:
        with self._lock:
            samples = self.latencies_ms[name]
            samples.append(ms)
            if len(samples) > 5000:
                del samples[: len(samples) - 5000]

    def set_gauge(self, name: str, v: float) -> None:
        with self._lock:
            self.gauges[name] = v

    @staticmethod
    def _pct(samples: list[float], p: float) -> float | None:
        if not samples:
            return None
        s = sorted(samples)
        idx = min(len(s) - 1, int(p * len(s)))
        return round(s[idx], 2)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counters = dict(self.counters)
            gauges = dict(self.gauges)
            lat = {k: {"p50": self._pct(v, 0.50), "p95": self._pct(v, 0.95),
                       "p99": self._pct(v, 0.99), "count": len(v)}
                   for k, v in self.latencies_ms.items()}
        dup = counters.get("processor_duplicate_total", 0)
        ok = counters.get("processor_accepted_total", 0)
        return {"uptime_s": round(time.time() - self.start, 1),
                "counters": counters,
                "gauges": gauges,
                "latency_ms": lat,
                "derived": {"duplicate_rate": round(dup / max(1, dup + ok), 4)}}

    def check_alarms(self, queue_age_s: float = 0.0, dlq_depth: int = 0) -> list[dict[str, Any]]:
        snap = self.snapshot()
        errs = snap["counters"].get("processor_error_total", 0)
        total = errs + snap["counters"].get("processor_accepted_total", 0)
        err_rate = errs / max(1, total)
        alarms = []
        if dlq_depth > 0:
            alarms.append({"alarm": "DLQDepthPositive", "severity": "page",
                           "detail": f"dlq_depth={dlq_depth}"})
        if queue_age_s > 30:
            alarms.append({"alarm": "QueueAgeHigh", "severity": "ticket",
                           "detail": f"oldest_pending_age_s={queue_age_s:.1f} > 30"})
        if err_rate > 0.01 and total >= 20:
            alarms.append({"alarm": "ErrorRateHigh", "severity": "ticket",
                           "detail": f"error_rate={err_rate:.3f} > 0.01"})
        return alarms


METRICS = Metrics()
