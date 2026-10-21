"""Structured JSON logs + in-memory SLIs + OpenTelemetry (OTLP/HTTP) — FR6.

Locally metrics are in-process; in AWS each function emits the same JSON schema
to CloudWatch Logs and Embedded Metric Format to CloudWatch Metrics (see
infra/cdk dashboard + alarms). Correlation ID is always `event_id`.

OpenTelemetry: when ``OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`` is set, counters and
gauges are exported as OTLP/HTTP JSON (the same wire format the OpenTelemetry
Collector accepts) on every ``snapshot()``/``flush_otel()``. Export is
best-effort and bounded: a failing collector never affects billing, and after
``max_failures`` consecutive errors the bridge disables itself instead of
retrying forever. This keeps the POC dependency-free while remaining
collector-compatible (Grafana/Tempo, Jaeger, ADOT, CloudWatch).
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request
from collections import defaultdict
from typing import Any, Callable

HTTPPost = Callable[[str, bytes, dict[str, str]], int]


def _http_post(url: str, body: bytes, headers: dict[str, str]) -> int:  # pragma: no cover
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=2.0) as response:
        return int(response.status)


def jlog(level: str, msg: str, **fields: Any) -> None:
    rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "level": level, "msg": msg, **fields}
    sys.stdout.write(json.dumps(rec, default=str) + "\n")
    sys.stdout.flush()


class OtelBridge:
    """Minimal OTLP/HTTP metrics exporter (cumulative sums + gauges)."""

    def __init__(self, endpoint: str, service_name: str = "billing-engine",
                 transport: HTTPPost | None = None, max_failures: int = 3,
                 clock: Callable[[], float] = time.time) -> None:
        if not endpoint:
            raise ValueError("OTEL endpoint must be non-empty")
        if max_failures < 1:
            raise ValueError("max_failures must be >= 1")
        endpoint = endpoint.rstrip("/")
        self.url = endpoint if endpoint.endswith("/v1/metrics") else endpoint + "/v1/metrics"
        self.service_name = service_name
        self.transport = transport or _http_post
        self.max_failures = max_failures
        self.clock = clock
        self.enabled = True
        self.failures = 0
        self.last_status: int | None = None

    def payload(self, counters: dict[str, int], gauges: dict[str, float],
                start_ns: int) -> dict[str, Any]:
        now = int(self.clock() * 1_000_000_000)
        metrics: list[dict[str, Any]] = []
        for name, value in sorted(counters.items()):
            metrics.append({
                "name": name,
                "sum": {
                    "dataPoints": [{"asInt": str(int(value)),
                                    "startTimeUnixNano": str(start_ns),
                                    "timeUnixNano": str(now)}],
                    "aggregationTemporality": 2,  # CUMULATIVE
                    "isMonotonic": True,
                },
            })
        for name, value in sorted(gauges.items()):
            metrics.append({
                "name": name,
                "gauge": {"dataPoints": [{"asDouble": float(value),
                                          "timeUnixNano": str(now)}]},
            })
        return {
            "resourceMetrics": [{
                "resource": {"attributes": [{"key": "service.name",
                                             "value": {"stringValue": self.service_name}}]},
                "scopeMetrics": [{"scope": {"name": "billing-engine"},
                                  "metrics": metrics}],
            }],
        }

    def export(self, counters: dict[str, int], gauges: dict[str, float],
               start_ns: int) -> bool:
        if not self.enabled:
            return False
        body = json.dumps(self.payload(counters, gauges, start_ns)).encode("utf-8")
        try:
            status = int(self.transport(self.url, body,
                                        {"Content-Type": "application/json"}))
        except Exception:  # noqa: BLE001 - best effort, never break billing
            self._record_failure(None)
            return False
        if 200 <= status < 300:
            self.last_status = status
            return True
        self._record_failure(status)
        return False

    def _record_failure(self, status: int | None) -> None:
        self.last_status = status
        self.failures += 1
        if self.failures >= self.max_failures:
            self.enabled = False


class Metrics:
    """Thread-safe counters + latency samples (p50/p95/p99 computed on read)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counters: dict[str, int] = defaultdict(int)
        self.latencies_ms: dict[str, list[float]] = defaultdict(list)
        self.gauges: dict[str, float] = {}
        self.start = time.time()
        self.otel: OtelBridge | None = None
        self._otel_dirty = False
        self._configure_otel()

    def _configure_otel(self) -> None:
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
        if not endpoint:
            return
        self.enable_otel(endpoint,
                         service_name=os.environ.get("OTEL_SERVICE_NAME", "billing-engine"))

    def enable_otel(self, endpoint: str, transport: HTTPPost | None = None,
                    service_name: str = "billing-engine") -> OtelBridge:
        self.otel = OtelBridge(endpoint, service_name=service_name, transport=transport)
        return self.otel

    def flush_otel(self) -> bool:
        """Export the current counters/gauges if OTEL is on and state changed."""
        if self.otel is None:
            return False
        with self._lock:
            if not self._otel_dirty:
                return False
            self._otel_dirty = False
            counters = dict(self.counters)
            gauges = dict(self.gauges)
            start_ns = int(self.start * 1_000_000_000)
        return self.otel.export(counters, gauges, start_ns)

    def inc(self, name: str, n: int = 1) -> None:
        with self._lock:
            self.counters[name] += n
            self._otel_dirty = True

    def observe_latency(self, name: str, ms: float) -> None:
        with self._lock:
            samples = self.latencies_ms[name]
            samples.append(ms)
            if len(samples) > 5000:
                del samples[: len(samples) - 5000]

    def set_gauge(self, name: str, v: float) -> None:
        with self._lock:
            self.gauges[name] = v
            self._otel_dirty = True

    @staticmethod
    def _pct(samples: list[float], p: float) -> float | None:
        if not samples:
            return None
        s = sorted(samples)
        idx = min(len(s) - 1, int(p * len(s)))
        return round(s[idx], 2)

    def snapshot(self) -> dict[str, Any]:
        self.flush_otel()  # scrape = natural export point
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
                "otel": {"enabled": self.otel.enabled if self.otel else False,
                         "failures": self.otel.failures if self.otel else 0,
                         "last_status": self.otel.last_status if self.otel else None},
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
