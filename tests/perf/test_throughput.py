"""PRD §6 load targets against the real stack (uvicorn API + worker processes).

PRD: ">= 500 events/sec sustained (burst to 1,000)"; SLOS.md: "ingest p99 < 200 ms".
  * capacity  — closed loop, as fast as CONCURRENCY clients can push: >= MIN_EPS
  * latency   — open loop paced at TARGET_EPS (the PRD rate); the SLO is read
                from the service itself (`ingest_ms`, the histogram SLOS.md
                names), and the end-to-end client-observed p99 is recorded and
                bounded as well, so a stalled server cannot hide behind a
                slowed-down client (coordinated omission)
  * burst     — 1,000 events as 10 concurrent batches accepted within BURST_S
Every scenario then drains through the workers and must reconcile exactly.

The client-observed number includes the load generator's own scheduling, so it
is the *diagnostic*; the pass/fail SLO is the server-side `ingest_ms` p99.

Opt-in: `pytest -m perf`. Env-tunable for slower runners: PERF_EVENTS,
PERF_CONCURRENCY, PERF_MIN_EPS, PERF_TARGET_EPS, PERF_P99_MS,
PERF_CLIENT_P99_MS, PERF_BURST_S, PERF_DRAIN_S. Results are merged into
reports/extras.json for the report.
"""
import json
import os
import statistics
import threading
import time

import httpx
import pytest
from conftest import API_KEY_HEADERS, ROOT, make_event

pytestmark = [pytest.mark.perf, pytest.mark.req("NFR-THROUGHPUT", "NFR-LATENCY")]

N_EVENTS = int(os.environ.get("PERF_EVENTS", "5000"))
CONCURRENCY = int(os.environ.get("PERF_CONCURRENCY", "16"))
MIN_EPS = float(os.environ.get("PERF_MIN_EPS", "500"))
TARGET_EPS = float(os.environ.get("PERF_TARGET_EPS", "500"))
P99_MS = float(os.environ.get("PERF_P99_MS", "200"))
# End-to-end guardrail (client-observed): catches a stalled service without
# failing on a busy load generator. The pass/fail SLO is the server-side p99.
CLIENT_P99_MS = float(os.environ.get("PERF_CLIENT_P99_MS", "600"))
BURST_S = float(os.environ.get("PERF_BURST_S", "1.0"))
DRAIN_S = float(os.environ.get("PERF_DRAIN_S", "60"))
EXTRAS = ROOT / "reports" / "extras.json"
_extras_lock = threading.Lock()


def _pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def _record(key: str, data: dict) -> None:
    with _extras_lock:
        EXTRAS.parent.mkdir(parents=True, exist_ok=True)
        cur = json.loads(EXTRAS.read_text()) if EXTRAS.exists() else {}
        cur.setdefault("perf", {})[key] = data
        EXTRAS.write_text(json.dumps(cur, indent=2))


def _fire(base_url: str, payloads: list, path: str, rate: float | None = None,
          concurrency: int = CONCURRENCY) -> tuple[list[float], list[int], float]:
    """Each thread owns a keep-alive client connected before the clock starts
    (TCP setup is not ingest latency; real producers pool). Closed loop: threads
    pull the next payload. Open loop: thread k sends payloads k, k+C, ... at
    their scheduled instants, with no central queue to fall behind."""
    n = len(payloads)
    lat, codes = [0.0] * n, [0] * n
    nxt, nxt_lock = iter(range(n)), threading.Lock()
    ready = threading.Barrier(concurrency + 1)
    t = {"start": 0.0}

    def run(k: int) -> None:
        with httpx.Client(base_url=base_url, headers=API_KEY_HEADERS, timeout=30) as c:
            c.get("/health")
            ready.wait()
            ready.wait()
            while True:
                if rate:
                    i = k
                    k += concurrency
                    if i >= n:
                        return
                    sched = t["start"] + i / rate
                    if (wait := sched - time.perf_counter()) > 0:
                        time.sleep(wait)
                else:
                    with nxt_lock:
                        i = next(nxt, None)
                    if i is None:
                        return
                    sched = time.perf_counter()
                r = c.post(path, json=payloads[i])
                lat[i], codes[i] = (time.perf_counter() - sched) * 1000, r.status_code

    threads = [threading.Thread(target=run, args=(k,)) for k in range(concurrency)]
    for th in threads:
        th.start()
    ready.wait()
    t["start"] = time.perf_counter() + 0.1
    ready.wait()
    for th in threads:
        th.join()
    return lat, codes, time.perf_counter() - t["start"]


def _latency(lat: list[float]) -> dict:
    return {"p50_ms": round(_pct(lat, 50), 2), "p95_ms": round(_pct(lat, 95), 2),
            "p99_ms": round(_pct(lat, 99), 2), "max_ms": round(max(lat), 2),
            "mean_ms": round(statistics.mean(lat), 2)}


def _settle(stack, n_customers: int) -> tuple[float, bool, int, dict]:
    t0 = time.perf_counter()
    stack.wait_drained(timeout=DRAIN_S)
    drain_s = time.perf_counter() - t0
    server = stack.client.get("/v1/ops/metrics").json()["latency_ms"].get("ingest_ms")
    recon = stack.client.get("/v1/ops/reconcile").json()
    billed = sum(
        stack.client.get(f"/v1/customers/c{k}/usage", params={"period": "2026-10"})
        .json()["lines"][0]["event_count"] for k in range(n_customers))
    return round(drain_s, 3), recon["match"], billed, server


def _events(prefix: str, n: int) -> list[dict]:
    return [make_event(f"{prefix}-{i}", customer_id=f"c{i % 7}", quantity=1.0) for i in range(n)]


def test_capacity_sustains_at_least_500_events_per_second(live_factory):
    """Closed-loop capacity: N single-event POSTs from CONCURRENCY clients at
    >= 500 ev/s, zero errors, then exact drain + reconcile."""
    stack = live_factory(workers=2, name="capacity")
    _fire(stack.base_url, _events("warm", 50), "/v1/usage-events")
    lat, codes, wall = _fire(stack.base_url, _events("cap", N_EVENTS), "/v1/usage-events")
    drain_s, recon_ok, billed, server = _settle(stack, 7)
    result = {"events": N_EVENTS, "concurrency": CONCURRENCY, "workers": 2,
              "wall_s": round(wall, 3), "throughput_eps": round(N_EVENTS / wall, 1),
              "client_latency": _latency(lat), "server_ingest_ms": server,
              "drain_after_ingest_s": drain_s, "non_2xx": sum(c != 202 for c in codes),
              "reconcile_ok": recon_ok, "billed_events": billed - 50,
              "thresholds": {"min_eps": MIN_EPS}}
    _record("capacity", result)

    assert result["non_2xx"] == 0
    assert result["throughput_eps"] >= MIN_EPS, result
    assert recon_ok and result["billed_events"] == N_EVENTS, result


def test_ingest_p99_under_200ms_at_target_rate(live_factory):
    """Open loop at the PRD rate (500 ev/s): the service's own `ingest_ms` p99
    stays under 200 ms (the SLI SLOS.md names) with workers draining
    concurrently, and the end-to-end client-observed p99 stays bounded."""
    stack = live_factory(workers=2, name="latency")
    _fire(stack.base_url, _events("warm", 50), "/v1/usage-events")
    lat, codes, wall = _fire(stack.base_url, _events("lat", N_EVENTS), "/v1/usage-events",
                             rate=TARGET_EPS, concurrency=max(CONCURRENCY, 64))
    drain_s, recon_ok, billed, server = _settle(stack, 7)
    result = {"events": N_EVENTS, "target_eps": TARGET_EPS,
              "achieved_eps": round(N_EVENTS / wall, 1), "workers": 2,
              "client_latency": _latency(lat), "server_ingest_ms": server,
              "drain_after_ingest_s": drain_s, "non_2xx": sum(c != 202 for c in codes),
              "reconcile_ok": recon_ok, "billed_events": billed - 50,
              "thresholds": {"p99_ms": P99_MS, "client_p99_ms": CLIENT_P99_MS}}
    _record("latency", result)

    assert result["non_2xx"] == 0
    assert result["achieved_eps"] >= 0.95 * TARGET_EPS, result
    assert result["server_ingest_ms"]["p99"] < P99_MS, result
    assert result["client_latency"]["p99_ms"] < CLIENT_P99_MS, result
    assert recon_ok and result["billed_events"] == N_EVENTS, result


def test_burst_of_1000_events_accepted_within_one_second(live_factory):
    """Burst: 1,000 events (10 concurrent batches of 100) accepted in <= 1 s."""
    stack = live_factory(workers=2, name="burst")
    batches = [{"events": _events(f"b{b}", 100)} for b in range(10)]
    for b, batch in enumerate(batches):
        for i, ev in enumerate(batch["events"]):
            ev["customer_id"] = f"c{(b * 100 + i) % 7}"
    lat, codes, wall = _fire(stack.base_url, batches, "/v1/usage-events:batch")
    drain_s, recon_ok, billed, _ = _settle(stack, 7)
    result = {"events": 1000, "batches": 10, "wall_s": round(wall, 3),
              "burst_eps": round(1000 / wall, 1), "max_batch_ms": round(max(lat), 2),
              "drain_after_ingest_s": drain_s, "reconcile_ok": recon_ok,
              "billed_events": billed, "thresholds": {"burst_s": BURST_S}}
    _record("burst", result)

    assert all(c == 202 for c in codes)
    assert wall <= BURST_S, result
    assert recon_ok and billed == 1000, result
