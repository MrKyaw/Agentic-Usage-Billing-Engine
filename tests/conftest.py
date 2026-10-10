import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest
from hypothesis import HealthCheck, settings

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from billing_engine.store import Store, reset_store_cache  # noqa: E402

API_KEY_HEADERS = {"X-API-Key": "dev-key-123"}

settings.register_profile("default", deadline=None, max_examples=60,
                          suppress_health_check=[HealthCheck.too_slow])
settings.register_profile("ci", parent=settings.get_profile("default"), max_examples=150)
settings.register_profile("quick", parent=settings.get_profile("default"), max_examples=15)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))


def make_event(event_id: str, customer_id: str = "acme", metric: str = "cloud_credits",
               quantity: float = 10.0, timestamp: str = "2026-10-09T10:00:00Z",
               product_id: str = "design-suite") -> dict:
    return {"event_id": event_id, "customer_id": customer_id, "product_id": product_id,
            "metric": metric, "quantity": quantity, "timestamp": timestamp}


def pytest_collection_modifyitems(items):
    """Expose PRD requirement IDs, test kind and docstring to the JUnit XML so
    tools/test_report.py can build the traceability matrix."""
    kinds = ("e2e", "perf", "property", "concurrency")
    for item in items:
        reqs = [r for m in item.iter_markers("req") for r in m.args]
        item.user_properties.append(("req", ",".join(dict.fromkeys(reqs)) or "UNMAPPED"))
        kind = next((k for k in kinds if item.get_closest_marker(k)), "unit")
        item.user_properties.append(("kind", kind))
        doc = (getattr(item, "obj", None).__doc__ or "").strip().splitlines()
        if doc:
            item.user_properties.append(("doc", doc[0]))


@pytest.fixture()
def store(tmp_path):
    return Store(tmp_path / "b.db", tmp_path / "raw")


@pytest.fixture()
def default_store(tmp_path, monkeypatch):
    """Points the process-wide get_store() singleton at a tmp DB (API, MCP,
    handlers, worker and agent_demo all resolve their store this way)."""
    monkeypatch.setenv("BILLING_DB_PATH", str(tmp_path / "default.db"))
    monkeypatch.setenv("BILLING_RAW_LOG_DIR", str(tmp_path / "default-raw"))
    reset_store_cache()
    from billing_engine.store import get_store
    yield get_store()
    reset_store_cache()


# ---------------------------------------------------------------- live stack

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(fn, timeout: float = 30.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            if last:
                return last
        except httpx.TransportError:
            pass
        time.sleep(interval)
    raise AssertionError(f"timed out waiting; last={last!r}")


@dataclass
class LiveStack:
    client: httpx.Client
    base_url: str
    tmp_path: Path
    store: Store
    procs: list = field(default_factory=list)

    def drained(self):
        q = self.client.get("/v1/ops/metrics").json()["queue"]
        return q if q["pending"] == 0 else None

    def wait_drained(self, timeout: float = 60.0):
        return wait_until(self.drained, timeout=timeout)


@pytest.fixture()
def live_factory(tmp_path):
    """Start a real uvicorn API + N worker processes sharing one SQLite DB."""
    started: list[tuple[LiveStack, list]] = []

    def start(workers: int = 1, api_workers: int = 1, name: str = "live") -> LiveStack:
        base = tmp_path / name
        base.mkdir()
        port = free_port()
        env = {**os.environ, "PYTHONPATH": str(SRC),
               "BILLING_DB_PATH": str(base / "live.db"),
               "BILLING_RAW_LOG_DIR": str(base / "raw"),
               "BILLING_EXPORT_DIR": str(base / "exports")}
        logs = []
        procs = []

        def spawn(args, log_name):
            f = open(base / f"{log_name}.log", "w")
            logs.append(f)
            procs.append(subprocess.Popen([sys.executable, *args], env=env,
                                          stdout=f, stderr=subprocess.STDOUT))

        spawn(["-m", "uvicorn", "billing_engine.api:app", "--host", "127.0.0.1",
               "--port", str(port), "--log-level", "warning",
               "--workers", str(api_workers)], "api")
        for i in range(workers):
            spawn(["-m", "billing_engine.worker", "--poll-s", "0.05"], f"worker-{i}")
        url = f"http://127.0.0.1:{port}"
        client = httpx.Client(base_url=url, headers=API_KEY_HEADERS, timeout=30)
        stack = LiveStack(client, url, base, Store(base / "live.db", base / "raw"), procs)
        started.append((stack, logs))
        wait_until(lambda: client.get("/health").status_code == 200)
        return stack

    yield start

    for stack, logs in started:
        stack.client.close()
        for p in stack.procs:
            p.terminate()
        for p in stack.procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        for f in logs:
            f.close()


@pytest.fixture()
def live(live_factory):
    return live_factory(workers=1)
