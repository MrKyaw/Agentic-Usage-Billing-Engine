"""Cross-process end-to-end: real uvicorn API + real worker process sharing one
SQLite DB, driven over HTTP exactly like a producer and a finance client."""
import shutil
from pathlib import Path

import pytest
from conftest import make_event

pytestmark = [pytest.mark.e2e,
              pytest.mark.req("FR1", "FR2", "FR3", "FR4", "FR5", "FR6", "NFR-CORRECTNESS")]


def test_full_billing_lifecycle_across_processes(live):
    """Async ingest -> worker -> bill -> reconcile -> close -> late event, over HTTP."""
    client = live.client

    # producers: single + batch, async (no ?sync) — worker process does the work
    for i in range(30):
        r = client.post("/v1/usage-events", json=make_event(f"live-{i:03d}", quantity=2))
        assert r.status_code == 202
    batch = [make_event(f"rnd-{i:03d}", customer_id="globex", metric="render_minutes",
                        quantity=60) for i in range(20)]
    r = client.post("/v1/usage-events:batch", json={"events": batch})
    assert r.status_code == 202 and r.json()["accepted_count"] == 20

    # at-least-once transport: redeliver 10 events + one poison message
    for i in range(10):
        client.post("/v1/usage-events", json=make_event(f"live-{i:03d}", quantity=2))
    client.post("/v1/usage-events", json=make_event("poison", metric="unknown_metric"))
    assert client.post("/v1/usage-events", json={"event_id": "x"}).status_code == 400

    queue = live.wait_drained()
    assert queue["dlq"] == 1

    usage = client.get("/v1/customers/acme/usage", params={"period": "2026-10"}).json()
    assert usage["lines"][0]["total_quantity"] == 60.0  # 30 unique * 2, no double count
    assert usage["lines"][0]["event_count"] == 30

    globex = client.get("/v1/customers/globex/bill", params={"period": "2026-10"}).json()
    # 1200 min - 100 included = 1100 billable: 1000@.05 + 100@.04
    assert globex["total_amount"] == 54.0 and globex["status"] == "open"

    # reconciliation over the raw log written by the API process
    rec = client.get("/v1/ops/reconcile").json()
    assert rec["match"], rec["mismatches"]
    assert rec["dlq_excluded"] == 1

    # finance closes the period → CSV export
    closed = client.post("/v1/customers/acme/close", params={"period": "2026-10"}).json()
    assert closed["bill"]["total_amount"] == 6.0 and closed["bill"]["status"] == "closed"
    csv_path = Path(closed["csv"])
    assert csv_path.parent == live.tmp_path / "exports" and "TOTAL" in csv_path.read_text()

    # late event after close: invoice stays frozen, delta is surfaced
    client.post("/v1/usage-events", json=make_event("late-1", quantity=10))
    live.wait_drained()
    bill = client.get("/v1/customers/acme/bill", params={"period": "2026-10"}).json()
    assert bill["status"] == "closed" and bill["total_amount"] == 6.0
    assert bill["post_close_delta"] == 1.0
    assert client.get("/v1/ops/reconcile").json()["match"]

    metrics = client.get("/v1/ops/metrics").json()
    assert {a["alarm"] for a in metrics["alarms"]} == {"DLQDepthPositive"}


def _clean_env(tmp_path: Path) -> dict:
    """A user's shell: no PYTHONPATH, package not pip-installed."""
    import os
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    env.update(BILLING_DB_PATH=str(tmp_path / "m.db"), BILLING_RAW_LOG_DIR=str(tmp_path / "raw"),
               BILLING_EXPORT_DIR=str(tmp_path / "exp"))
    return env


@pytest.mark.req("FR1", "FR6")
@pytest.mark.skipif(shutil.which("make") is None, reason="GNU make not installed")
def test_make_targets_run_from_a_clean_shell(tmp_path):
    """`make api` / `make reconcile` must import billing_engine without the
    caller exporting PYTHONPATH (regression: ModuleNotFoundError)."""
    import json
    import os
    import signal
    import subprocess

    import httpx
    from conftest import ROOT, free_port, wait_until

    env = _clean_env(tmp_path)
    port = free_port()
    log = open(tmp_path / "make-api.log", "w")
    proc = subprocess.Popen(["make", "-s", "api", f"PORT={port}"], cwd=ROOT, env=env,
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        wait_until(lambda: proc.poll() is not None
                   or httpx.get(f"http://127.0.0.1:{port}/health").status_code == 200,
                   timeout=30)
        assert proc.poll() is None, (tmp_path / "make-api.log").read_text()[-2000:]
        assert httpx.get(f"http://127.0.0.1:{port}/health").json()["ok"] is True
    finally:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=10)
        log.close()

    r = subprocess.run(["make", "-s", "reconcile"], cwd=ROOT, env=env,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    assert json.loads(r.stdout)["match"] is True
