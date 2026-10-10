#!/usr/bin/env python3
"""Run every verification layer and write markdown test reports.

  python tools/test_report.py            pytest (unit, property, concurrency, e2e) + demo + chaos
  python tools/test_report.py --perf     ... plus the perf suite and a headless locust run
  python tools/test_report.py --no-run   regenerate markdown from existing reports/ artifacts

Writes reports/TEST_RESULTS.md, reports/suites/<test file>.md and docs/TEST_PLAN.md.
Raw artifacts (junit.xml, coverage.json, extras.json, locust CSVs, logs/) are gitignored.
Exit code is non-zero if any layer failed, so CI can gate on it.
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
import os
import platform
import socket
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
LOGS = REPORTS / "logs"
SUITES = REPORTS / "suites"
JUNIT = REPORTS / "junit.xml"
JUNIT_PERF = REPORTS / "junit-perf.xml"
COVERAGE = REPORTS / "coverage.json"
EXTRAS = REPORTS / "extras.json"
PLAN = ROOT / "docs" / "TEST_PLAN.md"
PY = sys.executable
API_KEY = "dev-key-123"

REQUIREMENTS = {
    "FR1": ("Usage event ingestion",
            "POST single + batch (<=100), schema validation, 4xx on malformed, 202 on accept"),
    "FR2": ("Idempotent processing",
            "Dedupe on event_id, safe retries/redelivery, poison -> DLQ, event-time bucketing"),
    "FR3": ("Rating", "Config-driven flat/tiered/allowance plans, deterministic, versioned"),
    "FR4": ("Aggregation & billing summary",
            "Atomic per customer x period x metric totals, usage/bill APIs, period close + CSV"),
    "FR5": ("Audit & reconciliation", "Immutable raw log; recompute from raw and diff aggregates"),
    "FR6": ("Observability & operability", "SLI metrics, structured JSON logs, alarms"),
    "FR7": ("Agentic interface (MCP)",
            "Read-only MCP tools get_usage/get_bill/reconcile/explain_charge + agent demo"),
    "NFR-THROUGHPUT": ("Throughput", ">= 500 events/s sustained, burst to 1,000"),
    "NFR-LATENCY": ("Latency", "Ingest p99 < 200 ms; event-to-aggregate p95 < 5 s"),
    "NFR-CORRECTNESS": ("Correctness",
                        "0 double-counts under 10% duplicates; 0 lost events under failures"),
    "NFR-SECURITY": ("Security", "API key auth, no PII in logs"),
}
KINDS = {
    "unit": "Fast in-process tests of one component or API route",
    "property": "Hypothesis property-based + stateful (random streams, crash points, invariants)",
    "concurrency": "Many threads / processes sharing one database",
    "e2e": "Real uvicorn API + worker processes over HTTP",
    "perf": "Load against the live stack with PRD thresholds (opt-in)",
}


# ------------------------------------------------------------------ running

def _env() -> dict[str, str]:
    # scripts call bare `python3`; make it resolve to this interpreter
    return {**os.environ, "PYTHONPATH": str(ROOT / "src"),
            "PATH": f"{Path(PY).parent}{os.pathsep}{os.environ.get('PATH', '')}"}


def _run(cmd: list[str], log: Path, env: dict[str, str] | None = None) -> tuple[int, float]:
    LOGS.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    with open(log, "w") as f:
        rc = subprocess.call(cmd, cwd=ROOT, env=env or _env(), stdout=f, stderr=subprocess.STDOUT)
    return rc, round(time.perf_counter() - t0, 2)


def run_pytest() -> dict:
    cmd = [PY, "-m", "pytest", "-q", f"--junitxml={JUNIT}", "--cov",
           f"--cov-report=json:{COVERAGE}", "--cov-report=term", "--cov-fail-under=100"]
    rc, secs = _run(cmd, LOGS / "pytest.log")
    return {"rc": rc, "seconds": secs, "cmd": "pytest " + " ".join(cmd[4:]).replace(str(ROOT) + "/", "")}


def run_perf() -> dict:
    # separate process without coverage: tracing slows the load-generating client
    cmd = [PY, "-m", "pytest", "-q", "-m", "perf", f"--junitxml={JUNIT_PERF}"]
    rc, secs = _run(cmd, LOGS / "pytest-perf.log")
    return {"rc": rc, "seconds": secs, "cmd": "pytest " + " ".join(cmd[4:]).replace(str(ROOT) + "/", "")}


def run_script(name: str) -> dict:
    rc, secs = _run(["bash", f"tools/{name}.sh"], LOGS / f"{name}.log")
    keep = ("PASS", "COMPLETE", "reconcile match", "mid-crash", "recovery", "post-dupe")
    lines = (LOGS / f"{name}.log").read_text(errors="replace").splitlines()
    return {"rc": rc, "seconds": secs,
            "highlights": [ln.strip() for ln in lines if any(k in ln for k in keep)][:12]}


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"X-API-Key": API_KEY})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def run_locust(duration: str, users: int) -> dict:
    """Headless locust against a fresh API + worker; then drain and reconcile."""
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="locust-"))
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = {**_env(), "BILLING_DB_PATH": str(tmp / "l.db"),
           "BILLING_RAW_LOG_DIR": str(tmp / "raw"), "BILLING_EXPORT_DIR": str(tmp / "exp")}
    base = f"http://127.0.0.1:{port}"
    logs = [open(LOGS / "locust-api.log", "w"), open(LOGS / "locust-worker.log", "w")]
    procs = [subprocess.Popen([PY, "-m", "uvicorn", "billing_engine.api:app", "--port",
                               str(port), "--log-level", "warning"], cwd=ROOT, env=env,
                              stdout=logs[0], stderr=subprocess.STDOUT),
             subprocess.Popen([PY, "-m", "billing_engine.worker", "--poll-s", "0.05"],
                              cwd=ROOT, env=env, stdout=logs[1], stderr=subprocess.STDOUT)]
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/health", timeout=1)
                break
            except OSError:
                time.sleep(0.1)
        rc, secs = _run([PY, "-m", "locust", "-f", "loadtest/locustfile.py", "--headless",
                         "-u", str(users), "-r", str(max(1, users // 2)), "-t", duration,
                         "--host", base, "--csv", str(REPORTS / "locust"), "--only-summary"],
                        LOGS / "locust.log", env)
        t0 = time.perf_counter()
        while _get(f"{base}/v1/ops/metrics")["queue"]["pending"] and time.perf_counter() - t0 < 180:
            time.sleep(0.5)
        drain_s = round(time.perf_counter() - t0, 2)
        rec = _get(f"{base}/v1/ops/reconcile")
        metrics = _get(f"{base}/v1/ops/metrics")
    finally:
        for p in procs:
            p.terminate()
            p.wait(timeout=10)
        for f in logs:
            f.close()
    rows = list(csv.DictReader(open(REPORTS / "locust_stats.csv")))
    endpoints = [{
        "name": f'{r["Type"]} {r["Name"]}'.strip(), "requests": int(r["Request Count"]),
        "failures": int(r["Failure Count"]), "rps": round(float(r["Requests/s"]), 1),
        "p50_ms": float(r["50%"]), "p95_ms": float(r["95%"]), "p99_ms": float(r["99%"]),
        "max_ms": round(float(r["Max Response Time"]), 1)} for r in rows]
    by = {e["name"]: e for e in endpoints}
    single = by.get("POST /v1/usage-events", {}).get("rps", 0)
    batch = by.get("POST /v1/usage-events:batch", {}).get("rps", 0)
    return {"rc": rc, "seconds": secs, "duration": duration, "users": users,
            "endpoints": endpoints, "ingest_events_per_s": round(single + 20 * batch, 1),
            "drain_after_s": drain_s, "reconcile_match": rec["match"],
            "events_reconciled": rec["raw_events_scanned"],
            "duplicates": metrics["counters"].get("processor_duplicate_total", 0),
            "dlq": metrics["queue"]["dlq"]}


def _merge_extras(key: str, value: dict) -> None:
    cur = json.loads(EXTRAS.read_text()) if EXTRAS.exists() else {}
    cur[key] = value
    EXTRAS.write_text(json.dumps(cur, indent=2))


# ------------------------------------------------------------------ parsing

def _module_path(classname: str) -> tuple[str, str]:
    parts = classname.split(".")
    idx = next((i for i, p in enumerate(parts) if p.startswith("test_")), len(parts) - 1)
    return "/".join(parts[:idx + 1]) + ".py", ".".join(parts[idx + 1:])


def parse_junit(include_perf: bool) -> list[dict]:
    files = [JUNIT] + ([JUNIT_PERF] if include_perf and JUNIT_PERF.exists() else [])
    return [c for f in files for c in _parse_junit_file(f)]


def _parse_junit_file(path: Path) -> list[dict]:
    cases = []
    for tc in ET.parse(path).getroot().iter("testcase"):
        props = {p.get("name"): p.get("value") for p in tc.iter("property")}
        status, message = "passed", ""
        for tag in ("failure", "error", "skipped"):
            el = tc.find(tag)
            if el is not None:
                status = {"failure": "failed", "error": "error", "skipped": "skipped"}[tag]
                message = (el.get("message") or "") + "\n" + (el.text or "")
                break
        file, cls = _module_path(tc.get("classname", ""))
        name = tc.get("name", "")
        cases.append({
            "file": file, "cls": cls, "name": name, "time": float(tc.get("time", 0)),
            "status": status, "message": message.strip(),
            "req": [r for r in props.get("req", "UNMAPPED").split(",") if r],
            "kind": props.get("kind", "unit"), "doc": props.get("doc") or _humanize(name)})
    return cases


def _humanize(test_name: str) -> str:
    """Fallback description for tests without a docstring: the name as a sentence."""
    base, _, params = test_name.partition("[")
    words = base.removeprefix("test_").replace("_", " ").strip()
    text = words[:1].upper() + words[1:]
    return f"{text} [{params}" if params else text


def _module_doc(file: str) -> str:
    try:
        doc = ast.get_docstring(ast.parse((ROOT / file).read_text())) or ""
    except (OSError, SyntaxError):
        return ""
    return " ".join(doc.split())


def environment() -> dict:
    def git(*a: str) -> str:
        try:
            return subprocess.check_output(["git", *a], cwd=ROOT, text=True,
                                           stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            return ""
    sha = git("rev-parse", "--short", "HEAD") or "n/a (not a git checkout)"
    dirty = " (uncommitted changes)" if git("status", "--porcelain") else ""
    pkgs = {}
    for p in ("pytest", "hypothesis", "pytest-cov", "fastapi", "pydantic", "mcp", "locust"):
        try:
            pkgs[p] = metadata.version(p)
        except metadata.PackageNotFoundError:
            pkgs[p] = "-"
    return {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "git": sha + dirty, "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
            "cpus": os.cpu_count(), "packages": pkgs,
            "hypothesis_profile": os.environ.get("HYPOTHESIS_PROFILE", "default")}


# ------------------------------------------------------------------ writing

def _cell(s: object) -> str:
    return str(s).replace("|", "\\|").replace("\n", " ")


def _table(header: list[str], rows: list[list[object]]) -> list[str]:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    return out + ["| " + " | ".join(_cell(c) for c in r) + " |" for r in rows]


def _ok(flag: bool) -> str:
    return "PASS" if flag else "FAIL"


def _counts(cs: list[dict]) -> Counter:
    return Counter(c["status"] for c in cs)


def write_suites(cases: list[dict]) -> None:
    SUITES.mkdir(parents=True, exist_ok=True)
    for old in SUITES.glob("*.md"):
        old.unlink()
    by_file: dict[str, list[dict]] = defaultdict(list)
    for c in cases:
        by_file[c["file"]].append(c)
    for file, cs in sorted(by_file.items()):
        n = _counts(cs)
        lines = [f"# `{file}`", "", "_Generated by `tools/test_report.py` - do not edit._", ""]
        if doc := _module_doc(file):
            lines += [doc, ""]
        lines += [f"**{len(cs)} tests** - {n['passed']} passed, {n['failed'] + n['error']} failed, "
                  f"{n['skipped']} skipped - {sum(c['time'] for c in cs):.2f}s", ""]
        lines += _table(["Test", "Status", "Kind", "Requirements", "Time (s)", "What it verifies"],
                        [[f"`{(c['cls'] + '.' if c['cls'] else '') + c['name']}`",
                          c["status"].upper(), c["kind"], ", ".join(c["req"]),
                          f"{c['time']:.3f}", c["doc"]] for c in cs])
        (SUITES / (Path(file).stem + ".md")).write_text("\n".join(lines) + "\n")


def write_plan(cases: list[dict]) -> None:
    by_req: dict[str, list[dict]] = defaultdict(list)
    for c in cases:
        for r in c["req"]:
            by_req[r].append(c)
    kinds = Counter(c["kind"] for c in cases)
    L = ["# Test Plan & Requirement Traceability", "",
         "_Generated by `tools/test_report.py` from pytest `req` markers and test docstrings - "
         "do not edit by hand. Latest results: [reports/TEST_RESULTS.md]"
         "(../reports/TEST_RESULTS.md)._", "",
         "## How to run", "",
         "| Command | What runs |", "|---|---|",
         "| `make test` | All non-perf tests with the 100% line+branch coverage gate |",
         "| `make e2e` | Live uvicorn + worker tests only |",
         "| `make report` | Everything above + demo + chaos, then regenerates these reports |",
         "| `make report-perf` | ... plus the perf suite and a 20 s locust run |",
         "| `HYPOTHESIS_PROFILE=ci make test` | Property tests at 150 examples (default 60) |",
         "", "## Test layers", ""]
    L += _table(["Kind", "Tests", "Purpose"], [[k, kinds.get(k, 0), d] for k, d in KINDS.items()])
    L += ["", "## Requirement coverage", ""]
    L += _table(["Requirement", "Title", "Acceptance (PRD)", "Tests", "Kinds", "Last run"],
                [[r, t, a, len(by_req.get(r, [])),
                  ", ".join(sorted({c["kind"] for c in by_req.get(r, [])})) or "-",
                  _ok(all(c["status"] in ("passed", "skipped") for c in by_req[r]))
                  if by_req.get(r) else "NOT COVERED"]
                 for r, (t, a) in REQUIREMENTS.items()])
    unknown = sorted(set(by_req) - set(REQUIREMENTS) - {"UNMAPPED"})
    if unmapped := by_req.get("UNMAPPED"):
        L += ["", f"Unmapped tests: {len(unmapped)} "
              f"({', '.join(c['name'] for c in unmapped[:10])})"]
    if unknown:
        L += ["", f"Unknown requirement ids used by tests: {', '.join(unknown)}"]
    L += ["", "## Traceability matrix", ""]
    for r, (title, _) in REQUIREMENTS.items():
        cs = by_req.get(r, [])
        L += [f"### {r} - {title} ({len(cs)} tests)", ""]
        if not cs:
            L += ["_No tests._", ""]
            continue
        L += _table(["Test", "File", "Kind", "Verifies"],
                    [[f"`{c['name']}`", f"[{c['file']}](../{c['file']})", c["kind"], c["doc"]]
                     for c in sorted(cs, key=lambda c: (c["file"], c["name"]))])
        L.append("")
    PLAN.write_text("\n".join(L) + "\n")


def write_results(cases: list[dict], cov: dict, extras: dict, env: dict, run: dict) -> bool:
    main = [c for c in cases if c["kind"] != "perf"]
    n = _counts(main)
    failed = n["failed"] + n["error"]
    pyt = run.get("pytest", {})
    cov_pct = cov.get("totals", {}).get("percent_covered", 0.0)
    scripts = extras.get("scripts", {})
    perf = extras.get("perf", {})
    loc = extras.get("locust")
    perf_ok = all(_perf_checks(perf).values()) if perf else None
    layers = [
        ["pytest (unit, property, concurrency, e2e)", _ok(failed == 0 and pyt.get("rc", 0) == 0),
         f"{n['passed']} passed, {failed} failed, {n['skipped']} skipped "
         f"in {pyt.get('seconds', sum(c['time'] for c in main)):.1f}s"],
        ["Coverage (line + branch)", _ok(cov_pct >= 100.0), f"{cov_pct:.2f}% (gate 100%)"],
    ]
    if "pytest_perf" in run:
        pc = _counts([c for c in cases if c["kind"] == "perf"])
        layers.append(["pytest -m perf", _ok(run["pytest_perf"]["rc"] == 0),
                       f"{pc['passed']} passed, {pc['failed'] + pc['error']} failed "
                       f"in {run['pytest_perf']['seconds']:.1f}s"])
    for name in ("demo", "chaos"):
        if name in scripts:
            s = scripts[name]
            layers.append([f"tools/{name}.sh", _ok(s["rc"] == 0), f"exit {s['rc']} in {s['seconds']}s"])
    if perf:
        layers.append(["Perf (PRD NFR targets)", _ok(perf_ok), ", ".join(
            f"{k}: {_ok(v)}" for k, v in _perf_checks(perf).items())])
    if loc:
        tot = next((e for e in loc["endpoints"] if e["name"] == "Aggregated"), {})
        layers.append(["Locust load test", _ok(loc["rc"] == 0 and tot.get("failures", 1) == 0
                                               and loc["reconcile_match"]),
                       f"{tot.get('requests', 0)} requests, {tot.get('failures', '?')} failures, "
                       f"~{loc['ingest_events_per_s']} events/s, reconcile "
                       f"{'match' if loc['reconcile_match'] else 'MISMATCH'}"])
    all_ok = all(row[1] == "PASS" for row in layers)

    L = ["# Test Results", "",
         "_Generated by `tools/test_report.py` - do not edit by hand. "
         "Traceability: [docs/TEST_PLAN.md](../docs/TEST_PLAN.md)._", "",
         f"**Overall: {'PASS' if all_ok else 'FAIL'}** - {len(cases)} test cases, "
         f"{env['date']}, commit `{env['git']}`", "", "## Summary", ""]
    L += _table(["Layer", "Result", "Detail"], layers)
    L += ["", "## Environment", ""]
    L += _table(["Item", "Value"], [
        ["Python", env["python"]], ["OS", env["os"]], ["CPUs", env["cpus"]],
        ["Hypothesis profile", env["hypothesis_profile"]],
        ["Packages", ", ".join(f"{k} {v}" for k, v in env["packages"].items())],
        ["pytest command", f"`{pyt.get('cmd', 'n/a (--no-run)')}`"]]
        + ([["perf command", f"`{run['pytest_perf']['cmd']}`"]] if "pytest_perf" in run else []))

    by_kind: dict[str, list[dict]] = defaultdict(list)
    by_file: dict[str, list[dict]] = defaultdict(list)
    by_req: dict[str, list[dict]] = defaultdict(list)
    for c in cases:
        by_kind[c["kind"]].append(c)
        by_file[c["file"]].append(c)
        for r in c["req"]:
            by_req[r].append(c)

    def row(cs: list[dict]) -> list[object]:
        k = _counts(cs)
        return [len(cs), k["passed"], k["failed"] + k["error"], k["skipped"],
                f"{sum(c['time'] for c in cs):.2f}"]

    hdr = ["Tests", "Passed", "Failed", "Skipped", "Time (s)"]
    L += ["", "## By test layer", ""]
    L += _table(["Layer", *hdr], [[k, *row(by_kind[k])] for k in KINDS if k in by_kind])
    L += ["", "## By suite", ""]
    L += _table(["Suite", *hdr], [[f"[{f}](suites/{Path(f).stem}.md)", *row(cs)]
                                  for f, cs in sorted(by_file.items())])
    L += ["", "## By PRD requirement", ""]
    L += _table(["Requirement", "Title", *hdr],
                [[r, REQUIREMENTS[r][0], *row(by_req[r])] for r in REQUIREMENTS if r in by_req]
                + ([["UNMAPPED", "-", *row(by_req["UNMAPPED"])]] if "UNMAPPED" in by_req else []))

    if cov:
        L += ["", "## Coverage by file", ""]
        L += _table(["File", "Statements", "Missed", "Branches", "Partial", "Coverage", "Missing lines"],
                    [[f, s["summary"]["num_statements"], s["summary"]["missing_lines"],
                      s["summary"].get("num_branches", 0),
                      s["summary"].get("num_partial_branches", 0),
                      f"{s['summary']['percent_covered']:.1f}%",
                      ", ".join(map(str, s.get("missing_lines", []))) or "-"]
                     for f, s in sorted(cov.get("files", {}).items())])

    if perf:
        L += ["", "## Performance (live API + worker processes)", ""]
        L += _perf_section(perf)
    if loc:
        L += ["", "## Locust load test", "",
              f"{loc['users']} users for {loc['duration']} against a fresh API + worker. "
              f"Ingest ~{loc['ingest_events_per_s']} events/s (single + 20-event batches); "
              f"backlog drained {loc['drain_after_s']}s after load stopped; "
              f"reconcile over {loc['events_reconciled']} events: "
              f"{'match' if loc['reconcile_match'] else 'MISMATCH'}; DLQ {loc['dlq']}.", ""]
        L += _table(["Endpoint", "Requests", "Failures", "req/s", "p50 ms", "p95 ms", "p99 ms", "max ms"],
                    [[e["name"], e["requests"], e["failures"], e["rps"], e["p50_ms"],
                      e["p95_ms"], e["p99_ms"], e["max_ms"]] for e in loc["endpoints"]])
    for name in ("demo", "chaos"):
        if name in scripts:
            L += ["", f"## tools/{name}.sh", "", "```text", *scripts[name]["highlights"], "```"]

    L += ["", "## 10 slowest tests", ""]
    L += _table(["Test", "Suite", "Kind", "Time (s)"],
                [[f"`{c['name']}`", c["file"], c["kind"], f"{c['time']:.2f}"]
                 for c in sorted(cases, key=lambda c: -c["time"])[:10]])
    bad = [c for c in cases if c["status"] in ("failed", "error")]
    L += ["", "## Failures", ""]
    if not bad:
        L.append("None.")
    for c in bad:
        L += [f"### `{c['file']}::{c['name']}`", "", "```text",
              *c["message"].splitlines()[:30], "```", ""]
    (REPORTS / "TEST_RESULTS.md").write_text("\n".join(L) + "\n")
    return all_ok


def _perf_checks(perf: dict) -> dict[str, bool]:
    checks = {}
    if c := perf.get("capacity"):
        checks["capacity"] = (c["throughput_eps"] >= c["thresholds"]["min_eps"]
                              and c["non_2xx"] == 0 and c["reconcile_ok"])
    if lat := perf.get("latency"):
        checks["latency"] = (lat["client_latency"]["p99_ms"] < lat["thresholds"]["p99_ms"]
                             and lat["non_2xx"] == 0 and lat["reconcile_ok"])
    if b := perf.get("burst"):
        checks["burst"] = b["wall_s"] <= b["thresholds"]["burst_s"] and b["reconcile_ok"]
    return checks


def _perf_section(perf: dict) -> list[str]:
    rows = []
    if c := perf.get("capacity"):
        rows += [["Sustained throughput (closed loop)", f"{c['throughput_eps']} events/s",
                  f">= {c['thresholds']['min_eps']:g}", _ok(c["throughput_eps"] >= c["thresholds"]["min_eps"])],
                 ["  ...latency at capacity p50 / p95 / p99",
                  "{p50_ms} / {p95_ms} / {p99_ms} ms".format(**c["client_latency"]), "info", "-"],
                 ["  ...errors / exact drain + reconcile",
                  f"{c['non_2xx']} / {c['billed_events']} billed, reconcile {c['reconcile_ok']}",
                  "0 / all", _ok(c["non_2xx"] == 0 and c["reconcile_ok"])]]
    if lat := perf.get("latency"):
        cl = lat["client_latency"]
        rows += [[f"Ingest latency at {lat['target_eps']:g} events/s (open loop)",
                  f"p50 {cl['p50_ms']} / p95 {cl['p95_ms']} / p99 {cl['p99_ms']} / max {cl['max_ms']} ms",
                  f"p99 < {lat['thresholds']['p99_ms']:g} ms", _ok(cl["p99_ms"] < lat["thresholds"]["p99_ms"])],
                 ["  ...achieved rate / drain after load",
                  f"{lat['achieved_eps']} events/s / {lat['drain_after_ingest_s']} s", "info", "-"]]
    if b := perf.get("burst"):
        rows += [["Burst: 1,000 events as 10 x 100 batches", f"{b['wall_s']} s ({b['burst_eps']} events/s)",
                  f"<= {b['thresholds']['burst_s']:g} s", _ok(b["wall_s"] <= b["thresholds"]["burst_s"])]]
    return _table(["Scenario", "Measured", "Target", "Result"], rows)


# ------------------------------------------------------------------ main

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--perf", action="store_true", help="include perf suite + locust run")
    ap.add_argument("--no-run", action="store_true", help="only regenerate markdown")
    ap.add_argument("--locust-time", default=os.environ.get("LOCUST_TIME", "20s"))
    ap.add_argument("--locust-users", type=int, default=int(os.environ.get("LOCUST_USERS", "50")))
    a = ap.parse_args()
    REPORTS.mkdir(exist_ok=True)
    run: dict = {}
    if not a.no_run:
        EXTRAS.unlink(missing_ok=True)
        JUNIT_PERF.unlink(missing_ok=True)
        print("running pytest (all non-perf suites, coverage gate) ...", flush=True)
        run["pytest"] = run_pytest()
        if a.perf:
            print("running pytest -m perf ...", flush=True)
            run["pytest_perf"] = run_perf()
        scripts = {}
        for name in ("demo", "chaos"):
            print(f"running tools/{name}.sh ...", flush=True)
            scripts[name] = run_script(name)
        _merge_extras("scripts", scripts)
        if a.perf:
            print(f"running locust for {a.locust_time} ...", flush=True)
            _merge_extras("locust", run_locust(a.locust_time, a.locust_users))
        _merge_extras("run", run)
    extras = json.loads(EXTRAS.read_text()) if EXTRAS.exists() else {}
    run = run or extras.get("run", {})
    cases = parse_junit(include_perf="pytest_perf" in run)
    cov = json.loads(COVERAGE.read_text()) if COVERAGE.exists() else {}
    write_suites(cases)
    write_plan(cases)
    ok = write_results(cases, cov, extras, environment(), run)
    print(f"{'PASS' if ok else 'FAIL'}: wrote {REPORTS / 'TEST_RESULTS.md'}, "
          f"{SUITES}/*.md, {PLAN}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
