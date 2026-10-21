#!/usr/bin/env python3
"""Eval suite — zero-defect ledger accuracy gate (Production Guardrails).

Runs the invariants that must hold before a billing release, against fresh
local stores. Any failure exits non-zero and blocks the merge.

Evals
  1. idempotency under 10% redelivery
  2. loss safety after a simulated consumer crash
  3. deterministic rating (flat + tiered allowance)
  4. reconciliation across customers, periods and metrics
  5. DLQ redrive preserves exact totals
  6. agentic decision returns reconciled evidence
  7. partition scaling 20 -> 40 keeps one owner per key
  8. checksum chain traces and repairs broken ordering
  9. data-lake snapshots: time travel, compaction, checksum verify
 10. governance: green verdict deploys, red verdict is blocked

Run:  python3 tools/eval_suite.py
"""
from __future__ import annotations

import contextlib
import io
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from billing_engine.datalake import LakeTable  # noqa: E402
from billing_engine.governance import (  # noqa: E402
    deployment_allowed,
    evaluate,
    sign,
)
from billing_engine.ingest import ingest_one  # noqa: E402
from billing_engine.mcp_server import agentic_decision  # noqa: E402
from billing_engine.partitioning import (  # noqa: E402
    CutoverBarrier,
    Partitioner,
    build_chain,
    plan_scale,
    repair_chain,
    verify_chain,
)
from billing_engine.processor import drain, redrive_dlq  # noqa: E402
from billing_engine.rating import get_book  # noqa: E402
from billing_engine.reconcile import reconcile  # noqa: E402
from billing_engine.store import Store, get_store, reset_store_cache  # noqa: E402

ROOT = Path(tempfile.mkdtemp(prefix="billing-eval-"))
SECRET = "eval-signing-key"


def event(event_id: str, customer_id: str = "acme", metric: str = "cloud_credits",
          quantity: float = 1.0, timestamp: str = "2026-10-09T10:00:00Z") -> dict:
    return {"event_id": event_id, "customer_id": customer_id, "product_id": "p",
            "metric": metric, "quantity": quantity, "timestamp": timestamp}


def store(name: str) -> Store:
    return Store(ROOT / f"{name}.db", ROOT / f"{name}-raw")


def seed(store_: Store, count: int = 100) -> None:
    for i in range(count):
        ingest_one(store_, event(f"e-{i:04d}"))


def total_quantity(store_: Store, customer_id: str = "acme",
                   period: str = "2026-10") -> float:
    return sum(a["total_quantity"] for a in store_.aggregates_for(customer_id, period))


# ----------------------------------------------------------------------- evals

def eval_idempotency() -> None:
    s = store("idem")
    seed(s)
    for i in range(0, 100, 10):  # 10% producer redelivery
        ingest_one(s, event(f"e-{i:04d}"))
    result = drain(s)
    assert result["accepted"] == 100, result
    assert result["duplicate"] == 10, result
    assert reconcile(s)["match"] is True
    assert total_quantity(s) == 100.0


def eval_loss_safety() -> None:
    s = store("loss")
    seed(s, 50)
    claimed = s.claim_batch(limit=10)
    assert len(claimed) == 10
    s._conn().execute("UPDATE inbox SET claimed_at=NULL")  # crash before ack
    drain(s)
    assert reconcile(s)["match"] is True
    assert total_quantity(s) == 50.0


def eval_rating() -> None:
    book = get_book()
    flat = book.rate_period("cloud_credits", 100.0)
    assert flat["amount"] == 10.0 and flat["billable_quantity"] == 100.0, flat
    tiered = book.rate_period("render_minutes", 1500.0)
    assert tiered["billable_quantity"] == 1400.0, tiered
    assert abs(tiered["amount"] - 66.0) < 0.001, tiered


def eval_reconciliation() -> None:
    s = store("recon")
    for customer in ("cust-a", "cust-b", "cust-c"):
        for metric in ("cloud_credits", "render_minutes", "inference_tokens"):
            for period in ("2026-10", "2026-11", "2026-12"):
                for n in range(3):
                    ingest_one(s, event(f"{customer}-{metric}-{period}-{n}", customer,
                                        metric, float(n + 1),
                                        f"{period}-01T12:00:00Z"))
    drain(s)
    report = reconcile(s)
    assert report["match"] is True, report
    assert report["checked"] == 27, report


def eval_dlq_redrive() -> None:
    s = store("dlq")
    ingest_one(s, event("dlq-1", quantity=42.0))
    claimed = s.claim_batch(limit=1)
    s.move_to_dlq(claimed[0]["seq"], "dlq-1", claimed[0]["payload"], "eval injection")
    assert redrive_dlq(s) == 1
    drain(s)
    assert reconcile(s)["match"] is True
    assert total_quantity(s) == 42.0


def eval_agentic_decision() -> None:
    import os
    os.environ["BILLING_DB_PATH"] = str(ROOT / "agentic.db")
    os.environ["BILLING_RAW_LOG_DIR"] = str(ROOT / "agentic-raw")
    reset_store_cache()
    s = get_store()  # the MCP tool resolves the process-wide store
    seed(s, 3)
    drain(s)
    result = agentic_decision("acme", "2026-10", "What do I owe?")
    assert result["reconciliation"]["match"] is True, result
    assert len(result["evidence"]) == 1, result
    assert "owes" in result["answer"], result["answer"]
    reset_store_cache()


def eval_partition_scaling() -> None:
    keys = [f"cust-{i:04d}" for i in range(200)]
    plan = plan_scale(keys, 20, 40, epoch=1)
    assert plan.moved_count + len(plan.stable) == len(keys)
    assert 0.35 < plan.moved_fraction < 0.65, plan.moved_fraction
    targets = {move.key: move.to_partition for move in plan.moves}
    targets.update({key: Partitioner(40, 1).partition_for(key) for key in plan.stable})
    assert len(targets) == len(keys)  # one owner per key, never split
    assert all(0 <= p < 40 for p in targets.values())

    barrier = CutoverBarrier(Partitioner(20, 0))
    old = barrier.partitioner.route("cust-0001")
    barrier.fence()
    barrier.begin(plan)
    barrier.activate()
    assert barrier.accept(old) is False  # stale epoch cannot double-write
    assert barrier.accept(barrier.partitioner.route("cust-0001")) is True


def eval_checksum_repair() -> None:
    chain = build_chain([{"seq": i, "event_id": f"e{i}"} for i in range(5)])
    assert verify_chain(chain) == []
    broken = [chain[3], chain[0], chain[4], chain[1], chain[2]]
    assert verify_chain(broken)
    repaired = repair_chain(broken)
    assert repaired.order_changed is True and repaired.repaired is True
    assert [r["event_id"] for r in repaired.ordered] == [f"e{i}" for i in range(5)]
    assert verify_chain(repaired.ordered) == []


def eval_data_lake() -> None:
    table = LakeTable(ROOT, "events")
    table.append([event("d1", timestamp="2026-10-09T10:00:00Z")])
    table.append([event("d2", timestamp="2026-10-10T10:00:00Z")])
    assert [r["event_id"] for r in table.scan(as_of=1)] == ["d1"]
    compacted = table.compact()
    assert compacted.snapshot_id == 3
    assert [r["event_id"] for r in table.scan()] == ["d1", "d2"]
    assert [r["event_id"] for r in table.scan(as_of=1)] == ["d1"]  # history kept
    assert table.verify() == []


def eval_governance() -> None:
    green = evaluate("eval", "agent", ["src/billing_engine/rating.py"],
                     tests_passed=254, tests_failed=0, coverage_pct=100.0,
                     reconcile_match=True, todo_count=0, lint_errors=0)
    assert green.passed is True and green.reviewers_required == 2
    decision = deployment_allowed(green, sign(green, SECRET), SECRET)
    assert decision.allowed is True, decision

    red = evaluate("eval", "agent", ["src/billing_engine/store.py"],
                   tests_passed=254, tests_failed=1, coverage_pct=99.0,
                   reconcile_match=False, todo_count=1, lint_errors=2)
    blocked = deployment_allowed(red, sign(red, SECRET), SECRET)
    assert blocked.allowed is False and blocked.reasons


EVALS = [
    ("idempotency under 10% redelivery", eval_idempotency),
    ("loss safety after consumer crash", eval_loss_safety),
    ("deterministic flat + tiered rating", eval_rating),
    ("reconciliation across customers/periods", eval_reconciliation),
    ("DLQ redrive preserves exact totals", eval_dlq_redrive),
    ("agentic decision returns reconciled evidence", eval_agentic_decision),
    ("partition scaling 20->40 keeps one owner", eval_partition_scaling),
    ("checksum chain traces and repairs order", eval_checksum_repair),
    ("data-lake snapshots, time travel, compaction", eval_data_lake),
    ("governance verdict gates deployment", eval_governance),
]


def main() -> int:
    print(f"Eval suite — workspace {ROOT}")
    failures: list[str] = []
    for name, fn in EVALS:
        captured = io.StringIO()
        try:
            with contextlib.redirect_stdout(captured):
                fn()
        except AssertionError as exc:
            failures.append(name)
            print(f"FAIL  {name}: {exc}")
            tail = captured.getvalue().strip().splitlines()[-3:]
            for line in tail:
                print(f"      {line}")
        else:
            print(f"PASS  {name}")
    shutil.rmtree(ROOT, ignore_errors=True)
    print(f"\n{len(EVALS) - len(failures)}/{len(EVALS)} evals passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
