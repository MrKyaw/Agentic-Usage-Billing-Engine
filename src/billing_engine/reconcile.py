"""Reconciliation — FR5. Recompute totals from the immutable raw log and diff.

Aggregates are DERIVED and rebuildable; the raw log is the source of truth.
Any nonzero diff is a correctness incident (see RUNBOOK failure mode #1).
"""
from __future__ import annotations

import math
from typing import Any

from billing_engine.store import Store

# Aggregates are float running sums in arrival order; replay sums in raw-log
# order. The two can differ by a few ULPs, so compare at 1 part per billion.
REL_TOL = 1e-9
ABS_TOL = 1e-9


def reconcile(store: Store, customer_id: str | None = None,
              period: str | None = None) -> dict[str, Any]:
    # Raw log first, DB snapshot second: anything durably ingested before the
    # snapshot is visible in it as pending, DLQ'd, or billed. A raw event in
    # none of those is a lost event and shows up as a mismatch.
    raw = store.all_raw_payloads()
    pending_ids, dlq_ids, aggs = store.reconcile_snapshot()
    dlq_excluded: set[str] = set()
    pending_excluded: set[str] = set()
    expected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for p in raw:
        per = str(p["timestamp"])[:7]
        if customer_id and p["customer_id"] != customer_id:
            continue
        if period and per != period:
            continue
        if p["event_id"] in dlq_ids:  # never billed; tracked by DLQ alarm instead
            dlq_excluded.add(p["event_id"])
            continue
        if p["event_id"] in pending_ids:  # in flight; tracked by queue-age alarm
            pending_excluded.add(p["event_id"])
            continue
        key = (p["customer_id"], per, p["metric"])
        e = expected.setdefault(key, {"qtys": [], "ids": set()})
        if p["event_id"] not in e["ids"]:  # raw log may contain producer redeliveries
            e["ids"].add(p["event_id"])
            e["qtys"].append(float(p["quantity"]))

    mismatches: list[dict[str, Any]] = []
    checked = 0
    for key in sorted(expected):
        cust, per, metric = key
        stored = aggs.get(key, 0.0)
        exp = math.fsum(expected[key]["qtys"])
        checked += 1
        if not math.isclose(stored, exp, rel_tol=REL_TOL, abs_tol=ABS_TOL):
            mismatches.append({"customer_id": cust, "period": per, "metric": metric,
                               "stored": stored, "recomputed": exp, "diff": stored - exp})
    return {"checked": checked, "mismatches": mismatches,
            "match": len(mismatches) == 0,
            "raw_events_scanned": len(raw),
            "dlq_excluded": len(dlq_excluded),
            "pending_excluded": len(pending_excluded)}


if __name__ == "__main__":
    import argparse
    from billing_engine.store import get_store
    ap = argparse.ArgumentParser()
    ap.add_argument("--customer", default=None)
    ap.add_argument("--period", default=None)
    a = ap.parse_args()
    import json
    print(json.dumps(reconcile(get_store(), a.customer, a.period), indent=2))
