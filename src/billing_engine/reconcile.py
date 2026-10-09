"""Reconciliation — FR5. Recompute totals from the immutable raw log and diff.

Aggregates are DERIVED and rebuildable; the raw log is the source of truth.
Any nonzero diff is a correctness incident (see RUNBOOK failure mode #1).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from billing_engine.store import Store


def reconcile(store: Store, customer_id: str | None = None,
              period: str | None = None) -> dict[str, Any]:
    raw = store.all_raw_payloads()
    expected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for p in raw:
        per = str(p["timestamp"])[:7]
        if customer_id and p["customer_id"] != customer_id:
            continue
        if period and per != period:
            continue
        key = (p["customer_id"], per, p["metric"])
        e = expected.setdefault(key, {"qty": 0.0, "ids": set()})
        if p["event_id"] not in e["ids"]:  # raw log may contain producer redeliveries
            e["ids"].add(p["event_id"])
            e["qty"] += float(p["quantity"])

    mismatches: list[dict[str, Any]] = []
    checked = 0
    seen: set[tuple[str, str, str]] = set()
    # compare against stored aggregates for the same scope
    keys = set(expected.keys())
    for (cust, per, metric) in sorted(keys):
        seen.add((cust, per, metric))
        rows = [r for r in store.aggregates_for(cust, per) if r["metric"] == metric]
        stored = float(rows[0]["total_quantity"]) if rows else 0.0
        exp = expected[(cust, per, metric)]["qty"]
        checked += 1
        if abs(stored - exp) > 1e-9:
            mismatches.append({"customer_id": cust, "period": per, "metric": metric,
                               "stored": stored, "recomputed": exp, "diff": stored - exp})
    return {"checked": checked, "mismatches": mismatches,
            "match": len(mismatches) == 0,
            "raw_events_scanned": len(raw)}


if __name__ == "__main__":
    import argparse
    from billing_engine.store import get_store
    ap = argparse.ArgumentParser()
    ap.add_argument("--customer", default=None)
    ap.add_argument("--period", default=None)
    a = ap.parse_args()
    import json
    print(json.dumps(reconcile(get_store(), a.customer, a.period), indent=2))
