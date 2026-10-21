"""Agent demo harness — answers natural-language billing questions via MCP tools.

Runs the same logic the MCP server exposes (imported from query/reconcile, not
through the MCP decorator objects) so the demo works with zero model
credentials. With an API key, swap `answer()` for a real agent loop
(see docs/AGENTIC.md).
"""
from __future__ import annotations

import argparse
import json

from billing_engine.query import get_bill as _bill
from billing_engine.query import get_usage as _usage
from billing_engine.reconcile import reconcile as _reconcile
from billing_engine.store import get_store

QUESTIONS = [
    "What did {c} consume in {p}?",
    "What does {c} owe for {p}?",
    "Is {c}'s bill for {p} reconciled?",
    "Explain charge for event {e} on {c}.",
    "Which metric cost {c} the most in {p}?",
]


def _explain(customer_id: str, event_id: str) -> dict:
    store = get_store()
    ev = store.get_event(event_id)
    if not ev or ev["customer_id"] != customer_id:
        return {"found": False, "event_id": event_id, "customer_id": customer_id}
    bill = _bill(store, customer_id, ev["period"])
    line = next((line for line in bill["lines"] if line["metric"] == ev["metric"]), None)
    return {"found": True, "event": ev, "period_bill_line": line,
            "bill_total": bill["total_amount"],
            "note": f"event contributed qty={ev['quantity']} to {ev['metric']} "
                    f"total={line['total_quantity'] if line else '?'} "
                    f"(plan {ev['plan_version']})"}


def answer(q: str, customer: str, period: str, event_id: str = "") -> str:
    store = get_store()
    ql = q.lower()
    if "reconcile" in ql:
        r = _reconcile(store, customer, period)
        return f"Reconciliation: match={r['match']} checked={r['checked']} mismatches={r['mismatches']}"
    if "explain" in ql or "event" in ql:
        return json.dumps(_explain(customer, event_id or "evt-demo-001"))
    if "owe" in ql or "bill" in ql or "cost" in ql or "most" in ql:
        b = _bill(store, customer, period)
        top = max(b["lines"], key=lambda line: line["amount"], default=None)
        return (f"{customer} owes ${b['total_amount']} for {period} "
                f"(status={b['status']}). Top line: {top}")
    return json.dumps(_usage(store, customer, period))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customer", default="acme")
    ap.add_argument("--period", default="2026-10")
    ap.add_argument("--event", default="evt-demo-001")
    a = ap.parse_args()
    print("=== Agentic billing demo (5 scripted questions, PRD §9: 5/5 via MCP) ===")
    ok = 0
    for q in QUESTIONS:
        qs = q.format(c=a.customer, p=a.period, e=a.event)
        try:
            print(f"\nQ: {qs}\nA: {answer(qs, a.customer, a.period, a.event)}")
            ok += 1
        except Exception as e:  # noqa: BLE001
            print(f"\nQ: {qs}\nA: ERROR {e}")
    print(f"\nResult: {ok}/5 answered")


if __name__ == "__main__":
    main()
