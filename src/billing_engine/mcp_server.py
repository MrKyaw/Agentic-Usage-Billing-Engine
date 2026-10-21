"""MCP server — FR7. Read-only billing tools for AI agents and agentic workflows.

Tools: get_usage, get_bill, reconcile, explain_charge, agentic_decision.
Transport: stdio via FastMCP (works with Claude Desktop / any MCP client).

Run:  python -m billing_engine.mcp_server
"""
from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from billing_engine.query import get_bill as _get_bill
from billing_engine.query import get_usage as _get_usage
from billing_engine.reconcile import reconcile as _reconcile
from billing_engine.store import get_store

mcp = FastMCP("billing-engine")


@mcp.tool()
def get_usage(customer_id: str, period: str) -> dict[str, Any]:
    """Per-metric usage totals for a customer in YYYY-MM period."""
    return _get_usage(get_store(), customer_id, period)


@mcp.tool()
def get_bill(customer_id: str, period: str) -> dict[str, Any]:
    """Rated bill (lines + total) for a customer in YYYY-MM period."""
    return _get_bill(get_store(), customer_id, period)


@mcp.tool()
def reconcile(customer_id: str = "", period: str = "") -> dict[str, Any]:
    """Recompute totals from the raw event log and diff vs aggregates."""
    return _reconcile(get_store(), customer_id or None, period or None)


@mcp.tool()
def explain_charge(customer_id: str, event_id: str) -> dict[str, Any]:
    """Trace one event → rated record → its contribution to the period bill."""
    store = get_store()
    ev = store.get_event(event_id)
    if not ev or ev["customer_id"] != customer_id:
        return {"found": False, "event_id": event_id, "customer_id": customer_id}
    bill = _get_bill(store, customer_id, ev["period"])
    line = next((line for line in bill["lines"] if line["metric"] == ev["metric"]), None)
    return {"found": True, "event": ev, "period_bill_line": line,
            "bill_total": bill["total_amount"],
            "note": f"event contributed qty={ev['quantity']} to {ev['metric']} "
                    f"total={line['total_quantity'] if line else '?'} "
                    f"(plan {ev['plan_version']})"}


@mcp.tool()
def agentic_decision(customer_id: str, period: str, question: str) -> dict[str, Any]:
    """Answer a billing question using the MCP tools + internal heuristics.

    This is the agentic workflow entry point: an LLM or agent calls this tool
    with a customer/period/question, and it returns a structured answer citing
    usage, bill, reconciliation status, and any evidence from the event log.
    """
    from billing_engine.reconcile import reconcile as _reconcile_tool
    usage = _get_usage(get_store(), customer_id, period)
    bill = _get_bill(get_store(), customer_id, period)
    recon = _reconcile_tool(get_store(), customer_id, period)

    # Build evidence summary
    evidence = []
    for line in bill["lines"]:
        evidence.append({
            "metric": line["metric"],
            "total_quantity": line["total_quantity"],
            "billable_quantity": line["billable_quantity"],
            "amount": line["amount"],
            "plan_version": line["plan_version"]
        })

    return {
        "customer_id": customer_id,
        "period": period,
        "question": question,
        "usage": usage,
        "bill": bill,
        "reconciliation": {
            "match": recon["match"],
            "checked": recon["checked"],
            "mismatches": recon["mismatches"],
            "dlq_excluded": recon.get("dlq_excluded", 0),
            "pending_excluded": recon.get("pending_excluded", 0)
        },
        "evidence": evidence,
        "answer": _format_answer(question, bill, recon, evidence)
    }


def _format_answer(question: str, bill: dict, recon: dict, evidence: list) -> str:
    """Format a human-readable answer from the agentic decision output."""
    ql = question.lower()
    if "reconcile" in ql or "match" in ql:
        return (f"Reconciliation: match={recon['match']} checked={recon['checked']} "
                f"mismatches={len(recon['mismatches'])} (dlq_excluded={recon.get('dlq_excluded', 0)}, "
                f"pending_excluded={recon.get('pending_excluded', 0)})")
    if not bill["lines"]:
        return (f"No rated usage for {bill['customer_id']} in {bill['period']} "
                f"(status={bill['status']}).")
    if "owe" in ql or "bill" in ql or "cost" in ql:
        total = bill["total_amount"]
        top = max(bill["lines"], key=lambda line: line["amount"], default=None)
        return (f"{bill['customer_id']} owes ${total} for {bill['period']} "
                f"(status={bill['status']}). Top line: {top['metric']} "
                f"${top['amount']} ({top['billable_quantity']} billable@{top['plan_version']})")
    if "consume" in ql or "usage" in ql:
        lines = bill["lines"]
        total_qty = sum(line["total_quantity"] for line in lines)
        return (f"{bill['customer_id']} consumed {total_qty} total across "
                f"{len(lines)} metrics in {bill['period']}; "
                f"top metric: {max(lines, key=lambda line: line['total_quantity'])['metric']}")
    if "explain" in ql or "event" in ql:
        return json.dumps(bill, indent=2)
    return json.dumps({
        "customer_id": bill["customer_id"],
        "period": bill["period"],
        "total_amount": bill["total_amount"],
        "status": bill["status"],
        "lines": bill["lines"]
    })


if __name__ == "__main__":
    mcp.run()
