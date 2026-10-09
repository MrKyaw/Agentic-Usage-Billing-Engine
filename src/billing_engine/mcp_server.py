"""MCP server — FR7. Read-only billing tools for AI agents.

Tools: get_usage, get_bill, reconcile, explain_charge.
Transport: stdio via FastMCP (works with Claude Desktop / any MCP client).

Run:  python -m billing_engine.mcp_server
"""
from __future__ import annotations

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
    line = next((l for l in bill["lines"] if l["metric"] == ev["metric"]), None)
    return {"found": True, "event": ev, "period_bill_line": line,
            "bill_total": bill["total_amount"],
            "note": f"event contributed qty={ev['quantity']} to {ev['metric']} "
                    f"total={line['total_quantity'] if line else '?'} "
                    f"(plan {ev['plan_version']})"}


if __name__ == "__main__":
    mcp.run()
