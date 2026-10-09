"""Query service — FR4 reads (Query API Lambda analogue)."""
from __future__ import annotations

from typing import Any

from billing_engine.rating import get_book
from billing_engine.store import Store


def get_usage(store: Store, customer_id: str, period: str) -> dict[str, Any]:
    rows = store.aggregates_for(customer_id, period)
    book = get_book()
    lines = []
    for r in sorted(rows, key=lambda x: x["metric"]):
        rated = book.rate_period(r["metric"], float(r["total_quantity"]))
        lines.append({"metric": r["metric"], "total_quantity": r["total_quantity"],
                      "event_count": r["event_count"], **rated})
    return {"customer_id": customer_id, "period": period, "lines": lines}


def get_bill(store: Store, customer_id: str, period: str) -> dict[str, Any]:
    usage = get_usage(store, customer_id, period)
    total = round(sum(l["amount"] for l in usage["lines"]), 2)
    close = store.get_close(customer_id, period)
    return {"customer_id": customer_id, "period": period,
            "lines": usage["lines"], "total_amount": total,
            "currency": "USD", "status": "closed" if close else "open",
            "closed_at": close["closed_at"] if close else None}
