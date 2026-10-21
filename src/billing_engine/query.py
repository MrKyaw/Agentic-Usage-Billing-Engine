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


def get_bill(store: Store, customer_id: str, period: str,
             live: bool = False) -> dict[str, Any]:
    """Open periods are rated live. Closed periods return the frozen invoice;
    usage that landed after close is surfaced as `post_close_delta`, never
    silently folded into a closed bill. `live=True` ignores the freeze (used by
    period close itself)."""
    usage = get_usage(store, customer_id, period)
    total = round(sum((line["amount"] for line in usage["lines"]), 0.0), 2)
    close = None if live else store.get_close(customer_id, period)
    if close is None:
        return {"customer_id": customer_id, "period": period,
                "lines": usage["lines"], "total_amount": total,
                "currency": "USD", "status": "open", "closed_at": None,
                "post_close_delta": 0.0}
    frozen = close["summary"]
    return {"customer_id": customer_id, "period": period,
            "lines": frozen["lines"], "total_amount": frozen["total_amount"],
            "currency": frozen.get("currency", "USD"), "status": "closed",
            "closed_at": close["closed_at"],
            "post_close_delta": round(total - frozen["total_amount"], 2)}
