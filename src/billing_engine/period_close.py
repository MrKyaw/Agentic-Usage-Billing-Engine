"""Period close — FR4. Freeze a billing summary + CSV export (finance stub)."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from billing_engine.query import get_bill
from billing_engine.store import Store


def close_period(store: Store, customer_id: str, period: str,
                 export_dir: str | Path = "exports") -> dict[str, Any]:
    bill = get_bill(store, customer_id, period)
    bill["status"] = "closed"
    store.close_period(customer_id, period, bill)
    export_dir = Path(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    fp = export_dir / f"bill-{customer_id}-{period}.csv"
    with open(fp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "period", "metric", "total_quantity",
                    "billable_quantity", "amount", "plan_version"])
        for line in bill["lines"]:
            w.writerow([customer_id, period, line["metric"], line["total_quantity"],
                        line["billable_quantity"], line["amount"], line["plan_version"]])
        w.writerow([customer_id, period, "TOTAL", "", "", bill["total_amount"], ""])
    return {"bill": bill, "csv": str(fp)}
