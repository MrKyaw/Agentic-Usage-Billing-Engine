"""Deterministic, versioned rating — FR3. Human-owned correctness logic.

Semantics (per customer x period x metric, computed over period totals):
  - flat:    amount = max(0, total_qty - allowance) * unit_price
  - tiered:  allowance consumed first, then marginal tiers over billable qty.
Rating is a pure function of (plan_version, metric, quantity) for unit rating,
plus a period-level adjustment for allowance/tiers applied at query/close time.

Why two levels? Unit records stay immutable & auditable; period math stays
recomputable. `rate_unit` prices one event; `rate_period` prices a summed total.
Aggregates store BOTH raw quantity totals and rated amounts so reconciliation
can replay either.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RatingError(ValueError):
    pass


class PriceBook:
    def __init__(self, plan: dict[str, Any]):
        self.plan_version: str = plan["plan_version"]
        self.plans: dict[str, Any] = plan["plans"]

    @classmethod
    def load(cls, path: str | Path) -> "PriceBook":
        with open(path) as f:
            return cls(json.load(f))

    def unit_price_for(self, metric: str) -> float:
        cfg = self._cfg(metric)
        if cfg["type"] == "flat":
            return float(cfg["unit_price"])
        # tiered: marginal first-tier price shown for unit estimate
        return float(cfg["tiers"][0]["unit_price"])

    def rate_unit(self, metric: str, quantity: float) -> tuple[float, str]:
        """Return (estimated_amount, plan_version). Estimate ignores cross-event
        allowance/tiers; final money is settled at period level by rate_period."""
        cfg = self._cfg(metric)
        allowance = float(cfg.get("included_allowance", 0) or 0)
        billable = max(0.0, quantity - 0.0)  # unit estimate: full qty priced
        _ = allowance  # allowance is period-scoped, not unit-scoped
        if cfg["type"] == "flat":
            return round(billable * float(cfg["unit_price"]), 6), self.plan_version
        tiers = cfg["tiers"]
        price = float(tiers[0]["unit_price"])
        return round(billable * price, 6), self.plan_version

    def rate_period(self, metric: str, total_quantity: float) -> dict[str, Any]:
        """Authoritative period charge for a metric total."""
        cfg = self._cfg(metric)
        allowance = float(cfg.get("included_allowance", 0) or 0)
        billable = max(0.0, total_quantity - allowance)
        if cfg["type"] == "flat":
            amount = round(billable * float(cfg["unit_price"]), 2)
            return {"total_quantity": total_quantity, "billable_quantity": billable,
                    "amount": amount, "plan_version": self.plan_version}
        remaining = billable
        amount = 0.0
        prev_cap = 0.0
        for tier in cfg["tiers"]:
            cap = tier["up_to"]
            width = float("inf") if cap is None else float(cap) - prev_cap
            take = min(remaining, width)
            amount += take * float(tier["unit_price"])
            remaining -= take
            if cap is not None:
                prev_cap = float(cap)
            if remaining <= 1e-9:
                break
        return {"total_quantity": total_quantity, "billable_quantity": billable,
                "amount": round(amount, 2), "plan_version": self.plan_version}

    def _cfg(self, metric: str) -> dict[str, Any]:
        if metric not in self.plans:
            raise RatingError(f"unknown metric '{metric}' (no price plan)")
        return self.plans[metric]


_DEFAULT_BOOK: PriceBook | None = None


def get_book(plan_path: str | Path | None = None) -> PriceBook:
    global _DEFAULT_BOOK
    if plan_path is not None:
        return PriceBook.load(plan_path)
    if _DEFAULT_BOOK is None:
        from billing_engine.config import SETTINGS
        _DEFAULT_BOOK = PriceBook.load(SETTINGS.plan_path)
    return _DEFAULT_BOOK


def reset_book_cache() -> None:
    global _DEFAULT_BOOK
    _DEFAULT_BOOK = None
