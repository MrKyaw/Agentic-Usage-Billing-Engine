# ADR-002: Two-level rating (unit estimate + period settlement)
Date: 2026-10-09. Status: accepted.
Context: allowances/tiers are period-scoped; per-event application is wrong.
Decision: immutable per-event unit estimate; authoritative `rate_period` over summed totals.
Consequences: replay-safe; `plan_version` stored per event for mixed-version periods.
