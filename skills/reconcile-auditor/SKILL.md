# Skill: reconcile-auditor
Use for audit/reconciliation tasks.

1. Run full `reconcile()` (no scope) first; report checked/match counts.
2. On mismatch, narrow by customer then period; diff stored vs recomputed.
3. Suspect plan_version changes mid-period (mixed versions are expected, not a bug).
4. Propose replay-from-raw-log as the fix; never propose hand-editing aggregates.
