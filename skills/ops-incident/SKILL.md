# Skill: ops-incident
Use when metrics/alarms indicate pipeline trouble.

1. Read `/v1/ops/metrics`: pending, dlq, oldest_pending_age_s, alarms[].
2. DLQ > 0 → inspect reasons: unknown metric (add plan + replay) vs throttling.
3. Queue age rising without errors → scale processor concurrency (CDK), then verify
   lag → 0 and run reconcile.
4. Follow docs/RUNBOOK.md ordering; append findings to history/ENGINEERING_LOG.md.
