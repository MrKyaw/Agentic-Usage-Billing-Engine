# Roadmap — POC → pilot → production

## v0.1 POC (this repo, 2-day timebox) ✅
Thin slice: ingest → idempotent process → rate → aggregate → query →
reconcile → MCP. Local runs without AWS; CDK deploys to dev.

## v0.2 Pilot (4–6 wks)
- Auth: Cognito/SSO, per-tenant API keys, tenant isolation tests.
- Streaming decision: stay on SQS until sustained >5k ev/s, else Kinesis + Flink
  (spike first; ADR).
- Analytics: Athena over S3 raw log; Finance CSV → real ERP stub contract.
- Hardening: counter sharding for whale customers, DLQ auto-replay, chaos tests
  in CI, error budgets + paging (SLOS.md).
- MCP: eval set ≥50 questions, auth-scoped tools, audit log of agent queries.

## v1.0 Production
- Multi-region (active-passive read replicas → active-active with conflict
  strategy doc), backup/restore drills, deployment safety (canary + auto-rollback).
- Invoicing/tax/currency, idempotent close + reopen policy, SOC2 audit evidence
  from the raw log.
- Cost optimization pass (reserved concurrency right-sizing, S3 lifecycle,
  DDB autoscaling) with per-1M-events cost published.

## Explicit non-goals carried forward
No real-money movement in v0.x; no legacy migration until v1 (strategy ADR first).
