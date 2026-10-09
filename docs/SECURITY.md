# Security (POC posture → pilot upgrades)

- Auth: `X-API-Key` (POC only) → Cognito/SSO + per-tenant keys (pilot). Keys in
  Secrets Manager, never in repo (`.env.example` only).
- IAM least privilege: ingest writes queue only; processor consumes queue +
  writes tables/bucket; query reads only (see `infra/cdk/billing_stack.py`).
- Encryption: TLS in transit; SSE-S3 + DDB encryption at rest (defaults on).
- PII: no PII in events by contract; logs carry `event_id/customer_id` only —
  `metadata` values never logged (`log_safe_dict`), only key names.
- MCP: read-only tools; agent queries audit-logged (pilot: per-tool IAM scope).
