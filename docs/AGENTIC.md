# Agentic engineering (PRD §10) — what agents did vs humans

- **Agent-accelerated:** repo scaffold, CDK skeleton, CRUD boilerplate, test
  generation (rating/idempotency matrices), load-test scripts, doc drafts.
- **Human-owned (never delegated):** idempotency design (conditional-put +
  exactly-once-effect), rating semantics (period-scoped allowance/tiers),
  failure-mode analysis (RUNBOOK), security review (no PII in logs, IAM split).
- **Corrections needed:** first rating draft applied allowance per-event
  (wrong — allowance is period-scoped; fixed with two-level rating + ADR-002);
  first queue draft acked before write (loss risk; fixed to write-then-ack with
  transactional ingest); MCP draft proposed mutating tools (rejected — read-only).
- **Standing rule:** agent output on money paths requires a reconcile proof
  (`tools/demo.sh` green) before merge. See `history/ENGINEERING_LOG.md`.
