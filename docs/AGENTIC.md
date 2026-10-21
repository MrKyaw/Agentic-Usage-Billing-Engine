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
  (`tools/demo.sh` green) **and** the 10-eval suite
  (`python3 tools/eval_suite.py`) before merge. See `history/ENGINEERING_LOG.md`.

## Verification pipeline (executable)

Policy lives in `billing_engine.governance` and CI, not in prose:

1. **Mechanical gates** — tests, 100% line+branch coverage, reconcile proof,
   no TODO/FIXME, clean `ruff`; `evaluate()` turns the facts into a `Verdict`.
2. **Signed verdict** — `sign()`/`deployment_allowed()`; a green local run cannot
   be edited into a green deploy.
3. **Risk tiers** — `classify_paths()` marks money-path files; agent-authored
   money-path changes need **2 reviewers + reconcile proof**, standard code 1,
   docs 0.
4. **Capacity math** — `review_capacity()` shows whether the team can absorb the
   agent PR volume (e.g. 20 PRs/day × 15 min = 5 h vs 6 h available).

Full policy and the "can humans review everything?" answer:
[docs/GOVERNANCE.md](GOVERNANCE.md).
