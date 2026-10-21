# Agentic engineering governance

The question behind this note: **agents write a lot of code — how do you know
it is correct, and can humans really review all of it?**

The answer is not "trust the agent" and not "a human reads every line". It is a
graded system: mechanical gates for facts, risk tiers for human attention, and a
signed verdict before deployment. The policy is executable in
[`src/billing_engine/governance.py`](../src/billing_engine/governance.py).

## 1. How is agent-generated code verified?

Every change goes through the same gates regardless of author:

| Gate | Fact checked | Enforced by |
|---|---|---|
| `tests` | at least one test ran and none failed | `pytest` |
| `coverage` | line **and branch** coverage is 100% | `--cov-fail-under=100` |
| `reconcile` | recomputing from the raw log matches stored aggregates | `reconcile()` / eval suite |
| `todos` | no `TODO`/`FIXME` left in `src/` | CI guardrail |
| `lint` | `ruff check src tests tools` is clean | CI |
| eval suite | 10 invariants (idempotency, loss safety, rating, reconciliation, DLQ, agentic, scaling, checksums, lake, governance) | `tools/eval_suite.py` |

`evaluate(...)` turns those facts into a `Verdict`; `sign(verdict, secret)` makes
it tamper-evident; `deployment_allowed(...)` returns `allowed=False` if any gate
is red **or** the signature is invalid. A green local run cannot be edited into a
green deploy.

Property-based tests (Hypothesis) add adversarial coverage: arbitrary event
orders, duplicate patterns and crash points must still produce exactly-once
totals. The checksum/order evals cover the streaming failure modes directly.

## 2. The new-grad-with-Claude-Code scenario

Treat agent output as an **untrusted pull request**, not as a junior's finished
work:

1. **Design stays human-owned.** Idempotency, rating semantics, ordering,
   reconciliation and security are decided before prompting; the agent fills in
   implementation and tests.
2. **The agent must produce evidence, not claims.** Tests, coverage, reconcile
   proof and the eval suite are the artefact; a prose summary is not.
3. **Risk tier decides review depth** (next section). A money-path change gets
   two humans and a reconcile proof even if every gate is green.
4. **Reviewers read the diff and the failure modes**, not every generated line:
   boundary conditions, error paths, schema/idempotency changes, and anything
   that weakens a gate.

## 3. Risk tiers and required reviewers

`classify_paths(paths)` marks `rating.py`, `processor.py`, `store.py`,
`reconcile.py`, `ingest.py`, `period_close.py`, `query.py`, `models.py`,
`partitioning.py` and `datalake.py` as **money path**.

| Change | Agent-authored | Human-authored |
|---|---|---|
| money path | **2 reviewers + reconcile proof** | 1 reviewer |
| standard code | 1 reviewer | 0 |
| docs only | 0 (automated gates only) | 0 |

`required_reviewers(risk, generated_by)` is the source of truth; CI and the
review bot should read it rather than hard-code a number.

## 4. "They all passed, so you deployed to production?"

No. Passing gates is necessary, not sufficient. Deployment also requires:

- a **signed verdict** (`deployment_allowed`) for the exact commit;
- the **eval suite** green on that commit (not just unit tests);
- a **canary/progressive rollout** with rollback in the production path
  (`docs/ARCHITECTURE.md` → prod path);
- the reconcile proof attached to the change, so finance can audit the money
  invariant independently of the code review.

For the POC, "deploy" means merge to `main`; the same verdict object is the
contract a production pipeline would consume.

## 5. "Won't all this slow us down?"

Mechanical checks are cheap and parallel: lint, tests, coverage and the eval
suite run in CI in minutes. The expensive resource is **human judgement**, so it
is spent only where risk is:

- docs/boilerplate: automated gates, no human queue;
- standard code: one reviewer, checklist + tests;
- money path: two reviewers, design + reconcile proof.

`review_capacity(prs_per_day, minutes_per_review, reviewers, hours_per_reviewer)`
makes the trade-off explicit. Example: 20 agent PRs/day × 15 min = 5 reviewer
hours; 3 reviewers × 2 h/day = 6 h available → feasible (83% utilisation).
At 40 PRs/day × 30 min the same team needs 20 h and is infeasible — the answer
is to reduce PR size/risk, not to pretend review happened.

## 6. Is human review of all agent code realistic?

Not line-by-line at high volume, and it is the wrong target. Realistic model:

- **100% automated gates** on every change (no exceptions, no `|| true`);
- **100% human review of money-path and design changes**;
- **sampled/automated review** of low-risk generated code, with a periodic audit
  of a random sample and a rollback path;
- **small PRs** so review is bounded and evidence is readable.

The POC keeps the money path small and explicit precisely so two humans can
review all of it. Scaling the agent volume means scaling the *gate quality* and
shrinking the risky surface, not multiplying reviewers.

## 7. Code and tests

| Concern | Code | Tests |
|---|---|---|
| Gate evaluation | `evaluate`, `Gate`, `Verdict` | `test_governance.py::test_green_verdict_*`, `test_each_failing_fact_*` |
| Risk classification | `classify_paths` | `test_classify_paths_*` |
| Reviewer matrix | `required_reviewers` | `test_required_reviewers_matrix` |
| Signed verdict | `sign`, `verify` | `test_signed_verdict_*` |
| Deploy gate | `deployment_allowed` | `test_deployment_requires_*` |
| Review capacity | `review_capacity` | `test_review_capacity_*` |
| CI enforcement | `.github/workflows/ci.yml` | CI `test` + `guardrails` jobs |
| Eval suite | `tools/eval_suite.py` | eval 10 + all evals |

Related: [AGENTIC.md](AGENTIC.md) (what was delegated vs human-owned),
[TEST_PLAN.md](TEST_PLAN.md) (traceability), [RUNBOOK.md](RUNBOOK.md)
(incident response).
