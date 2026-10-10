# Lifecycle (AU engineering lifecycle, production-first)

`Plan → Design → Build → Test → Deploy → Operate → Improve` — with production
gates moved left (IaC, SLOs, runbook, reconciliation exist in v0.1, not v1.0).

| Phase | Done in this repo | Exit criterion |
|---|---|---|
| Plan | PRD (root `Agentic-Usage-Billing-Engine_PRD.md`), scope/non-goals, cut line | Day-1 exit: curl event → aggregate; dupe = no-op |
| Design | `docs/ARCHITECTURE.md` + `docs/ADRS/` | ADR-001..003 accepted |
| Build | `src/billing_engine/` + `infra/cdk/` (IaC for everything) | `cdk synth` clean |
| Test | `tests/` (rating, idempotency 10k, loss-safety, api) + `loadtest/` | `pytest -q` green; chaos.sh green |
| Deploy | `Dockerfile`, `docker-compose.yml`, CI (`ci.yml`), progressive notes | `tools/smoke.sh` green |
| Operate | metrics/logs/alarms (`observability.py`, CDK alarms), `RUNBOOK.md` | dashboard + test alarm fires |
| Improve | `ROADMAP.md`, `history/ENGINEERING_LOG.md`, agent skills | weekly review of burns + tech debt |

Production-first rules enforced: no merge without IaC change if infra changed;
no rating change without `plan_version` bump + reconcile proof; no MCP tool
mutation (read-only); every incident appends to `history/`.
