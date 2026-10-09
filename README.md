# Agentic Usage Billing Engine — working prototype (POC v0.1)

Event-driven, idempotent, auditable usage-based billing pipeline on AWS
(serverless-first), with operability from day zero and an agent-facing (MCP)
interface.

> Status: working local prototype. Same code path maps 1:1 to AWS
> (API Gateway → Lambda → SQS → Lambda → DynamoDB + S3). See `docs/ARCHITECTURE.md`
> and `infra/cdk/`.

## 30-second run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make test          # unit + correctness (idempotency, reconciliation)
make demo          # end-to-end: ingest → process → query → reconcile (local, no AWS)
make api           # run FastAPI locally → curl examples in tools/demo.sh
```

## What this proves (PRD §2, §9)

| PRD goal | Where |
|---|---|
| Ingest `POST /v1/usage-events` + batch ≤100, 202 on accept | `src/billing_engine/api.py`, `ingest.py` |
| Idempotent exactly-once-effect (10% dupes → 0 double-bill) | `processor.py` (conditional put on `event_id`), `tests/test_idempotency.py` |
| Config-driven versioned rating + per-customer/period aggregation | `rating.py`, `price_plans.json`, `store.py` |
| Billing summary + audit trail reconcilable to raw events | `query.py`, `reconcile.py`, `period_close.py` |
| SLIs/metrics, structured logs, alarms | `observability.py`, `GET /v1/ops/metrics`, `docs/SLOS.md`, `docs/RUNBOOK.md` |
| MCP tools `get_usage/get_bill/reconcile/explain_charge` | `src/billing_engine/mcp_server.py` |
| Demo + architecture note with trade-offs + prod path | `tools/demo.sh`, `docs/ARCHITECTURE.md`, `ROADMAP.md` |

## Repo map (production-first lifecycle)

```
src/billing_engine/   # application code (ingest → processor → query → mcp)
infra/cdk/            # IaC for everything (API GW, Lambdas, SQS+DLQ, DDB, S3, alarms)
tests/                # rating, idempotency, reconciliation, api, loss-safety
loadtest/             # locust + k6 (500 ev/s sustained, burst 1000)
tools/                # demo.sh, smoke.sh, failure-injection
docs/                 # ARCHITECTURE, RUNBOOK, SLOS, COST, SECURITY, ADRS, HISTORY
skills/               # agent skills (billing-investigator, reconcile-auditor, ops-incident)
history/              # engineering session log (human + agent decisions)
ROADMAP.md            # POC → pilot → production path
```

Engineering lifecycle followed: `docs/LIFECYCLE.md`
(Plan → Design → Build → Test → Deploy → Operate → Improve).
Every phase has an exit criterion; production concerns (IaC, SLOs, runbook,
reconciliation) were built on Day 1, not bolted on.

## API quick reference

```bash
export KEY=dev-key-123
# single event
curl -s -X POST localhost:8000/v1/usage-events -H "X-API-Key: $KEY" \
  -H Content-Type:application/json -d '{
  "event_id":"evt-001","customer_id":"acme","product_id":"design-suite",
  "metric":"cloud_credits","quantity":10,"timestamp":"2026-10-09T10:00:00Z"}'
# batch (≤100)
curl -s -X POST localhost:8000/v1/usage-events:batch -H "X-API-Key: $KEY" \
  -H Content-Type:application/json -d '{"events":[...]}'
# queries
curl -s "localhost:8000/v1/customers/acme/usage?period=2026-10" -H "X-API-Key: $KEY"
curl -s "localhost:8000/v1/customers/acme/bill?period=2026-10"   -H "X-API-Key: $KEY"
curl -s localhost:8000/v1/ops/metrics -H "X-API-Key: $KEY"
```

## MCP (agentic interface, read-only)

```bash
python -m billing_engine.mcp_server   # stdio MCP server (FastMCP)
# tools: get_usage, get_bill, reconcile, explain_charge
python -m billing_engine.agent_demo --customer acme --period 2026-10
```

## Deploy to AWS (dev)

```bash
cd infra/cdk && pip install -r requirements.txt
cdk bootstrap && cdk deploy BillingDev --context stage=dev
```

## Cut line / non-goals

Per PRD §3 + §8 cut line: no real payments/tax/currency, no multi-region
(documented in ROADMAP), API-key auth only, finance = CSV stub, no UI beyond
CLI/curl. Never cut: idempotency, reconciliation, observability basics.

## License

MIT — prototype for interview/portfolio purposes.
