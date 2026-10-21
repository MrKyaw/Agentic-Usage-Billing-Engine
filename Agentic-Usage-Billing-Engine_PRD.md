# PRD: Usage-Based Billing Platform – POC v0.1

**Status:** Draft | **Timebox:** 2 days (Fri Oct 9 – Sun Oct 11, 2026) | **Owner:** Principal Software Engineer

---

## 1. Background & Problem

The business is moving toward consumption-based pricing. That requires a platform that captures product usage in real time, rates it, aggregates it, and feeds billing, with correctness, reliability, latency, and cost all mattering.

This POC proves the **core architectural bet** on a thin vertical slice: an event-driven, idempotent, auditable usage pipeline on AWS, with operability built in from the start and an agent-facing (MCP) interface.

## 2. Goals (what "done" means in 2 days)

1. Ingest usage events through an API and process them asynchronously at the target POC rate.
2. Guarantee **idempotent, exactly-once-effect** processing (duplicates and retries never double-bill).
3. **Rate** usage against a configurable price plan and **aggregate** per customer per billing period.
4. Produce a **billing summary** and an **audit trail** reconcilable back to raw events.
5. Expose **SLIs/metrics, structured logs, and alarms** for the pipeline.
6. Expose usage and billing queries as an **MCP tool** so an AI agent can answer "what did customer X consume/owe?".
7. Deliver a **demo + short architecture note** documenting trade-offs and a path to production.

## 3. Non-Goals (explicitly out of scope)

- Real payment processing, invoicing PDFs, tax, or currency handling
- Multi-region active-active deployment (documented as a next step only)
- Production auth/SSO integration (a simple API key is enough)
- Finance/ERP integrations (stubbed export file only)
- UI beyond a minimal CLI/curl demo script
- Migration of any legacy system

## 4. Users & Stakeholders

| Persona | Need |
|---|---|
| Product teams (event producers) | Simple, reliable API to emit usage events |
| Finance / commerce | Accurate, auditable, reconcilable charges |
| Platform / SRE | Observability, SLOs, safe operations |
| AI agents / engineers | Query usage and billing through MCP tools |

## 5. Scope: Functional Requirements

### FR1 – Usage Event Ingestion
- `POST /v1/usage-events` (API Gateway → Lambda → queue).
- Event schema: `event_id` (UUID, producer-supplied idempotency key), `customer_id`, `product_id`, `metric` (e.g., `cloud_credits`, `render_minutes`), `quantity`, `timestamp`, optional `metadata`.
- Validate schema; reject malformed events with 4xx; return 202 on accept.
- Batch endpoint accepting up to 100 events.

### FR2 – Idempotent Processing
- Consumer (SQS → Lambda) dedupes on `event_id` using a DynamoDB conditional write.
- Retries and redelivery are safe; poison messages go to a DLQ with an alarm.
- Late/out-of-order events are handled by event timestamp, not arrival time.

### FR3 – Rating
- Config-driven price plans (JSON in repo or DynamoDB): per-metric unit price, optional tiered pricing, optional included allowance.
- Rating is deterministic and versioned (`plan_version` stored with each rated record).

### FR4 – Aggregation & Billing Summary
- Per `customer_id` × `billing_period` × `metric` totals, updated atomically.
- `GET /v1/customers/{id}/usage?period=YYYY-MM` and `GET /v1/customers/{id}/bill?period=YYYY-MM`.
- Period close job (manual trigger OK) that freezes a summary and writes a CSV export (stub for finance integration).

### FR5 – Audit & Reconciliation
- Immutable raw-event log (S3, partitioned by date) and rated-record table.
- Reconciliation script: recompute totals from raw events and diff against aggregates; report mismatches.

### FR6 – Observability & Operability
- Metrics: ingest rate, processing lag (queue age), end-to-end latency (p50/p95/p99), duplicate rate, DLQ depth, error rate.
- Structured JSON logs with correlation ID (`event_id`).
- Alarms: DLQ > 0, queue age > threshold, error rate > threshold.
- One-page runbook for the top 3 failure modes.

### FR7 – Agentic Interface (MCP)
- MCP server exposing tools: `get_usage(customer_id, period)`, `get_bill(customer_id, period)`, `reconcile(customer_id, period)`, `explain_charge(customer_id, event_id)`.
- Read-only by default; no mutating tools in the POC.
- Demo: an agent answers a natural-language billing question using these tools.

## 6. Non-Functional Requirements (POC targets)

| Area | Target |
|---|---|
| Throughput | ≥ 500 events/sec sustained in load test (burst to 1,000) |
| Latency | Ingest API p99 < 200 ms; event-to-aggregate p95 < 5 s |
| Correctness | 0 double-counts under 10% injected duplicates; 0 lost events under injected failures |
| Availability | Single-region, multi-AZ managed services; graceful degradation via queue buffering |
| Security | API key auth, least-privilege IAM, encryption at rest and in transit, no PII in logs |
| Cost | Serverless pay-per-use; record estimated cost per 1M events |
| Maintainability | IaC for everything; unit tests on rating and idempotency logic |
| Streaming resilience | Deterministic per-key ordering; 20 → 40 partition scaling moves ~50% of keys with one owner each; bounded lateness; checksum trace + repair |
| Data lake | Immutable partitioned files, atomic snapshots, time travel, compaction, per-file checksums; Iceberg-shaped and format-neutral |
| Agentic governance | 100% coverage + reconcile + lint/TODO gates; HMAC-signed deploy verdict; risk-tiered human review (2 reviewers on money paths) |
| Production guardrails | Automated 10-eval suite, strict CI/CD gates, OTLP/HTTP OpenTelemetry export with bounded failure isolation |

## 7. Proposed Architecture

```
Producers ─► API Gateway ─► Ingest Lambda ─► SQS (FIFO or standard + dedupe) ─► DLQ
                                  │                    │
                                  ▼                    ▼
                         S3 raw event log      Processor Lambda
                                                  │   ├─ dedupe (DynamoDB conditional put)
                                                  │   ├─ rate (price plan)
                                                  │   └─ aggregate (DynamoDB atomic update)
                                                  ▼
                        DynamoDB: events / rated_records / aggregates
                                                  │
                    ┌─────────────────────────────┼───────────────────────┐
                    ▼                             ▼                       ▼
            Query API (Lambda)           Period-close job          MCP server (tools)
                                         + CSV export               ─► AI agent
                    CloudWatch metrics / logs / alarms (all components)
```

**Key design decisions (to be recorded in the architecture note):**
- At-least-once delivery + idempotent consumer, rather than chasing exactly-once transport.
- DynamoDB for dedupe and aggregates (conditional writes, atomic counters); S3 as the source of truth for replay.
- Aggregates are derived and rebuildable from the raw log, which is what makes reconciliation possible.
- Alternative considered for production: Kinesis/Kafka + Flink for higher throughput and windowing. A local Kafka 3.8 (KRaft, 20 partitions) + Flink 1.19 SQL runtime is implemented and smoke-tested as the `streaming` profile of the monolith's own Compose project (assets in `infra/streaming/`, started with `make up-streaming`); the partition/ordering/checksum semantics are also executable in `billing_engine.partitioning`. The managed AWS deployment (MSK + Managed Service for Apache Flink) remains deferred.

**Suggested stack:** Python or Go (pick one), AWS CDK or Terraform, pytest, k6 or Locust for load testing.

## 8. Two-Day Plan

### Day 1 (Fri evening → Sat): Core pipeline
| Block | Deliverable |
|---|---|
| 0–1h | Repo scaffold, IaC skeleton, event schema, price plan config |
| 1–4h | Ingest API + queue + DLQ + raw log to S3 |
| 4–7h | Processor: idempotent dedupe, rating, aggregation |
| 7–9h | Query endpoints; basic unit tests (rating, dedupe) |
| 9–10h | Deploy to dev account; smoke test end-to-end |

**Day 1 exit criterion:** an event posted via curl appears in the customer's aggregate, and a duplicate does not change it.

### Day 2 (Sun): Hardening, agentic layer, demo
| Block | Deliverable |
|---|---|
| 0–2h | Metrics, structured logs, alarms, DLQ handling |
| 2–4h | Reconciliation script + period-close CSV export |
| 4–6h | MCP server with 4 tools; agent demo prompt set |
| 6–8h | Load test + failure injection (duplicates, consumer crash, out-of-order) |
| 8–10h | Architecture note, runbook, README, demo script and recording |

**Day 2 exit criterion:** the demo runs live end-to-end and all success metrics in section 9 are measured.

### Cut line (if time runs short, drop in this order)
1. Tiered pricing (keep flat unit price)
2. Batch ingest endpoint
3. `explain_charge` MCP tool
4. Period-close CSV export (keep reconciliation)

**Never cut:** idempotency, reconciliation, observability basics.

## 9. Success Metrics

| Metric | Pass condition |
|---|---|
| Duplicate safety | 10k events with 10% duplicates → aggregate equals expected total exactly |
| Loss safety | Kill the processor mid-run → zero lost events after recovery |
| Reconciliation | Recomputed totals match aggregates with 0 diffs |
| Performance | Meets section 6 throughput and latency targets in load test |
| Observability | Dashboard shows lag, error rate, and DLQ depth; a test alarm fires |
| Agentic | Agent correctly answers 5 of 5 scripted billing questions via MCP |
| Documentation | Architecture note and runbook reviewed by a peer |

## 10. AI-Assisted Development Approach

- Use coding agents for scaffolding, IaC, test generation, and load-test scripts.
- Human-owned (not delegated): idempotency design, rating correctness, failure-mode analysis, security review.
- Record in the architecture note what the agent accelerated and where its output needed correction.

## 11. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Scope creep in a 2-day window | Miss deadline | Strict non-goals and cut line above |
| AWS account/IAM delays | Blocks Day 1 | Verify access and bootstrap IaC in the first hour; fall back to LocalStack |
| Hot-partition on DynamoDB aggregates | Throttling in load test | Shard counters per customer; document the production approach |
| Out-of-order or late events | Incorrect period totals | Bucket by event timestamp; reconciliation catches drift |
| Load test cost or limits | Skewed results | Cap the test duration; report extrapolated numbers clearly |

## 12. Open Questions

1. Which AWS account and region for the POC, and who approves the IAM roles?
2. Preferred language (Python vs. Go) given the team's stack?
3. Real product metric names and price-plan examples, or synthetic ones?
4. Should the MCP server be demoed through Claude Desktop or a custom agent harness?
5. Is Kafka/Kinesis comparison expected in the write-up, or is a mention enough?

## 13. Post-POC Roadmap (not in scope now)

- Multi-region resilience and data consistency strategy
- Managed streaming deployment (Amazon MSK + Managed Service for Apache Flink) and a Kinesis comparison if throughput requires it; the local Kafka + Flink tier already runs inside the monolith's Compose project
- Finance/ERP integration and invoice generation
- Full authN/authZ, tenant isolation, SLO definitions and error budgets
- Progressive delivery, deployment safety, and cost-optimization pass
