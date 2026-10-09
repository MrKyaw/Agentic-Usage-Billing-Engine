# Architecture Note — Usage-Based Billing POC v0.1

## Bet
At-least-once delivery + idempotent consumer beats chasing exactly-once
transport. DynamoDB conditional writes (local: SQLite PK insert) give
exactly-once *effect*; S3 raw log is the immutable source of truth;
aggregates are derived and rebuildable. That single decision makes
duplicates safe, crashes recoverable, and reconciliation possible.

## Data flow
`Producers → API GW → Ingest Lambda → SQS (+DLQ) → Processor Lambda →`
`DDB events/rated/aggregates + S3 raw log → Query Lambda / close job / MCP`.

## Key decisions (trade-offs)
1. **DynamoDB over RDS for dedupe/aggregates** — single-digit-ms conditional
   puts + atomic counters at any scale; no connection management in Lambda.
   Cost: no ad-hoc joins (accept; analytics reads the S3 log via Athena).
2. **SQS standard + dedupe vs FIFO** — POC uses standard + app-level dedupe
   (higher throughput, no 300 msg/s FIFO limit per group). FIFO considered for
   per-customer ordering if rating ever becomes order-dependent (it isn't —
   rating is commutative over period totals).
3. **Two-level rating** — immutable unit estimate per event + authoritative
   period settlement (allowance/tiers are period-scoped). Keeps raw records
   auditable while money math stays replayable.
4. **Bucket by event timestamp, not arrival** — late/out-of-order events land in
   the right period; reconciliation catches drift.
5. **Sync vs async ingest** — ingest only validates + enqueues (p99 <200ms);
   all heavy work is async with queue buffering for graceful degradation.

## Hot-partition plan
`aggregates` PK = `customer#period#metric`; whale customers get counter shards
(`#shard-N`, summed at read) — documented, not built in POC. DDB on-demand +
adaptive capacity covers the POC rate; provisioned + autoscaling at pilot.

## What we did NOT build (and why)
Kinesis/Kafka+Flink (throughput not needed at POC rate; SQS is cheaper and
operationally free), multi-region (single-region multi-AZ; active-active is a
pilot milestone with a consistency strategy), real auth/ERP (API key + CSV stub).

## Scale math
POC target 500 ev/s: SQS + Lambda concurrency 50 × 50-batch ≈ 2.5k ev/s headroom.
DDB on-demand handles bursts; S3 log ≈ 30 KB/s. See `docs/COST.md` (~$X/1M events).

## Prod path
ROADMAP.md: pilot (Kinesis if >5k ev/s, Cognito, Athena analytics, error budgets)
→ production (multi-region, Flink windowing, ERP invoicing, progressive delivery).
