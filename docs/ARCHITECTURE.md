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

## Streaming semantics (implemented, broker-free)

Kafka only guarantees order within a partition. The repo implements the
semantics that matter as a tested library (`billing_engine.partitioning`) so the
same design moves to Kafka/Kinesis without a rewrite:

- jump-consistent hash: one owner per key, minimal movement when partitions grow;
- fenced 20 → 40 cutover: pause → drain → bump epoch → resume, stale epochs rejected;
- per-key sequencer: duplicates, gaps, buffered out-of-order records;
- k-way merge + watermark: deterministic cross-partition order, bounded lateness;
- SHA-256 checksum chain: trace the exact broken record and repair the order.

See [STREAMING.md](STREAMING.md) for the scaling/repair runbook.

## Hot-partition plan
`aggregates` PK = `customer#period#metric`; whale customers get counter shards
(`#shard-N`, summed at read) — documented, not built in POC. DDB on-demand +
adaptive capacity covers the POC rate; provisioned + autoscaling at pilot.

## Data lake

The immutable raw log is the source of truth; `billing_engine.datalake` projects
it into Iceberg-shaped snapshots (immutable partitioned files + atomic manifest)
with partition pruning, time travel, compaction and per-file checksums. Local
JSONL maps to Parquet + an Iceberg catalog in production. See
[DATALAKE.md](DATALAKE.md).

## What we did NOT build (and why)
Kafka/Kinesis **brokers** + Flink (throughput not needed at POC rate; SQS is
cheaper and operationally free — the partition/ordering/checksum semantics are
implemented and tested in `partitioning.py`), multi-region (single-region
multi-AZ; active-active is a pilot milestone with a consistency strategy), real
auth/ERP (API key + CSV stub).

## Scale math
POC target 500 ev/s: SQS + Lambda concurrency 50 × 50-batch ≈ 2.5k ev/s headroom.
DDB on-demand handles bursts; S3 log ≈ 30 KB/s. See `docs/COST.md` (~$X/1M events).

## Prod path
ROADMAP.md: pilot (Kinesis if >5k ev/s, Cognito, Athena analytics, error budgets)
→ production (multi-region, Flink windowing, ERP invoicing, progressive delivery).
