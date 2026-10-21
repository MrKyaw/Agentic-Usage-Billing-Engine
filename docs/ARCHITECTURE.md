# Architecture Note — Usage-Based Billing POC v0.1

## Bet
At-least-once delivery + idempotent consumer beats chasing exactly-once
transport. DynamoDB conditional writes (local: SQLite PK insert) give
exactly-once *effect*; S3 raw log is the immutable source of truth;
aggregates are derived and rebuildable. That single decision makes
duplicates safe, crashes recoverable, and reconciliation possible.

## Data flow

Two local paths are runnable:

1. **Default (broker-free):** `Producers → API → raw log + SQLite inbox → worker →`
   `SQLite events/rated/aggregates → query / close / MCP`.
2. **Streaming:** `Producers → API → raw log + Kafka usage-events → kafka-worker →`
   `idempotent ledger`, with `Kafka → Flink SQL → deduped read model + window aggregates`.

The AWS target remains:
`Producers → API GW → Ingest Lambda → SQS (+DLQ) → Processor Lambda →`
`DDB events/rated/aggregates + S3 raw log → Query Lambda / close job / MCP`.
The local Kafka/Flink topology maps to Amazon MSK + Managed Service for Apache Flink.

## Key decisions (trade-offs)
1. **DynamoDB over RDS for dedupe/aggregates** — single-digit-ms conditional
   puts + atomic counters at any scale; no connection management in Lambda.
   Cost: no ad-hoc joins (accept; analytics reads the S3 log via Athena).
2. **SQS standard + dedupe vs FIFO** — the AWS blueprint uses standard + app-level
   dedupe (higher throughput, no 300 msg/s FIFO limit per group). The local
   streaming runtime uses Kafka with 20 partitions keyed by `customer_id`; Kafka
   only orders within a partition, so the same conditional insert on `event_id`
   remains the authority. FIFO/ordering is considered only if rating becomes
   order-dependent (it is not — rating is commutative over period totals).
3. **Two-level rating** — immutable unit estimate per event + authoritative
   period settlement (allowance/tiers are period-scoped). Keeps raw records
   auditable while money math stays replayable.
4. **Bucket by event timestamp, not arrival** — late/out-of-order events land in
   the right period; reconciliation catches drift.
5. **Sync vs async ingest** — ingest only validates + enqueues (p99 <200ms);
   all heavy work is async with queue buffering for graceful degradation.
6. **Kafka is a transport, not the billing authority** — the raw log is written
   before publish, broker failures do not fail ingest, and the ledger's
   conditional insert dedupes at-least-once delivery. Flink topics are read
   models, not invoice inputs.

## Streaming semantics and the local runtime (implemented)

Kafka only guarantees order within a partition. The repo implements the
semantics that matter as a tested library (`billing_engine.partitioning`), and
wires them to a runnable Kafka + Flink stack in `infra/streaming/`:

- jump-consistent hash: one owner per key, minimal movement when partitions grow;
- fenced 20 → 40 cutover: pause → drain → bump epoch → resume, stale epochs rejected;
- per-key sequencer: duplicates, gaps, buffered out-of-order records;
- k-way merge + watermark: deterministic cross-partition order, bounded lateness;
- SHA-256 checksum chain: trace the exact broken record and repair the order.

Runtime facts:

- Kafka 3.8 in KRaft mode, 20 partitions, keyed by `customer_id`; raw topic
  `usage-events`, DLQ `usage-events-dlq`.
- `kafka-worker` consumes the raw topic with manual commits and runs the same
  `process_event` path as the SQLite worker.
- Flink 1.19 SQL job #1 dedupes by `event_id` into the compacted
  `usage-events-deduped` upsert topic (analytics read model).
- Flink 1.19 SQL job #2 emits 10-second processing-time operational aggregates
  to `usage-window-aggregates`; it counts raw deliveries, including retries.
- `make stream-smoke` proves bill = $1.00 for 10 unique events, reconciliation
  match, 10 deduped ids from 12 raw events, and 12.0 aggregate quantity.

See [STREAMING.md](STREAMING.md) for commands, ports, topics and the repair runbook.

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

## What remains blueprint-only (and why)
The local Kafka + Flink runtime is implemented and smoke-tested; what is **not**
deployed is the managed AWS topology (MSK + Managed Service for Apache Flink +
S3/Glue/Athena), multi-region (single-region multi-AZ; active-active is a pilot
milestone with a consistency strategy), and real auth/ERP (API key + CSV stub).
The DynamoDB/SQS/S3 adapters and the Lambda bundling gaps are listed in the README.

## Scale math
POC target 500 ev/s: the local Kafka runtime uses 20 partitions, and the SQLite
worker remains the default for laptop-scale runs. The AWS blueprint's SQS +
Lambda concurrency 50 × 50-batch ≈ 2.5k ev/s headroom; DDB on-demand handles
bursts; S3 log ≈ 30 KB/s. See `docs/COST.md` (~$X/1M events).

## Prod path
ROADMAP.md: pilot (MSK + Managed Flink if sustained >5k ev/s, Cognito, Athena
analytics, error budgets) → production (multi-region, ERP invoicing, progressive
delivery).
