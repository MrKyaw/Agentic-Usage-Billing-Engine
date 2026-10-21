# Streaming, partitions and ordering

This note answers the Kafka/event-ordering questions directly and points at the
code that implements each answer. Two transports are runnable:

- **Default local mode:** SQLite inbox + `worker.py` (no broker).
- **Streaming mode:** Kafka (KRaft) is the queue, the worker applies the
  idempotent ledger, and Flink SQL maintains a deduped read model plus 10-second
  operational aggregates. It is the `streaming` profile of the same
  `docker-compose.yml` project (Kafka/Flink assets in
  [`infra/streaming/`](../infra/streaming/)) and starts with `make up-streaming`.

The *semantics* below live in
[`src/billing_engine/partitioning.py`](../src/billing_engine/partitioning.py) so
they are executable and tested without a broker. The same logic applies to both
modes and to Kinesis.

## 0. Deployed Kafka + Flink runtime

The local runtime is a real streaming stack inside the monolith's own Compose
project — the same API, worker, database and volume, with Kafka and Flink added
by the `streaming` profile. It is not a design sketch:

```text
producer
  └─ POST /v1/usage-events
       ├─ raw event log (JSONL, source of truth)
       └─ Kafka usage-events (20 partitions, key = customer_id)
            ├─ worker ──> idempotent processor ──> SQLite ledger/aggregates
            ├─ Flink SQL #1 ──> usage-events-deduped (upsert-kafka, compacted)
            └─ Flink SQL #2 ──> usage-window-aggregates (10-second windows)
```

The API writes the raw log **before** publishing, and a broker/client failure is
logged and counted but never fails ingest. In streaming mode the API skips the
SQLite inbox (`enqueue=False`); the raw log is still durable and the Kafka worker
can replay it from offsets. A failed publish is therefore a delayed event, not a
lost one: alert on `kafka_publish_failed` and replay the raw log (or reset the
worker group) once Kafka recovers.

| Topic | Partitions | Purpose |
|---|---:|---|
| `usage-events` | 20 | Raw immutable events; keyed by `customer_id` |
| `usage-events-dlq` | 20 | Poison payloads and non-retryable validation failures |
| `usage-events-deduped` | 20 | Flink dedupe by `event_id`; compacted upsert read model |
| `usage-window-aggregates` | 20 | Flink 10-second processing-time operational aggregates |

| Service | Port / access | Notes |
|---|---|---|
| API | <http://localhost:8000> (`PORT`) | `X-API-Key: dev-key-123` |
| Kafka (host listener) | `localhost:29092` (`KAFKA_PORT`) | Container-to-container uses `kafka:9092` |
| Flink UI | <http://localhost:8081> (`FLINK_PORT`) | JobManager REST + dashboard |
| `worker` | internal | Consumes `usage-events` with manual commits when Kafka is enabled |

### Commands

```bash
make up-streaming    # whole solution: monolith + Kafka + Flink (streaming profile)
make stream-smoke    # isolated Compose project: publish, dedupe, aggregate, reconcile
make stream-logs     # follow all services
make stream-down     # stop and delete the streaming volumes
```

`make stream-smoke` is the acceptance test. It publishes 10 unique events plus
two retries and asserts:

```text
PASS both Flink jobs are RUNNING
PASS bill total=1.0 event_count=10
PASS reconcile match (raw log -> Kafka worker -> aggregates)
PASS Flink deduped topic has 10 unique event ids (12 raw events)
PASS Flink aggregate saw all 12 raw events (total_quantity=12.0)
=== KAFKA + FLINK SMOKE OK ===
```

The unit tests in [`tests/test_kafka.py`](../tests/test_kafka.py) drive the
producer/consumer with fake clients, so the suite still runs without Docker or a
broker, and [`tests/test_compose_stack.py`](../tests/test_compose_stack.py) guards
the Compose wiring (one switch for the transport, topics before the API, 20
partitions). CI also has a non-blocking `streaming-smoke` job that runs the
Compose stack end to end.

### What is authoritative, and what is a read model

- **Billing authority:** the conditional insert on `event_id` in the SQLite
  ledger. The worker commits Kafka offsets only after the processor returns, and
  a duplicate is a successful no-op. At-least-once delivery therefore has
  exactly-once *effect*.
- **Flink dedupe:** an analytics read model. `ROW_NUMBER()` writes an
  upsert/changelog stream, so consumers must treat `usage-events-deduped` as
  "latest value wins", not as an append-only audit log. The raw log remains the
  audit source.
- **Flink aggregates:** operational visibility (volume, event count, max
  quantity) in 10-second processing-time windows. They intentionally count raw
  deliveries, including retries, so they are not used to compute invoices.
- **Delivery guarantee:** at-least-once for Kafka and Flink sinks; no claim of
  end-to-end exactly-once transport. The idempotent ledger is what makes the
  money correct.

### AWS mapping (blueprint)

The Compose stack maps directly to managed services:

| Local | AWS target |
|---|---|
| `apache/kafka` KRaft, 20 partitions, RF 1 | Amazon MSK (KRaft), 3 AZs, RF 3, 20+ partitions |
| Flink 1.19 JobManager/TaskManager + SQL jobs | Amazon Managed Service for Apache Flink |
| `usage-events`, `usage-events-dlq` | MSK topics with retention and DLQ policy |
| S3/Glue/Iceberg (not part of the Compose stack) | S3 raw-log bucket + Glue/Athena catalog |

The AWS CDK deployment is still a **blueprint** (see the README); the local Kafka
and Flink *runtime* is implemented, smoke-tested and exercised in CI.

### Streaming runbook

1. `make stream-logs` — look for `kafka_worker_started` and the two Flink jobs in
   the Flink UI.
2. No bill movement: check the worker logs for consumer group rebalances and
   `/v1/ops/metrics` for `kafka_publish_failed` or `kafka_consume_error`.
3. Poison event: inspect `usage-events-dlq` with
   `docker compose --profile streaming exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic usage-events-dlq --from-beginning`.
4. Replay after a fix: reset the worker group to `earliest`, or replay the raw log
   into `usage-events`; the ledger will ignore duplicates.
5. Flink jobs stopped: recreate `flink-submit` (it exits after submitting) with
   `docker compose --profile streaming up -d --force-recreate flink-submit`;
   state is checkpointed to the `flink-checkpoints` volume.

## 1. Dynamic scaling — "click a button and Kafka grows"

Adding brokers adds disk/network; adding **partitions** adds parallelism. In
Kafka you can only increase partition count (never decrease without a rewrite),
and existing keys keep their partition until the mapping changes. Growing the
cluster is therefore two separate operations:

1. **Brokers** join the cluster and the controller moves partition *leadership*
   to them. No message is copied by the producer and no key changes.
2. **Partitions** increase (20 → 40). Each key's target may change; that is the
   migration described below.

`plan_scale(keys, 20, 40)` computes exactly which keys move and which stay.

## 2. Partition and key choice

| Granularity | Key | Ordering | Parallelism | Used for |
|---|---|---|---|---|
| `customer` (default) | `customer_id` | all events for a customer ordered | one hot customer can cap one partition | billing/rating |
| `customer_metric` | `customer_id\|metric` | per customer+metric | more | metric-isolated pipelines |
| `event` | `event_id` | none | maximum | only because rating is commutative over period totals |

Code: `partition_key(event, granularity)`. Billing uses `customer_id` because
allowances, credits and corrections are order-sensitive per customer; keying by
`event_id` would spread perfectly but destroy that order.

## 3. Scaling 20 → 40 — what happens to the key?

`jump_hash(key, buckets)` (Lamping–Veach consistent hashing) maps every key to
exactly one bucket and moves the minimum number of keys when the count changes.
Doubling 20 → 40 **must** move about half the keys — there are twice as many
buckets and every bucket must receive keys — but it never splits a key across
partitions:

```
plan = plan_scale(customer_ids, old_buckets=20, new_buckets=40, epoch=1)
plan.moves    # key, from_partition, to_partition
plan.stable   # keys that keep the same partition
plan.moved_fraction  # ~0.5 for a doubling
```

Cutover is fenced, not a live rehash:

```
producers paused  ->  old partitions drained  ->  epoch bumped  ->  producers resume
```

`CutoverBarrier` enforces this: while fenced, `accept()` returns `False`; after
activation only routes with the new epoch are accepted. A producer still holding
the old map is rejected instead of writing the same key to a second partition
(which is how "skip and wrong order" bugs happen).

**Rollback:** keep the old partition map and epoch; if the new map misbehaves,
fence again, restore the previous `Partitioner`, and replay from the last
committed offsets. Because each key has one owner per epoch, replay is
idempotent.

## 4. Out-of-order events across partitions

Kafka guarantees order **within** a partition, not across them. Two mechanisms
make cross-partition order explicit:

- `merge_partitions(streams)` — deterministic k-way merge by
  `(event_timestamp, event_id)` with the stream index as tie-break. Each
  partition's internal order is preserved; the global order is reproducible.
- `WatermarkWindow(lateness_s)` — buffers events until
  `max_seen_event_time - lateness` passes them, emits in event-time order, and
  routes anything older to a `late` side output. Late events are never silently
  reordered into a closed window.

Billing is tolerant by design: rating is commutative over period totals, and
events are bucketed by **event time**, not arrival time. Ordering matters for
credits/adjustments, which is why the default key is the customer.

## 5. "If the key points to the wrong partition, we lose ordering"

That is a producer/partitioner bug, not a broker feature. This repo detects it
instead of hoping:

- `PerKeySequencer.observe(key, seq)` returns `in_order`, `duplicate` or
  `buffered` (out-of-order), and releases contiguous runs when a gap fills.
- A key observed on a new partition without an epoch bump is a configuration
  error; `CutoverBarrier.accept()` rejects the stale epoch.
- `ledger_integrity(events)` returns a content digest plus duplicate counts;
  `GET /v1/ops/ledger-integrity` exposes it for operators.

## 6. Checksums — trace and repair broken ordering

Each record is chained:

```
checksum[i] = sha256(checksum[i-1] + 0x1f + canonical_json(record[i]))
```

- `build_chain(records)` assigns `seq` and checksums in order.
- `verify_chain(records)` returns every break with index, expected/actual hash
  and kind (`sequence` or `checksum`). Mutating one record invalidates it and
  every later record, which localises the damage.
- `repair_chain(records)` sorts by `seq`, rebuilds the chain, and reports
  `order_changed` plus the original breaks — so the repair itself is auditable.

**Runbook (broken order):**

1. `verify_chain` on the partition/segment → note first break index and kind.
2. If `sequence`: fetch the missing offsets; replay from the last good offset
   into a side topic, then `repair_chain` to rebuild the chain.
3. If `checksum`: compare the raw payload with the producer's copy; quarantine
   the record, replay the producer's original, and re-verify.
4. Only then resume the consumer. Reconciliation (`reconcile`) is the final
   proof that money still matches the raw log.

## 7. Code and tests

| Concern | Code | Tests |
|---|---|---|
| Hash / one owner per key | `jump_hash`, `Partitioner` | `test_streaming.py::test_jump_hash_*`, `test_scaling_20_to_40_*` |
| 20 → 40 migration plan | `plan_scale`, `ScalePlan` | `test_scaling_20_to_40_*`, `test_scale_plan_*` |
| Fenced cutover / stale epoch | `CutoverBarrier` | `test_cutover_barrier_*` |
| Key choice | `partition_key` | `test_partition_key_granularities` |
| Per-key order / gaps / duplicates | `PerKeySequencer` | `test_per_key_sequencer_*` |
| Cross-partition merge | `merge_partitions` | `test_merge_partitions_*` |
| Bounded lateness | `WatermarkWindow` | `test_watermark_window_*` |
| Checksum trace / repair | `build_chain`, `verify_chain`, `repair_chain` | `test_checksum_chain_*`, `test_repair_chain_*` |
| Ledger digest | `ledger_integrity`, `GET /v1/ops/ledger-integrity` | `test_ledger_integrity_*` |
| Kafka producer/consumer | `kafka_transport.py` | `test_kafka.py::test_publish_*`, `test_consume_*` |
| API bridge / streaming ingest | `api.py`, `ingest.py`, `processor.py` | `test_kafka.py::test_ingest_*`, `test_api_*` |
| Kafka worker | `kafka_worker.py` | `test_kafka.py::test_kafka_worker_*` |
| Flink dedupe + aggregates | `infra/streaming/flink/jobs/window_aggregates.sql` | `tools/kafka_smoke.sh` (Compose, end to end) |

Eval suite: evals 7 and 8 (`tools/eval_suite.py`) re-check scaling and repair on
every CI run. The Docker smoke test is in `tools/kafka_smoke.sh` and the CI
`streaming-smoke` job.
