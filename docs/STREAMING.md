# Streaming, partitions and ordering

This note answers the Kafka/event-ordering questions directly and points at the
code that implements each answer. The POC runs on SQS + SQLite (see
[ARCHITECTURE.md](ARCHITECTURE.md)); the *semantics* below live in
[`src/billing_engine/partitioning.py`](../src/billing_engine/partitioning.py) so
they are executable and tested without a broker. The same logic applies when the
transport becomes Kafka/Kinesis.

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

Eval suite: evals 7 and 8 (`tools/eval_suite.py`) re-check scaling and repair on
every CI run.
