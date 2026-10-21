# Kafka + Flink streaming tier

This directory holds the **streaming assets** of the billing engine: the Kafka
topic script, the Flink image and the Flink SQL jobs. The deployment itself is
**not** a separate stack — it is the `streaming` Compose profile of the root
`docker-compose.yml`, so the whole solution is one project:

```text
docker-compose.yml
  api        (always)  monolith HTTP API
  worker     (always)  SQLite inbox poller  ─┐  one entry point:
  kafka      (streaming)  KRaft broker       ├─ BILLING_KAFKA_BOOTSTRAP
  kafka-init (streaming)  creates the topics │
  jobmanager (streaming)  Flink UI/REST      │  switches the API *and* the
  taskmanager(streaming)  Flink workers      │  worker to the Kafka queue
  flink-submit(streaming)  submits the jobs  ┘
```

`make up` runs the monolith (SQLite inbox, no broker). `make up-streaming` starts
the same project with the `streaming` profile and sets
`BILLING_KAFKA_BOOTSTRAP=kafka:9092`, so Kafka becomes the queue, Flink runs the
read-model jobs, and the API, the worker, the database and the volume are
unchanged.

## Topology

```text
producer
  └─ POST /v1/usage-events
       ├─ raw event log (JSONL, source of truth)
       └─ Kafka usage-events (20 partitions, key = customer_id)
            ├─ worker ──> idempotent processor ──> SQLite ledger/aggregates
            ├─ Flink SQL #1 ──> usage-events-deduped (upsert-kafka, compacted)
            └─ Flink SQL #2 ──> usage-window-aggregates (10-second windows)
```

The API writes the raw log before publishing. A Kafka/client failure is logged and
counted but never fails ingest. In this mode the API skips the SQLite inbox
(`enqueue=False`); the raw log and Kafka offsets make replay possible.

## Services and ports

| Service | Port / access | Notes |
|---|---|---|
| `api` | <http://localhost:8000> (`PORT`) | FastAPI; `X-API-Key: dev-key-123` |
| `worker` | internal | Polls the inbox, or consumes `usage-events` in Kafka mode |
| `kafka` | `localhost:29092` (`KAFKA_PORT`) | Host listener; containers use `kafka:9092` |
| `jobmanager` | <http://localhost:8081> (`FLINK_PORT`) | Flink UI and REST |
| `taskmanager` | internal | Flink workers |
| `flink-submit` | one-shot | Submits both SQL jobs and exits (expected) |
| `kafka-init` | one-shot | Creates the four topics before the API/worker start |

## Topics

| Topic | Partitions | Purpose |
|---|---:|---|
| `usage-events` | 20 | Raw immutable events; keyed by `customer_id` |
| `usage-events-dlq` | 20 | Poison payloads and non-retryable validation failures |
| `usage-events-deduped` | 20 | Flink dedupe by `event_id`; compacted upsert read model |
| `usage-window-aggregates` | 20 | Flink 10-second processing-time operational aggregates |

## Commands

Run from the repository root:

```bash
make up-streaming    # whole solution: monolith + Kafka + Flink (streaming profile)
make stream-logs     # follow all services
make stream-smoke    # isolated end-to-end acceptance test
make down            # stop the stack (keep volumes)
make stream-down     # stop the stack and delete its volumes
```

`make up-streaming` uses the default Compose project. To run a second copy, set
`COMPOSE_PROJECT_NAME` and the ports:

```bash
COMPOSE_PROJECT_NAME=billing-stream-2 PORT=18001 KAFKA_PORT=29093 FLINK_PORT=8082 \
  BILLING_KAFKA_BOOTSTRAP=kafka:9092 docker compose --profile streaming up -d --build
```

Check the stack:

```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8081/overview
docker compose --profile streaming ps
```

`flink-submit` exits after submitting the jobs; that is expected. Confirm the jobs
are running in the Flink UI or with:

```bash
docker compose --profile streaming exec jobmanager /opt/flink/bin/flink list
```

## End-to-end smoke test

`tools/kafka_smoke.sh` uses its own Compose project and port, publishes 10 unique
events plus two retries, and asserts the whole path:

```text
PASS both Flink jobs are RUNNING
PASS bill total=1.0 event_count=10
PASS reconcile match (raw log -> Kafka worker -> aggregates)
PASS Flink deduped topic has 10 unique event ids (12 raw events)
PASS Flink aggregate saw all 12 raw events (total_quantity=12.0)
=== KAFKA + FLINK SMOKE OK ===
```

It tears the stack down afterwards. If it fails, it prints the last 120 log lines.
CI has a non-blocking `streaming-smoke` job that runs the same script.

## Guarantees and responsibilities

- **Billing authority:** the ledger's conditional insert on `event_id`. The worker
  commits a Kafka offset only after the processor returns; duplicates are
  acknowledged and dropped.
- **Flink dedupe:** an analytics read model written with `upsert-kafka`. Treat
  `usage-events-deduped` as "latest value wins", not an append-only audit log.
- **Flink aggregates:** 10-second processing-time windows for operational
  visibility. They count raw deliveries (including retries), so they are not used
  to compute invoices.
- **Delivery:** at-least-once for Kafka and Flink sinks. There is no end-to-end
  exactly-once transport claim; the idempotent ledger gives exactly-once *effect*.
- **Raw log:** always the immutable source of truth and the reconciliation input.

## AWS mapping

| Local | AWS target |
|---|---|
| Apache Kafka 3.8 KRaft, 20 partitions, RF 1 | Amazon MSK (KRaft), 3 AZs, RF 3, 20+ partitions |
| Flink 1.19 JobManager/TaskManager + SQL jobs | Amazon Managed Service for Apache Flink |
| `usage-events`, `usage-events-dlq` | MSK topics with retention and a DLQ policy |
| SQLite ledger (local) | DynamoDB events/aggregates (blueprint; not implemented) |
| JSONL raw log (local) | S3 bucket + Glue/Athena catalog (blueprint) |

The local runtime is implemented and tested. The managed AWS deployment is a
blueprint; see the main [README](../../README.md#aws-deployment-blueprint).

## Configuration

| Variable | Default in this stack | Purpose |
|---|---|---|
| `BILLING_API_KEY` | `dev-key-123` | API key for ingest/query |
| `BILLING_KAFKA_BOOTSTRAP` | `kafka:9092` | Enables streaming mode for the API and the worker |
| `BILLING_KAFKA_TOPIC` | `usage-events` | Raw topic |
| `BILLING_KAFKA_DLQ_TOPIC` | `usage-events-dlq` | DLQ topic |
| `BILLING_KAFKA_GROUP` | `billing-worker` | Consumer group |
| `BILLING_KAFKA_OFFSET_RESET` | `earliest` | New-group start |
| `BILLING_KAFKA_PROPERTIES` | `{}` | Extra client properties (JSON) |
| `PORT` | `8000` | Host API port |
| `KAFKA_PORT` | `29092` | Host Kafka port |
| `FLINK_PORT` | `8081` | Host Flink UI port |

The Flink connector jar (`flink-sql-connector-kafka-3.2.0-1.19.jar`) is baked into
the Flink image by `flink/Dockerfile`. The SQL jobs are mounted from
`flink/jobs/window_aggregates.sql`.

## Troubleshooting

- **No deduped messages:** check `flink-submit` logs for SQL errors; recreate it
  with `docker compose --profile streaming up -d --force-recreate flink-submit`.
- **Flink UI unreachable:** wait for `jobmanager` REST startup; the submit container
  waits for `http://jobmanager:8081/overview` before running SQL.
- **Port conflict:** stop the other stack or set `PORT` / `KAFKA_PORT` / `FLINK_PORT`
  and a different Compose project name.
- **No bill movement:** check the worker logs and `/v1/ops/metrics` for
  `kafka_publish_failed` or `kafka_consume_error`. The worker only consumes Kafka
  when `BILLING_KAFKA_BOOTSTRAP` is set.
- **Poison payload:** consume `usage-events-dlq` with
  `kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic usage-events-dlq --from-beginning`.

## Tests

- Unit tests: `tests/test_kafka.py` (fake producer/consumer, no broker needed).
- Compose wiring: `tests/test_compose_stack.py` (parses `docker-compose.yml`).
- End-to-end: `tools/kafka_smoke.sh` (Docker Compose).
- CI: `.github/workflows/ci.yml` job `streaming-smoke` (non-blocking).
