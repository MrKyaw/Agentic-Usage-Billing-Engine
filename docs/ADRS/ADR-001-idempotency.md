# ADR-001: At-least-once + idempotent consumer (not exactly-once transport)
Date: 2026-10-09. Status: accepted.
Context: need 0 double-bills under retries/redelivery.
Decision: SQS standard + `try_insert_event` conditional put on `event_id`.
Local variants: the SQLite inbox (default) and the Kafka 20-partition transport
(the `streaming` Compose profile, assets in `infra/streaming/`) use the same
conditional insert; at-least-once delivery is unchanged.
Consequences: duplicates are free; ordering not guaranteed (fine — rating is commutative).
