# ADR-001: At-least-once + idempotent consumer (not exactly-once transport)
Date: 2026-10-09. Status: accepted.
Context: need 0 double-bills under retries/redelivery.
Decision: SQS standard + `try_insert_event` conditional put on `event_id`.
Consequences: duplicates are free; ordering not guaranteed (fine — rating is commutative).
