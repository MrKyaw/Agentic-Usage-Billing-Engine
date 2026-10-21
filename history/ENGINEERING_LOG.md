# Engineering history (human + agent decisions)

## 2026-10-09 — v0.1 scaffold (agent-assisted, human-reviewed)
- Scaffolded thin slice first (ingest → process → query) before extras.
- Agent drafted rating with per-event allowance — human corrected to period-scoped
  two-level rating (ADR-002) after writing the tiered test by hand.
- Agent drafted ack-before-write queue — human reordered to transactional
  raw-log+enqueue and write-then-ack (loss-safety test proves crash recovery).
- MCP kept read-only over agent's mutating suggestion (audit risk).
- Exit: `make test && make demo` green; Day-1 criterion met.

## 2026-10-10 — stateful test found a real redrive gap (agent-fixed, human-spec'd)
- `BillingLifecycle` invariant failed: 6x crash-claims on one row burned the
  SQS-style retry budget, DLQing a VALID event with inbox drained → stored
  (0.0,0) vs expected (1.0,1). System behaved per its redrive contract; the gap
  was the missing recovery path the RUNBOOK already promised ("replay DLQ").
- Fix (product, not test-only): `Store.replay_dlq()` + `processor.redrive_dlq()`
  move DLQ rows back with a fresh budget; duplicates stay safe via conditional
  put, poison re-DLQs bounded-ly. Test model gained the operator `redrive` rule
  and scopes the exactness invariant to the redriven state.
- Proof: crash-loop → DLQ → redrive → drain gives exact totals + clean
  reconcile; `tests/test_properties.py` green.
- Lesson: hypothesis state machines earn their keep — the hand-written tests
  never interleaved 6 crashes before a process.

## 2026-10-21 — Kafka + Flink runtime, embedded in the monolith (agent-built, human-reviewed)
- Earlier the streaming tier lived in its own Compose project next to the
  monolith, so "the system" was two stacks, two ports and two workers.
  Now `docker-compose.yml` is the whole solution: the `streaming` profile adds
  Kafka (KRaft, 20 partitions) and Flink 1.19 SQL jobs to the *same* project,
  and one variable (`BILLING_KAFKA_BOOTSTRAP`) switches the API **and** the
  worker to the Kafka queue. `python -m billing_engine.worker` is now the single
  worker entry point (inbox poller or Kafka consumer).
- Decision: the raw log stays the source of truth and the ledger's conditional
  insert stays the authoritative dedupe; Flink output is a read model only.
- Proof: `make stream-smoke` (isolated project) — bill $1.00 for 10 unique
  events, reconciliation match, 10 deduped ids from 12 raw events, 12.0
  aggregate quantity; `make compose-smoke` still green for the monolith path;
  `tests/test_compose_stack.py` guards the wiring; 291 tests, 100% line and
  branch coverage.

## 2026-10-21 — CI gates fixed after a Ruff release (agent-built, human-reviewed)
- `ruff check src tests tools` started failing with 80 findings because a new
  Ruff release enables extra rule families by default. Pinned `ruff==0.11.10`
  and made the baseline explicit (`[tool.ruff.lint] select = E4,E7,E9,F`).
- The TODO/FIXME guardrail matched the *words* in `governance.py`'s own
  docstring, not real comments; it now greps for `#.*(TODO|FIXME)`.
- Proof: CI run 39639636341 — `test`, `guardrails`, `compose-smoke`,
  `streaming-smoke`, `cdk-synth` all green (the perf job stays non-gating: it
  tracks a shared-runner load measurement, and local perf passes at
  p99 80.8 ms vs the 200 ms SLO).

## How to log future entries
Append date, change, who decided (human/agent), proof (`demo.sh`/test/ADR link).
Every production incident gets an entry + RUNBOOK update.
