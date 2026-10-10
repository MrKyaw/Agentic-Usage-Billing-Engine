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

## How to log future entries
Append date, change, who decided (human/agent), proof (`demo.sh`/test/ADR link).
Every production incident gets an entry + RUNBOOK update.
