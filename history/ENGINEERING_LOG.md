# Engineering history (human + agent decisions)

## 2026-10-09 — v0.1 scaffold (agent-assisted, human-reviewed)
- Scaffolded thin slice first (ingest → process → query) before extras.
- Agent drafted rating with per-event allowance — human corrected to period-scoped
  two-level rating (ADR-002) after writing the tiered test by hand.
- Agent drafted ack-before-write queue — human reordered to transactional
  raw-log+enqueue and write-then-ack (loss-safety test proves crash recovery).
- MCP kept read-only over agent's mutating suggestion (audit risk).
- Exit: `make test && make demo` green; Day-1 criterion met.

## How to log future entries
Append date, change, who decided (human/agent), proof (`demo.sh`/test/ADR link).
Every production incident gets an entry + RUNBOOK update.
