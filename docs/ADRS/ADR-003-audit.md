# ADR-003: Raw log is source of truth; aggregates are derived
Date: 2026-10-09. Status: accepted.
Context: finance needs auditability; aggregates must survive corruption.
Decision: S3 (local: JSONL dir) immutable log; `reconcile` recomputes and diffs.
Consequences: never hand-edit aggregates; rebuild by replay. Storage cost trivial.
