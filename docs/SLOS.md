# SLOs / SLIs (POC targets → pilot budgets)

| SLI | POC target | Pilot SLO (30d) | Source |
|---|---|---|---|
| Ingest availability | best-effort dev | 99.9% 2xx on `POST /v1/usage-events` | API GW 5xx / CloudWatch |
| Ingest latency p99 | < 200 ms | < 250 ms | `ingest_ms` histogram |
| Event-to-aggregate p95 | < 5 s | < 10 s | `event_to_aggregate_ms` |
| Correctness: double-count | 0 under 10% dupes | 0 (error budget burns on any) | reconcile job |
| Correctness: loss | 0 after consumer kill | 0 | reconcile + DLQ=0 |
| Freshness: queue age | alarm > 30 s | alarm > 60 s, page > 5 min | SQS age metric or Kafka consumer lag |
| Agent answers | 5/5 scripted via MCP | 95% eval set | `agent_demo` eval |

Ingest latency is read from the service's own `ingest_ms` histogram (the same
SLI the API exports at `/v1/ops/metrics`). The opt-in perf suite
(`pytest -m perf`) asserts that server-side p99 against the 200 ms target and
records the end-to-end client-observed p99 next to it: the client number also
contains the load generator's own scheduling, so it is a diagnostic and a
stall guardrail (3x the target), not the SLO.

Error budget policy (pilot): correctness burn = freeze deploys + incident;
latency burn = scale consumer before features. History of burns: `history/`.
