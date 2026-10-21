# Data lake and table formats

## Which data lake?

**Apache Iceberg first**, then Delta Lake if the estate is Databricks-only, Hudi
when upserts/CDC dominate. The choice is about the *table format*, not the
storage: all three sit on object storage (S3/GCS/ADLS) and all three give
snapshots, schema evolution and time travel.

| Format | Best fit | Trade-off |
|---|---|---|
| **Apache Iceberg** | multi-engine (Spark, Trino, Flink, Athena), hidden partitioning, large audits | more metadata services to run (catalog) |
| **Delta Lake** | Databricks/Spark-centric teams | best tooling inside that ecosystem, weaker outside |
| **Apache Hudi** | heavy upserts/CDC, incremental pulls | more tuning (file groups, compaction) |
| **Plain Parquet** | simple immutable exports | no snapshots, no atomic commits, no time travel |

For a billing ledger the deciding properties are: immutable snapshots,
engine-neutral reads for finance/analytics, and partition pruning. Iceberg gives
all three without locking the ledger to one vendor.

## "What is Apache X?" — Apache Iceberg

Iceberg is an **open table format**: a metadata layer over Parquet/Avro/ORC
files. Instead of listing an S3 prefix (and hoping no one wrote a partial file),
a reader opens a **metadata file → manifest list → manifests → data files**.
That indirection buys:

- **Atomic commits** — a new snapshot appears only when its manifest is written;
  readers never see half a commit.
- **Snapshot isolation & time travel** — query `as of` a snapshot id/timestamp.
- **Hidden partitioning** — partition by `dt`/`customer_id` without exposing the
  columns in every query; pruning is automatic.
- **Schema evolution** — add/rename/drop columns without rewriting data.
- **Engine neutrality** — the same table is readable by Spark, Trino, Flink,
  Athena, Snowflake (via catalog) and DuckDB.

Delta Lake and Hudi solve overlapping problems with different metadata layouts;
the repo's writer keeps the same contract (immutable files + atomic manifest) so
the POC can move to any of them without changing callers.

## This repo's implementation

[`src/billing_engine/datalake.py`](../src/billing_engine/datalake.py) is a
local, format-neutral lake writer used for audit and analytics:

```
<root>/<table>/dt=YYYY-MM-DD/customer_id=<id>/part-<uuid>.jsonl
<root>/<table>/_manifest.json          # ordered snapshot list
```

Each manifest entry records `path`, `rows`, `sha256`, `dt`, `customer_id`,
`min_ts`, `max_ts`. Commit is `write temp manifest → os.replace`, so a crash
leaves the previous snapshot intact.

```python
from billing_engine.datalake import LakeTable

table = LakeTable("data/lake", "usage_events")      # fmt="iceberg" (default)
table.append(events)                                 # snapshot 1
table.append(more_events)                            # snapshot 2
table.scan(customer_id="acme", dt="2026-10-09")      # partition pruning
table.scan(as_of=1)                                  # time travel
table.compact()                                      # merge small files
table.verify()                                       # [] or corruption list
table.properties()                                   # catalog properties
```

Guarantees:

- **Snapshot isolation** — readers use the latest committed manifest.
- **Time travel** — compaction writes a new snapshot; old snapshots still point
  at their original files, so history is never rewritten.
- **Checksums** — `verify()` reports `missing:`, `checksum:` and `rows:` errors;
  a truncated or partial file is caught before finance reads it.
- **Partition pruning** — `scan(dt=..., customer_id=...)` opens only matching
  files, which is what keeps Athena/Spark cost proportional to the query.
- **Format metadata** — `properties()` returns Iceberg/Delta/Hudi catalog keys
  (`format-version`, `delta.appendOnly`, `hoodie.table.type`, …).

## Production mapping

| Local POC | AWS/production |
|---|---|
| JSONL files under `data/lake` | Parquet under `s3://<bucket>/<table>/` |
| `_manifest.json` | Iceberg metadata + manifest list in Glue/REST/Nessie catalog |
| `LakeTable.append` | Spark/Flink Iceberg writer, commit per micro-batch |
| `compact()` | Iceberg rewrite/compaction job |
| `verify()` | nightly checksum job + Athena audit query |
| raw JSONL log (`data/raw-log`) | immutable S3 source of truth, replayed into the table |

The raw event log remains the system of record; the lake is a derived,
rebuildable projection. That is why a bad compaction can be repaired by replay
without touching billing state.

## Code and tests

| Concern | Code | Tests |
|---|---|---|
| Partitioned writes + checksums | `LakeTable._write_rows`, `append` | `test_datalake.py::test_append_partitions_*` |
| Atomic snapshot manifest | `_read_snapshots`, `_write_snapshots` | `test_append_partitions_*`, `test_scan_*` |
| Partition pruning | `scan(dt, customer_id)` | `test_scan_prunes_partitions_*` |
| Time travel | `scan(as_of=...)`, `history()` | `test_scan_prunes_partitions_*`, `test_compaction_*` |
| Compaction | `compact()` | `test_compaction_*` |
| Corruption detection | `verify()` | `test_verify_detects_*` |
| Format catalog properties | `properties()`, `TABLE_FORMATS` | `test_catalog_properties_*` |

Eval suite: eval 9 (`tools/eval_suite.py`) exercises append, time travel,
compaction and verification on every CI run.
