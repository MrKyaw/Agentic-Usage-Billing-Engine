"""Data-lake table with Iceberg-shaped snapshots — audit layer.

The raw event log is the source of truth; this module is the analytics/audit
projection of it. It writes immutable files under Hive-style partitions and
commits a manifest atomically, which is the same contract Iceberg, Delta Lake
and Hudi expose:

    <root>/<table>/dt=YYYY-MM-DD/customer_id=<id>/part-<uuid>.jsonl
    <root>/<table>/_manifest.json   # ordered snapshot list

Properties that make it safe for billing:

- **Snapshot isolation** — readers see the latest committed manifest; a crash
  before ``os.replace`` leaves the previous snapshot untouched.
- **Time travel** — every snapshot keeps its file list, so ``scan(as_of=N)``
  reproduces the ledger as of that commit (compaction never rewrites history).
- **Checksums** — each file carries ``sha256`` and a row count; ``verify()``
  detects truncation, corruption and partial writes.
- **Partition pruning** — ``scan(dt=..., customer_id=...)`` only opens matching
  files, which is what keeps Athena/Spark queries cheap at scale.

Why Iceberg first: hidden partitioning + snapshot isolation + engine-neutral
metadata (Spark, Trino, Flink, Athena) fit a ledger that must be replayable by
several teams. Delta is the choice inside a Databricks-only estate; Hudi when
upserts/CDC dominate. The layout here is deliberately format-neutral.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from billing_engine.partitioning import canonical_json

TABLE_FORMATS: dict[str, dict[str, str]] = {
    "iceberg": {
        "format-version": "2",
        "write.format.default": "parquet",
        "partition-spec": "dt,customer_id",
        "write.target-file-size-bytes": "134217728",
    },
    "delta": {
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "2",
        "delta.appendOnly": "true",
        "partitionColumns": "dt,customer_id",
    },
    "hudi": {
        "hoodie.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.partitionpath.field": "dt,customer_id",
        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    },
}


@dataclass(frozen=True)
class LakeFile:
    path: str
    rows: int
    sha256: str
    dt: str
    customer_id: str
    min_ts: str
    max_ts: str

    def as_dict(self) -> dict[str, Any]:
        return {"path": self.path, "rows": self.rows, "sha256": self.sha256,
                "dt": self.dt, "customer_id": self.customer_id,
                "min_ts": self.min_ts, "max_ts": self.max_ts}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LakeFile":
        return cls(path=str(data["path"]), rows=int(data["rows"]),
                   sha256=str(data["sha256"]), dt=str(data["dt"]),
                   customer_id=str(data["customer_id"]),
                   min_ts=str(data["min_ts"]), max_ts=str(data["max_ts"]))


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: int
    created_at: float
    message: str
    files: tuple[LakeFile, ...]

    @property
    def rows(self) -> int:
        return sum(f.rows for f in self.files)

    def as_dict(self) -> dict[str, Any]:
        return {"snapshot_id": self.snapshot_id, "created_at": self.created_at,
                "message": self.message, "files": [f.as_dict() for f in self.files]}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Snapshot":
        return cls(snapshot_id=int(data["snapshot_id"]),
                   created_at=float(data["created_at"]), message=str(data["message"]),
                   files=tuple(LakeFile.from_dict(f) for f in data["files"]))


class LakeTable:
    """A single append-only table with atomic, checksummed snapshots."""

    def __init__(self, root: str | Path, name: str, fmt: str = "iceberg",
                 clock: Callable[[], float] = time.time) -> None:
        if fmt not in TABLE_FORMATS:
            raise ValueError(f"unknown table format: {fmt}")
        if not name:
            raise ValueError("table name must be non-empty")
        self.root = Path(root)
        self.name = name
        self.fmt = fmt
        self.dir = self.root / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.dir / "_manifest.json"
        self._clock = clock
        self.table_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"billing-lake:{name}"))

    # ---------------------------------------------------------------- manifests
    def _read_snapshots(self) -> list[Snapshot]:
        if not self.manifest_path.exists():
            return []
        data = json.loads(self.manifest_path.read_text())
        return [Snapshot.from_dict(s) for s in data["snapshots"]]

    def _write_snapshots(self, snapshots: list[Snapshot]) -> None:
        payload = {"format": self.fmt, "table": self.name,
                   "snapshots": [s.as_dict() for s in snapshots]}
        tmp = self.manifest_path.with_name("_manifest.tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, self.manifest_path)

    def snapshot(self) -> Snapshot | None:
        snapshots = self._read_snapshots()
        return snapshots[-1] if snapshots else None

    def history(self) -> list[Snapshot]:
        return self._read_snapshots()

    def properties(self) -> dict[str, str]:
        """Catalog properties an Iceberg/Delta/Hudi catalog would store."""
        return {**TABLE_FORMATS[self.fmt], "table-name": self.name,
                "table-uuid": self.table_uuid}

    # ------------------------------------------------------------------- writes
    def _write_rows(self, rows: Iterable[Mapping[str, Any]]) -> tuple[LakeFile, ...]:
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            if "timestamp" not in row or "customer_id" not in row:
                raise ValueError("row missing timestamp or customer_id")
            key = (str(row["timestamp"])[:10], str(row["customer_id"]))
            groups.setdefault(key, []).append(dict(row))
        files: list[LakeFile] = []
        for (dt, customer_id), group in sorted(groups.items()):
            rel = f"dt={dt}/customer_id={customer_id}/part-{uuid.uuid4().hex}.jsonl"
            path = self.dir / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            body = "".join(canonical_json(row) + "\n" for row in group)
            path.write_text(body)
            timestamps = [str(row["timestamp"]) for row in group]
            files.append(LakeFile(
                path=rel, rows=len(group),
                sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
                dt=dt, customer_id=customer_id,
                min_ts=min(timestamps), max_ts=max(timestamps)))
        return tuple(files)

    def _commit(self, rows: list[dict[str, Any]], message: str,
                replace: bool) -> Snapshot:
        snapshots = self._read_snapshots()
        base = snapshots[-1] if snapshots else None
        kept = () if replace or base is None else base.files
        snapshot = Snapshot(
            snapshot_id=base.snapshot_id + 1 if base else 1,
            created_at=self._clock(), message=message,
            files=tuple(kept) + self._write_rows(rows))
        self._write_snapshots(snapshots + [snapshot])
        return snapshot

    def append(self, rows: Iterable[Mapping[str, Any]]) -> Snapshot:
        materialized = list(rows)
        if not materialized:
            raise ValueError("append requires at least one row")
        return self._commit(materialized, "append", replace=False)

    def compact(self) -> Snapshot:
        """Merge all live files into fresh files; old snapshots stay readable."""
        if not self._read_snapshots():
            raise ValueError("cannot compact an empty table")
        return self._commit(self.scan(), "compact", replace=True)

    # -------------------------------------------------------------------- reads
    def scan(self, dt: str | None = None, customer_id: str | None = None,
             as_of: int | None = None) -> list[dict[str, Any]]:
        snapshots = self._read_snapshots()
        if not snapshots:
            return []
        if as_of is None:
            snapshot = snapshots[-1]
        else:
            found = next((s for s in snapshots if s.snapshot_id == as_of), None)
            if found is None:
                raise KeyError(f"unknown snapshot {as_of}")
            snapshot = found
        rows: list[dict[str, Any]] = []
        for lake_file in snapshot.files:
            if dt is not None and lake_file.dt != dt:
                continue
            if customer_id is not None and lake_file.customer_id != customer_id:
                continue
            body = (self.dir / lake_file.path).read_text()
            rows.extend(json.loads(line) for line in body.splitlines() if line)
        return rows

    def verify(self) -> list[str]:
        """Return one message per corrupted, missing or truncated file."""
        snapshot = self.snapshot()
        if snapshot is None:
            return []
        errors: list[str] = []
        for lake_file in snapshot.files:
            path = self.dir / lake_file.path
            if not path.exists():
                errors.append(f"missing:{lake_file.path}")
                continue
            body = path.read_bytes()
            if hashlib.sha256(body).hexdigest() != lake_file.sha256:
                errors.append(f"checksum:{lake_file.path}")
            elif body.count(b"\n") != lake_file.rows:
                errors.append(f"rows:{lake_file.path}")
        return errors


def open_table(root: str | Path, name: str, fmt: str = "iceberg",
               clock: Callable[[], float] = time.time) -> LakeTable:
    return LakeTable(root, name, fmt=fmt, clock=clock)
