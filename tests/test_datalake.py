"""Data-lake table: snapshots, partition pruning, time travel, compaction, checksums."""
from __future__ import annotations

import json

import pytest
from conftest import make_event

from billing_engine.datalake import LakeTable, open_table

pytestmark = pytest.mark.req("NFR-DATALAKE", "FR5")


def _row(event_id="e1", customer_id="acme", timestamp="2026-10-09T10:00:00Z",
         quantity=1.0):
    event = make_event(event_id, customer_id=customer_id, timestamp=timestamp,
                       quantity=quantity)
    event.pop("product_id")
    return event


def test_table_creation_validates_format_and_name(tmp_path):
    with pytest.raises(ValueError, match="format"):
        LakeTable(tmp_path, "events", fmt="csv")
    with pytest.raises(ValueError, match="name"):
        LakeTable(tmp_path, "")
    table = open_table(tmp_path, "events", fmt="delta")
    assert isinstance(table, LakeTable)
    assert table.snapshot() is None
    assert table.history() == []
    assert table.scan() == []
    assert table.verify() == []


@pytest.mark.parametrize("fmt", ["iceberg", "delta", "hudi"])
def test_catalog_properties_match_the_format(tmp_path, fmt):
    table = LakeTable(tmp_path, f"events-{fmt}", fmt=fmt)
    props = table.properties()
    assert props["table-name"] == f"events-{fmt}"
    assert props["table-uuid"] == table.table_uuid
    if fmt == "iceberg":
        assert props["format-version"] == "2"
    elif fmt == "delta":
        assert props["delta.appendOnly"] == "true"
    else:
        assert props["hoodie.table.type"] == "COPY_ON_WRITE"


def test_append_partitions_by_day_and_customer_with_checksums(tmp_path):
    table = LakeTable(tmp_path, "events", clock=lambda: 123.0)
    with pytest.raises(ValueError, match="at least one row"):
        table.append([])
    with pytest.raises(ValueError, match="timestamp or customer_id"):
        table.append([{"event_id": "bad"}])

    snapshot = table.append([
        _row("e1", "acme", "2026-10-09T10:00:00Z"),
        _row("e2", "acme", "2026-10-09T11:00:00Z"),
        _row("e3", "other", "2026-10-10T09:00:00Z"),
    ])
    assert snapshot.snapshot_id == 1
    assert snapshot.created_at == 123.0
    assert snapshot.rows == 3 and len(snapshot.files) == 2
    assert table.snapshot() == snapshot
    assert table.history() == [snapshot]
    assert table.verify() == []

    layout = {f.path for f in snapshot.files}
    assert any(p.startswith("dt=2026-10-09/customer_id=acme/") for p in layout)
    assert any(p.startswith("dt=2026-10-10/customer_id=other/") for p in layout)
    for lake_file in snapshot.files:
        assert len(lake_file.sha256) == 64
        assert lake_file.min_ts <= lake_file.max_ts


def test_scan_prunes_partitions_and_supports_time_travel(tmp_path):
    table = LakeTable(tmp_path, "events")
    table.append([
        _row("e1", "acme", "2026-10-09T10:00:00Z"),
        _row("e2", "other", "2026-10-10T10:00:00Z"),
    ])
    table.append([_row("e3", "acme", "2026-10-11T10:00:00Z")])

    assert [r["event_id"] for r in table.scan()] == ["e1", "e2", "e3"]
    assert [r["event_id"] for r in table.scan(dt="2026-10-09")] == ["e1"]
    assert [r["event_id"] for r in table.scan(customer_id="acme")] == ["e1", "e3"]
    assert [r["event_id"] for r in table.scan(dt="2026-10-09",
                                              customer_id="acme")] == ["e1"]
    assert [r["event_id"] for r in table.scan(as_of=1)] == ["e1", "e2"]
    assert [r["event_id"] for r in table.scan(as_of=2)] == ["e1", "e2", "e3"]
    with pytest.raises(KeyError, match="unknown snapshot"):
        table.scan(as_of=99)


def test_compaction_rewrites_live_files_but_keeps_history_readable(tmp_path):
    table = LakeTable(tmp_path, "events")
    table.append([_row("e1", "acme", "2026-10-09T10:00:00Z")])
    table.append([_row("e2", "acme", "2026-10-09T11:00:00Z")])
    before = table.snapshot()
    assert before is not None and len(before.files) == 2

    compacted = table.compact()
    assert compacted.snapshot_id == 3
    assert compacted.message == "compact"
    assert len(compacted.files) == 1  # merged into one file per partition
    assert [r["event_id"] for r in table.scan()] == ["e1", "e2"]
    assert [r["event_id"] for r in table.scan(as_of=before.snapshot_id)] == ["e1", "e2"]
    assert table.verify() == []


def test_compaction_rejects_empty_table(tmp_path):
    with pytest.raises(ValueError, match="empty table"):
        LakeTable(tmp_path, "events").compact()


def test_verify_detects_missing_corrupt_and_truncated_files(tmp_path):
    table = LakeTable(tmp_path, "events")
    snapshot = table.append([_row("e1", "acme", "2026-10-09T10:00:00Z")])
    lake_file = snapshot.files[0]
    path = table.dir / lake_file.path
    original = path.read_text()

    path.write_text(original + '{"event_id":"extra"}\n')
    assert table.verify() == [f"checksum:{lake_file.path}"]

    # correct checksum, wrong row count -> truncation/partial-write signal
    manifest = json.loads(table.manifest_path.read_text())
    manifest["snapshots"][-1]["files"][0]["rows"] = lake_file.rows + 5
    table.manifest_path.write_text(json.dumps(manifest))
    path.write_text(original)
    assert table.verify() == [f"rows:{lake_file.path}"]

    path.unlink()
    assert table.verify() == [f"missing:{lake_file.path}"]
