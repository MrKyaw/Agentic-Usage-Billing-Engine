"""Kafka-style partitioning, ordering, checksums and the ledger digest endpoint."""
from __future__ import annotations

import pytest
from conftest import make_event
from fastapi.testclient import TestClient

from billing_engine.api import app
from billing_engine.ingest import ingest_one
from billing_engine.partitioning import (
    CutoverBarrier,
    Partitioner,
    PerKeySequencer,
    Route,
    WatermarkWindow,
    build_chain,
    canonical_json,
    event_checksum,
    jump_hash,
    ledger_integrity,
    merge_partitions,
    partition_key,
    plan_scale,
    repair_chain,
    verify_chain,
)

pytestmark = pytest.mark.req("NFR-STREAMING", "NFR-CORRECTNESS")

H = {"X-API-Key": "dev-key-123"}


# ------------------------------------------------------------------ partitioning

def test_jump_hash_is_deterministic_and_bounded():
    assert jump_hash("cust-a", 1) == 0
    for key in ("cust-a", "cust-b", "acme", "whale-42"):
        first = jump_hash(key, 20)
        assert first == jump_hash(key, 20)
        assert 0 <= first < 20


def test_jump_hash_rejects_invalid_bucket_count():
    with pytest.raises(ValueError, match="buckets"):
        jump_hash("cust", 0)


def test_scaling_20_to_40_moves_a_bounded_fraction_and_keeps_one_owner():
    keys = [f"cust-{i:04d}" for i in range(400)]
    plan = plan_scale(keys, 20, 40, epoch=1)
    assert plan.moved_count + len(plan.stable) == len(keys)
    # Doubling the partition count must move roughly half the keys: unavoidable,
    # and each moved key is migrated whole, never split across partitions.
    assert 0.35 < plan.moved_fraction < 0.65
    for move in plan.moves:
        assert 0 <= move.from_partition < 20
        assert 0 <= move.to_partition < 40
        assert move.from_partition != move.to_partition
    summary = plan.summary()
    assert summary["old_buckets"] == 20 and summary["new_buckets"] == 40
    assert summary["moved"] == plan.moved_count


def test_scale_plan_with_same_bucket_count_is_a_noop():
    plan = plan_scale(["a", "b"], 20, 20, epoch=1)
    assert plan.moves == () and plan.stable == ("a", "b")
    assert plan.moved_fraction == 0.0


def test_scale_plan_empty_keys_has_zero_fraction():
    plan = plan_scale([], 20, 40, epoch=1)
    assert plan.moved_count == 0 and plan.moved_fraction == 0.0


def test_scale_plan_rejects_invalid_inputs():
    with pytest.raises(ValueError, match="bucket counts"):
        plan_scale(["a"], 0, 40)
    with pytest.raises(ValueError, match="bucket counts"):
        plan_scale(["a"], 20, 0)
    with pytest.raises(ValueError, match="epoch"):
        plan_scale(["a"], 20, 40, epoch=0)
    with pytest.raises(ValueError, match="non-empty"):
        plan_scale([""], 20, 40)


def test_partitioner_routes_key_and_validates():
    with pytest.raises(ValueError, match="buckets"):
        Partitioner(0)
    with pytest.raises(ValueError, match="epoch"):
        Partitioner(10, epoch=-1)
    p = Partitioner(20, epoch=3)
    route = p.route("cust-a")
    assert route == Route(key="cust-a", partition=p.partition_for("cust-a"), epoch=3)
    with pytest.raises(ValueError, match="non-empty"):
        p.partition_for("")


def test_partition_key_granularities():
    event = make_event("ev-1", customer_id="acme", metric="cloud_credits")
    assert partition_key(event) == "acme"
    assert partition_key(event, "customer_metric") == "acme|cloud_credits"
    assert partition_key(event, "event") == "ev-1"
    with pytest.raises(ValueError, match="granularity"):
        partition_key(event, "product")


def test_cutover_barrier_fences_old_epoch_and_activates_new_map():
    barrier = CutoverBarrier(Partitioner(20, epoch=0))
    old_route = barrier.partitioner.route("cust-a")
    barrier.fence()
    assert barrier.fenced is True
    assert barrier.accept(old_route) is False  # writes paused during migration
    plan = plan_scale(["cust-a"], 20, 40, epoch=1)
    barrier.begin(plan)
    new_partitioner = barrier.activate()
    assert barrier.fenced is False
    assert new_partitioner == Partitioner(40, epoch=1)
    assert barrier.accept(new_partitioner.route("cust-a")) is True
    assert barrier.accept(old_route) is False  # stale producer cannot double-write


def test_cutover_barrier_rejects_invalid_state_transitions():
    barrier = CutoverBarrier(Partitioner(20))
    with pytest.raises(RuntimeError, match="fence producers"):
        barrier.begin(plan_scale(["a"], 20, 40))
    barrier.fence()
    with pytest.raises(RuntimeError, match="already fenced"):
        barrier.fence()
    barrier.begin(plan_scale(["a"], 20, 40))
    with pytest.raises(RuntimeError, match="already begun"):
        barrier.begin(plan_scale(["a"], 20, 40))
    with pytest.raises(RuntimeError, match="begin migration"):
        CutoverBarrier(Partitioner(20)).activate()


# --------------------------------------------------------------------- ordering

def test_per_key_sequencer_orders_releases_and_detects_duplicates():
    seq = PerKeySequencer()
    assert seq.observe("k", 0, "a") == "in_order"
    assert seq.observe("k", 2, "c") == "buffered"       # out-of-order
    assert seq.observe("k", 2, "c") == "duplicate"      # already buffered
    assert seq.observe("k", 1, "b") == "in_order"       # fills the hole
    assert seq.next_seq("k") == 3                        # contiguous run released
    assert seq.buffered("k") == []
    assert seq.observe("k", 1, "b") == "duplicate"      # below expected


def test_per_key_sequencer_flush_and_buffer_limits():
    with pytest.raises(ValueError, match="max_buffer"):
        PerKeySequencer(0)
    seq = PerKeySequencer(max_buffer_per_key=1)
    with pytest.raises(ValueError, match="seq"):
        seq.observe("k", -1)
    assert seq.observe("k", 5, "e") == "buffered"
    with pytest.raises(BufferError, match="full"):
        seq.observe("k", 6, "f")
    assert seq.flush("k") == ["e"]
    assert seq.next_seq("k") == 6
    assert seq.flush("k") == []  # nothing left


def test_merge_partitions_produces_deterministic_global_order():
    p0 = [make_event("a", timestamp="2026-10-09T10:00:00Z"),
          make_event("c", timestamp="2026-10-09T10:02:00Z")]
    p1 = [make_event("b", timestamp="2026-10-09T10:01:00Z"),
          make_event("d", timestamp="2026-10-09T10:03:00Z")]
    merged = [e["event_id"] for e in merge_partitions([p0, p1])]
    assert merged == ["a", "b", "c", "d"]
    # empty streams (initial StopIteration and exhausted mid-merge) are skipped
    assert [e["event_id"] for e in merge_partitions([[], p1, []])] == ["b", "d"]
    assert list(merge_partitions([])) == []


def test_merge_partitions_custom_key_and_tie_break():
    p0 = [{"id": "x", "ts": 1}, {"id": "y", "ts": 1}]
    p1 = [{"id": "z", "ts": 1}]
    out = [e["id"] for e in merge_partitions([p0, p1], key=lambda e: e["ts"])]
    assert out == ["x", "y", "z"]  # ties broken by stream index, order preserved


def test_watermark_window_holds_bounded_reordering_and_side_outputs_late_events():
    with pytest.raises(ValueError, match="lateness"):
        WatermarkWindow(-1)
    window = WatermarkWindow(lateness_s=5)
    assert window.watermark() == float("-inf")
    assert window.add({"id": "1"}, 100.0) is True
    assert window.watermark() == 95.0
    assert window.add({"id": "2"}, 99.0) is True
    assert window.add({"id": "late"}, 90.0) is False
    assert window.late == [{"id": "late"}]
    assert window.add({"id": "3"}, 110.0) is True  # advances the watermark
    assert [e["id"] for e in window.drain()] == ["2", "1", "3"]
    assert window.drain() == []


# --------------------------------------------------------------- checksum chain

def _records():
    return [
        {"seq": 0, "event_id": "e0", "quantity": 1.0},
        {"seq": 1, "event_id": "e1", "quantity": 2.0},
        {"seq": 2, "event_id": "e2", "quantity": 3.0},
    ]


def test_checksum_chain_is_stable_and_canonical():
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    chain = build_chain(_records())
    assert [r["seq"] for r in chain] == [0, 1, 2]
    assert verify_chain(chain) == []
    assert build_chain(chain) == chain  # rebuilding a clean chain is idempotent
    assert build_chain([]) == []
    assert event_checksum({"a": 1}) != event_checksum({"a": 2})


def test_checksum_chain_traces_mutation_and_sequence_breaks():
    chain = build_chain(_records())
    tampered = [dict(r) for r in chain]
    tampered[1]["quantity"] = 99.0
    breaks = verify_chain(tampered)
    assert {b.index for b in breaks} == {1, 2}
    assert breaks[0].kind == "checksum"
    assert breaks[0].actual == chain[1]["checksum"]

    reordered = [chain[1], chain[0], chain[2]]
    seq_breaks = verify_chain(reordered)
    assert any(b.kind == "sequence" and b.index == 0 for b in seq_breaks)


def test_repair_chain_sorts_rebuilds_and_reports_original_breaks():
    chain = build_chain(_records())
    shuffled = [chain[2], chain[0], chain[1]]
    result = repair_chain(shuffled)
    assert result.order_changed is True
    assert result.repaired is True
    assert [r["event_id"] for r in result.ordered] == ["e0", "e1", "e2"]
    assert verify_chain(result.ordered) == []
    assert result.breaks  # the pre-repair state was broken

    clean = repair_chain(chain)
    assert clean.order_changed is False and clean.repaired is False
    assert clean.breaks == ()

    missing_seq = repair_chain([{"event_id": "only"}])
    assert missing_seq.order_changed is True
    assert [r["seq"] for r in missing_seq.ordered] == [0]

    empty = repair_chain([])
    assert empty.ordered == () and empty.breaks == () and empty.order_changed is False


def test_ledger_integrity_digest_is_order_independent_and_counts_duplicates():
    empty = ledger_integrity([])
    assert empty == {"events": 0, "unique_events": 0, "duplicates": 0,
                     "customers": 0, "digest": None, "breaks": []}

    events = [
        make_event("e1", customer_id="a", timestamp="2026-10-09T10:00:00Z"),
        make_event("e2", customer_id="b", timestamp="2026-10-09T10:01:00Z"),
        make_event("e1", customer_id="a", timestamp="2026-10-09T10:00:00Z"),
    ]
    forward = ledger_integrity(events)
    backward = ledger_integrity(list(reversed(events)))
    assert forward["events"] == 3 and forward["unique_events"] == 2
    assert forward["duplicates"] == 1 and forward["customers"] == 2
    assert forward["digest"] == backward["digest"]
    assert forward["breaks"] == []


# ------------------------------------------------------------------ API surface

def test_ledger_integrity_endpoint(default_store):
    client = TestClient(app)
    assert client.get("/v1/ops/ledger-integrity").status_code == 401
    empty = client.get("/v1/ops/ledger-integrity", headers=H)
    assert empty.status_code == 200 and empty.json()["events"] == 0

    ingest_one(default_store, make_event("li-1"))
    ingest_one(default_store, make_event("li-2", customer_id="other"))
    body = client.get("/v1/ops/ledger-integrity", headers=H).json()
    assert body["events"] == 2 and body["unique_events"] == 2
    assert body["customers"] == 2 and body["duplicates"] == 0
    assert len(body["digest"]) == 64
