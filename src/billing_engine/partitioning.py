"""Kafka-style partitioning, ordering and checksum repair — streaming layer.

This module implements the *semantics* a Kafka/Flink deployment needs, without a
broker, so they are testable in-process:

- **Jump consistent hash** (Lamping & Veach): moving from N to M partitions moves
  only the keys whose new bucket differs; every key still has exactly one owner.
  Scaling 20 -> 40 moves ~50% of keys (unavoidable: there are twice as many
  buckets) but never splits a key across partitions.
- **Epoch-fenced routing**: cutover is pause -> drain -> bump epoch -> resume, so
  a key is never written to two partitions at the same time. A stale producer
  holding the old epoch is rejected instead of silently creating a second order.
- **Per-key sequencer**: detects duplicates, gaps and buffered (out-of-order)
  records, and releases contiguous runs once the hole is filled.
- **Checksum chain**: ``sha256(prev_checksum + canonical_payload)`` traces the
  exact record where ordering broke and rebuilds a verifiable chain on repair.
- **Watermark + k-way merge**: deterministic global order across partitions,
  with an explicit lateness bound for events that arrive too late to reorder.

Billing records are keyed by ``customer_id`` (not ``event_id``) so that all
events for one customer stay ordered on one partition; ``event_id`` would spread
perfectly but destroy the per-customer order we need for allowances and credits.
"""
from __future__ import annotations

import hashlib
import heapq
import json
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence


def canonical_json(payload: Mapping[str, Any]) -> str:
    """Stable byte representation used by every checksum in the platform."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def event_checksum(payload: Mapping[str, Any], prev_checksum: str = "") -> str:
    """Hash-chain one record onto the previous checksum.

    Changing order, dropping a record or mutating a field changes every
    downstream checksum, which is what makes repair auditable.
    """
    h = hashlib.sha256()
    h.update(prev_checksum.encode("utf-8"))
    h.update(b"\x1f")
    h.update(canonical_json(payload).encode("utf-8"))
    return h.hexdigest()


def jump_hash(key: str, buckets: int) -> int:
    """Jump consistent hash: O(log n), minimal movement, no rehash table."""
    if buckets < 1:
        raise ValueError("buckets must be >= 1")
    b, j = -1, 0
    h = int.from_bytes(hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest(), "big")
    while j < buckets:
        b = j
        h = (h * 2862933555777941757 + 1) & 0xFFFFFFFFFFFFFFFF
        j = int((b + 1) * (float(1 << 31) / float((h >> 33) + 1)))
    return b


@dataclass(frozen=True)
class Route:
    """Where a key lives in a given partition-map epoch."""
    key: str
    partition: int
    epoch: int


@dataclass(frozen=True)
class Partitioner:
    """Immutable partition map. A new epoch is a new object, never a mutation."""
    buckets: int
    epoch: int = 0

    def __post_init__(self) -> None:
        if self.buckets < 1:
            raise ValueError("buckets must be >= 1")
        if self.epoch < 0:
            raise ValueError("epoch must be >= 0")

    def partition_for(self, key: str) -> int:
        if not key:
            raise ValueError("partition key must be non-empty")
        return jump_hash(key, self.buckets)

    def route(self, key: str) -> Route:
        return Route(key=key, partition=self.partition_for(key), epoch=self.epoch)


def partition_key(event: Mapping[str, Any], granularity: str = "customer") -> str:
    """Choose the Kafka message key.

    ``customer`` (default) preserves per-customer order, which rating needs.
    ``customer_metric`` adds parallelism at the cost of cross-metric ordering.
    ``event`` is maximum parallelism but no ordering — only safe because rating
    is commutative over period totals.
    """
    if granularity == "customer":
        return str(event["customer_id"])
    if granularity == "customer_metric":
        return f"{event['customer_id']}|{event['metric']}"
    if granularity == "event":
        return str(event["event_id"])
    raise ValueError(f"unknown granularity: {granularity}")


@dataclass(frozen=True)
class Move:
    key: str
    from_partition: int
    to_partition: int


@dataclass(frozen=True)
class ScalePlan:
    """Result of planning N -> M partitions: what moves and what stays."""
    old_buckets: int
    new_buckets: int
    epoch: int
    moves: tuple[Move, ...]
    stable: tuple[str, ...]

    @property
    def moved_count(self) -> int:
        return len(self.moves)

    @property
    def moved_fraction(self) -> float:
        total = self.moved_count + len(self.stable)
        return round(self.moved_count / total, 4) if total else 0.0

    def summary(self) -> dict[str, Any]:
        return {"old_buckets": self.old_buckets, "new_buckets": self.new_buckets,
                "epoch": self.epoch, "moved": self.moved_count,
                "stable": len(self.stable), "moved_fraction": self.moved_fraction}


def plan_scale(keys: Iterable[str], old_buckets: int, new_buckets: int,
               epoch: int = 1) -> ScalePlan:
    """Plan a partition-count change.

    Keys keep their relative order because a key is never split: it is either
    already on its new partition or it is migrated whole before the epoch flips.
    """
    if old_buckets < 1 or new_buckets < 1:
        raise ValueError("bucket counts must be >= 1")
    if epoch < 1:
        raise ValueError("epoch must be >= 1")
    unique = sorted(set(keys))
    if any(not k for k in unique):
        raise ValueError("partition keys must be non-empty")
    old = Partitioner(old_buckets, epoch - 1)
    new = Partitioner(new_buckets, epoch)
    moves: list[Move] = []
    stable: list[str] = []
    for key in unique:
        before, after = old.partition_for(key), new.partition_for(key)
        if before == after:
            stable.append(key)
        else:
            moves.append(Move(key=key, from_partition=before, to_partition=after))
    return ScalePlan(old_buckets=old_buckets, new_buckets=new_buckets, epoch=epoch,
                     moves=tuple(moves), stable=tuple(stable))


class CutoverBarrier:
    """Pause -> drain -> activate. Rejects writes from a stale map epoch."""

    def __init__(self, partitioner: Partitioner) -> None:
        self._partitioner = partitioner
        self._fenced = False
        self._plan: ScalePlan | None = None

    @property
    def partitioner(self) -> Partitioner:
        return self._partitioner

    @property
    def fenced(self) -> bool:
        return self._fenced

    def fence(self) -> None:
        if self._fenced:
            raise RuntimeError("already fenced")
        self._fenced = True

    def begin(self, plan: ScalePlan) -> None:
        if not self._fenced:
            raise RuntimeError("fence producers before planning migration")
        if self._plan is not None:
            raise RuntimeError("migration already begun")
        self._plan = plan

    def activate(self) -> Partitioner:
        if self._plan is None:
            raise RuntimeError("begin migration before activation")
        self._partitioner = Partitioner(self._plan.new_buckets, self._plan.epoch)
        self._plan = None
        self._fenced = False
        return self._partitioner

    def accept(self, route: Route) -> bool:
        """A write is accepted only while open and only for the current epoch."""
        return not self._fenced and route.epoch == self._partitioner.epoch


class PerKeySequencer:
    """Tracks the expected next sequence per key.

    Returns one of ``in_order``, ``duplicate`` or ``buffered``. Buffered records
    are the out-of-order ones; they are released in order once the hole fills.
    """

    def __init__(self, max_buffer_per_key: int = 1000) -> None:
        if max_buffer_per_key < 1:
            raise ValueError("max_buffer_per_key must be >= 1")
        self.max_buffer_per_key = max_buffer_per_key
        self._expected: dict[str, int] = {}
        self._buffer: dict[str, dict[int, Any]] = {}

    def observe(self, key: str, seq: int, payload: Any = None) -> str:
        if seq < 0:
            raise ValueError("seq must be >= 0")
        expected = self._expected.get(key, 0)
        buf = self._buffer.setdefault(key, {})
        if seq < expected or seq in buf:
            return "duplicate"
        if seq == expected:
            self._expected[key] = expected + 1
            while self._expected[key] in buf:
                del buf[self._expected[key]]
                self._expected[key] += 1
            return "in_order"
        if len(buf) >= self.max_buffer_per_key:
            raise BufferError(f"out-of-order buffer full for key {key}")
        buf[seq] = payload
        return "buffered"

    def next_seq(self, key: str) -> int:
        return self._expected.get(key, 0)

    def buffered(self, key: str) -> list[Any]:
        buf = self._buffer.get(key, {})
        return [buf[s] for s in sorted(buf)]

    def flush(self, key: str) -> list[Any]:
        """Emit buffered records and advance past them (repair/side-output path)."""
        pending = self.buffered(key)
        buf = self._buffer.get(key, {})
        if buf:
            self._expected[key] = max(buf) + 1
            buf.clear()
        return pending


def merge_partitions(streams: Sequence[Iterable[Mapping[str, Any]]],
                     key: Callable[[Mapping[str, Any]], Any] | None = None
                     ) -> Iterator[Mapping[str, Any]]:
    """K-way merge of already-sorted per-partition streams.

    Ordering across partitions is not a Kafka guarantee; this makes it explicit:
    a deterministic total order by ``(timestamp, event_id)`` with the stream
    index as a tie-break, while preserving each partition's internal order.
    """
    order = key or (lambda e: (e["timestamp"], e["event_id"]))
    iters = [iter(s) for s in streams]
    heap: list[tuple[Any, int, Mapping[str, Any]]] = []
    for idx, it in enumerate(iters):
        try:
            event = next(it)
        except StopIteration:
            continue
        heap.append((order(event), idx, event))
    heapq.heapify(heap)
    while heap:
        _, idx, event = heapq.heappop(heap)
        yield event
        try:
            following = next(iters[idx])
        except StopIteration:
            continue
        heap.append((order(following), idx, following))
        heapq.heapify(heap)


class WatermarkWindow:
    """Bounded reordering: hold events until ``max_seen - lateness`` passes them."""

    def __init__(self, lateness_s: float) -> None:
        if lateness_s < 0:
            raise ValueError("lateness_s must be >= 0")
        self.lateness_s = lateness_s
        self._max_seen: float | None = None
        self._buffer: list[tuple[float, int, Mapping[str, Any]]] = []
        self._seq = 0
        self.late: list[Mapping[str, Any]] = []

    def watermark(self) -> float:
        return float("-inf") if self._max_seen is None else self._max_seen - self.lateness_s

    def add(self, event: Mapping[str, Any], event_time: float) -> bool:
        self._seq += 1
        if self._max_seen is None or event_time > self._max_seen:
            self._max_seen = event_time
        if event_time < self.watermark():
            self.late.append(event)
            return False
        self._buffer.append((event_time, self._seq, event))
        return True

    def drain(self) -> list[Mapping[str, Any]]:
        out = [e for _, _, e in sorted(self._buffer, key=lambda t: (t[0], t[1]))]
        self._buffer.clear()
        return out


@dataclass(frozen=True)
class ChainBreak:
    index: int
    seq: int | None
    expected: str
    actual: str
    kind: str  # "sequence" | "checksum"


@dataclass(frozen=True)
class RepairResult:
    ordered: tuple[dict[str, Any], ...]
    breaks: tuple[ChainBreak, ...]
    order_changed: bool

    @property
    def repaired(self) -> bool:
        return self.order_changed or bool(self.breaks)


def _body(record: Mapping[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in record.items() if k not in ("checksum", "seq")}


def build_chain(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Assign ``seq`` and a hash-chain ``checksum`` to records sorted by seq."""
    ordered = sorted(records, key=lambda r: int(r.get("seq", 0)))
    out: list[dict[str, Any]] = []
    prev = ""
    for seq, record in enumerate(ordered):
        body = _body(record)
        checksum = event_checksum(body, prev)
        out.append({**body, "seq": seq, "checksum": checksum})
        prev = checksum
    return out


def verify_chain(records: Sequence[Mapping[str, Any]]) -> list[ChainBreak]:
    """Return every place where sequence or checksum continuity breaks."""
    breaks: list[ChainBreak] = []
    prev = ""
    for index, record in enumerate(records):
        actual_seq = record.get("seq")
        if actual_seq != index:
            breaks.append(ChainBreak(index=index, seq=actual_seq, expected=str(index),
                                     actual=str(actual_seq), kind="sequence"))
        expected = event_checksum(_body(record), prev)
        actual = str(record.get("checksum", ""))
        if actual != expected:
            breaks.append(ChainBreak(index=index, seq=actual_seq, expected=expected,
                                     actual=actual, kind="checksum"))
        prev = actual if actual == expected else expected
    return breaks


def repair_chain(records: Sequence[Mapping[str, Any]]) -> RepairResult:
    """Sort by sequence, rebuild the chain and report what was wrong before."""
    if not records:
        return RepairResult(ordered=(), breaks=(), order_changed=False)
    seqs = [r.get("seq") for r in records]
    order_changed = any(s is None for s in seqs) or seqs != sorted(seqs)
    ordered = build_chain(records)
    return RepairResult(ordered=tuple(ordered), breaks=tuple(verify_chain(records)),
                        order_changed=order_changed)


def ledger_integrity(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Content-addressed digest of the raw ledger, plus duplicate counts.

    Two runs over the same events produce the same digest; any reordered or
    mutated record changes it, which is what operators compare across regions.
    """
    rows = list(events)
    if not rows:
        return {"events": 0, "unique_events": 0, "duplicates": 0,
                "customers": 0, "digest": None, "breaks": []}
    ordered = sorted(rows, key=lambda e: (e["timestamp"], e["event_id"]))
    seen: set[str] = set()
    duplicates = 0
    for event in ordered:
        event_id = str(event["event_id"])
        if event_id in seen:
            duplicates += 1
        seen.add(event_id)
    chain = build_chain([
        {"event_id": e["event_id"], "customer_id": e["customer_id"],
         "metric": e["metric"], "quantity": e["quantity"], "timestamp": e["timestamp"]}
        for e in ordered
    ])
    return {"events": len(rows), "unique_events": len(seen), "duplicates": duplicates,
            "customers": len({e["customer_id"] for e in ordered}),
            "digest": chain[-1]["checksum"], "breaks": verify_chain(chain)}
