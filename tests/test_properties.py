"""Property-based tests: billing invariants must hold for ANY event stream,
delivery order, duplicate pattern, crash point, timezone, and price plan."""
import math
import shutil
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.stateful import (RuleBasedStateMachine, initialize, invariant, precondition,
                                 rule)

from billing_engine.ingest import ingest_one
from billing_engine.period_close import close_period
from billing_engine.processor import drain, process_batch
from billing_engine.query import get_bill, get_usage
from billing_engine.rating import PriceBook, get_book
from billing_engine.reconcile import reconcile
from billing_engine.store import Store

pytestmark = pytest.mark.property

CUSTOMERS = ["acme", "globex", "initech"]
METRICS = ["cloud_credits", "render_minutes", "inference_tokens"]
UTC = timezone.utc


@contextmanager
def fresh_store():
    d = tempfile.mkdtemp(prefix="aube-prop-")
    try:
        yield Store(f"{d}/b.db", f"{d}/raw")
    finally:
        shutil.rmtree(d, ignore_errors=True)


timestamps_2026 = st.datetimes(min_value=datetime(2026, 1, 1), max_value=datetime(2026, 12, 31),
                               timezones=st.just(UTC))


@st.composite
def unique_events(draw, min_size=1, max_size=40, quantities=st.integers(1, 10_000)):
    n = draw(st.integers(min_size, max_size))
    return [{"event_id": f"e-{i:04d}", "customer_id": draw(st.sampled_from(CUSTOMERS)),
             "product_id": "design-suite", "metric": draw(st.sampled_from(METRICS)),
             "quantity": draw(quantities), "timestamp": draw(timestamps_2026).isoformat()}
            for i in range(n)]


def expected_totals(events):
    out = defaultdict(lambda: [0.0, 0])
    for e in events:
        per = datetime.fromisoformat(e["timestamp"]).astimezone(UTC).strftime("%Y-%m")
        k = (e["customer_id"], per, e["metric"])
        out[k][0] += e["quantity"]
        out[k][1] += 1
    return out


def stored_totals(store, keys):
    out = {}
    for cust, per, metric in keys:
        rows = [r for r in store.aggregates_for(cust, per) if r["metric"] == metric]
        out[(cust, per, metric)] = ((rows[0]["total_quantity"], rows[0]["event_count"])
                                    if rows else (0.0, 0))
    return out


# ------------------------------------------------------------ pipeline streams

@pytest.mark.req("FR2", "FR4", "FR5", "NFR-CORRECTNESS")
@given(events=unique_events(), data=st.data())
def test_any_order_any_duplicates_bills_each_event_exactly_once(events, data):
    """Shuffled delivery + random redeliveries -> every event counted exactly once."""
    dupes = data.draw(st.lists(st.sampled_from(events), max_size=len(events) * 2))
    delivery = data.draw(st.permutations(events + dupes))
    with fresh_store() as s:
        for e in delivery:
            ingest_one(s, e)
        r = drain(s, batch=data.draw(st.integers(1, 50)))
        assert r["accepted"] == len(events) and r["duplicate"] == len(dupes)
        exp = expected_totals(events)
        got = stored_totals(s, exp)
        assert {k: (v[0], v[1]) for k, v in exp.items()} == got
        rec = reconcile(s)
        assert rec["match"], rec["mismatches"]
        assert rec["checked"] == len(exp)
        assert drain(s) == {"accepted": 0, "duplicate": 0, "dlq": 0}


@pytest.mark.req("FR2", "NFR-CORRECTNESS")
@given(events=unique_events(), data=st.data())
def test_crash_at_any_point_loses_and_double_counts_nothing(events, data):
    """Worker dies after k batches, and/or after writing but before ack -> exact totals."""
    with fresh_store() as s:
        for e in events:
            ingest_one(s, e)
        batch = data.draw(st.integers(1, 10))
        for _ in range(data.draw(st.integers(0, 5))):
            process_batch(s, limit=batch)
        # crash between the idempotent write and the ack (worst case for at-least-once)
        for item in s.claim_batch(limit=data.draw(st.integers(0, 5))):
            amt, ver = get_book().rate_unit(item["payload"]["metric"], item["payload"]["quantity"])
            s.try_insert_event(item["payload"], ver, amt)
        # visibility timeout expires; a new worker resumes
        s._conn().execute("UPDATE inbox SET claimed_at=NULL")
        drain(s)
        assert s.pending_count() == 0
        exp = expected_totals(events)
        assert stored_totals(s, exp) == {k: (v[0], v[1]) for k, v in exp.items()}
        assert reconcile(s)["match"]


mixed_magnitude_floats = st.one_of(
    st.floats(1e-6, 1e-3), st.floats(1e-3, 1e3), st.floats(1e8, 1e9))


@st.composite
def one_key_float_events(draw):
    """Many events in ONE customer x period x metric so summation order matters."""
    n = draw(st.integers(3, 30))
    return [{"event_id": f"f-{i:03d}", "customer_id": "acme", "product_id": "p",
             "metric": "cloud_credits", "quantity": draw(mixed_magnitude_floats),
             "timestamp": draw(st.datetimes(datetime(2026, 10, 1), datetime(2026, 10, 31),
                                            timezones=st.just(UTC))).isoformat()}
            for i in range(n)]


@pytest.mark.req("FR5")
@given(events=one_key_float_events(), data=st.data())
def test_reconcile_matches_for_arbitrary_float_quantities(events, data):
    """Summation order differs between processing and raw-log replay; no false alarms."""
    with fresh_store() as s:
        for e in data.draw(st.permutations(events)):
            ingest_one(s, e)
        drain(s)
        rec = reconcile(s)
        assert rec["match"], rec["mismatches"]
        for k, (qty, n) in expected_totals(events).items():
            stored = stored_totals(s, [k])[k]
            assert stored[1] == n and math.isclose(stored[0], qty, rel_tol=1e-9)


@pytest.mark.req("FR2")
@given(dt=st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2099, 12, 31),
                       timezones=st.builds(lambda m: timezone(timedelta(minutes=m)),
                                           st.integers(-12 * 60, 14 * 60))))
def test_any_timezone_buckets_by_utc_instant(dt):
    """Period and raw-log partition always follow the UTC instant, never local time."""
    utc = dt.astimezone(UTC)
    with fresh_store() as s:
        ingest_one(s, {"event_id": "tz", "customer_id": "acme", "product_id": "p",
                       "metric": "cloud_credits", "quantity": 1, "timestamp": dt.isoformat()})
        drain(s)
        assert s.get_event("tz")["period"] == utc.strftime("%Y-%m")
        assert list(s.raw_log_dir.glob(f"dt={utc:%Y-%m-%d}/*.jsonl"))
        assert get_usage(s, "acme", utc.strftime("%Y-%m"))["lines"][0]["event_count"] == 1


# ------------------------------------------------------------------- rating

@st.composite
def tiered_plans(draw):
    caps = sorted(draw(st.sets(st.integers(1, 100_000), max_size=4)))
    prices = draw(st.lists(st.floats(0, 10, allow_nan=False), min_size=len(caps) + 1,
                           max_size=len(caps) + 1))
    tiers = [{"up_to": c, "unit_price": p} for c, p in zip(caps, prices)]
    tiers.append({"up_to": None, "unit_price": prices[-1]})
    allowance = draw(st.integers(0, 5_000))
    return {"type": "tiered", "tiers": tiers, "included_allowance": allowance}


def reference_tiered(cfg, qty):
    billable = max(0.0, qty - cfg["included_allowance"])
    amount, lower = 0.0, 0.0
    for t in cfg["tiers"]:
        upper = math.inf if t["up_to"] is None else float(t["up_to"])
        amount += t["unit_price"] * max(0.0, min(billable, upper) - lower)
        lower = upper
    return amount


qty_strategy = st.floats(0, 1_000_000, allow_nan=False)


@pytest.mark.req("FR3")
@given(cfg=tiered_plans(), q=qty_strategy)
def test_tiered_rating_matches_reference_model(cfg, q):
    book = PriceBook({"plan_version": "p", "plans": {"m": cfg}})
    r = book.rate_period("m", q)
    assert r["billable_quantity"] == max(0.0, q - cfg["included_allowance"])
    assert abs(r["amount"] - reference_tiered(cfg, q)) <= 0.01 + 1e-9 * reference_tiered(cfg, q)


@pytest.mark.req("FR3")
@given(cfg=tiered_plans(), q1=qty_strategy, q2=qty_strategy)
def test_rating_is_monotonic_and_non_negative(cfg, q1, q2):
    q1, q2 = sorted((q1, q2))
    book = PriceBook({"plan_version": "p", "plans": {"m": cfg}})
    a1, a2 = book.rate_period("m", q1)["amount"], book.rate_period("m", q2)["amount"]
    assert 0 <= a1 <= a2


@pytest.mark.req("FR3")
@given(price=st.floats(0, 100, allow_nan=False), allowance=st.integers(0, 10_000), q=qty_strategy)
def test_flat_rating_formula(price, allowance, q):
    book = PriceBook({"plan_version": "p", "plans": {"m": {
        "type": "flat", "unit_price": price, "included_allowance": allowance}}})
    r = book.rate_period("m", q)
    assert r["amount"] == round(max(0.0, q - allowance) * price, 2)
    assert r["plan_version"] == "p"


@pytest.mark.req("FR3")
@given(metric=st.sampled_from(METRICS), q=qty_strategy)
def test_rating_is_deterministic(metric, q):
    book = get_book()
    assert book.rate_period(metric, q) == book.rate_period(metric, q)
    assert book.rate_unit(metric, q) == book.rate_unit(metric, q)


# --------------------------------------------------------- stateful lifecycle

PERIODS = {"2026-09": "2026-09-15T12:00:00Z", "2026-10": "2026-10-15T12:00:00Z",
           "2026-11": "2026-11-15T12:00:00Z"}


@pytest.mark.req("FR2", "FR4", "FR5", "NFR-CORRECTNESS")
class BillingLifecycle(RuleBasedStateMachine):
    """Random interleavings of ingest, redelivery, poison, processing, crashes,
    period close and late events. Invariants are checked after every step."""

    @initialize()
    def setup(self):
        self.dir = tempfile.mkdtemp(prefix="aube-sm-")
        self.store = Store(f"{self.dir}/b.db", f"{self.dir}/raw")
        self.events: dict[str, dict] = {}
        self.poison: set[str] = set()
        self.frozen: dict[tuple[str, str], float] = {}
        self.n = 0

    def teardown(self):
        if hasattr(self, "dir"):
            shutil.rmtree(self.dir, ignore_errors=True)

    @rule(cust=st.sampled_from(CUSTOMERS), metric=st.sampled_from(METRICS),
          per=st.sampled_from(sorted(PERIODS)), qty=st.integers(1, 5_000))
    def ingest_new(self, cust, metric, per, qty):
        self.n += 1
        e = {"event_id": f"sm-{self.n}", "customer_id": cust, "product_id": "p",
             "metric": metric, "quantity": qty, "timestamp": PERIODS[per]}
        ingest_one(self.store, e)
        self.events[e["event_id"]] = e

    @precondition(lambda self: self.events)
    @rule(data=st.data())
    def redeliver(self, data):
        ingest_one(self.store, self.events[data.draw(st.sampled_from(sorted(self.events)))])

    @rule()
    def ingest_poison(self):
        self.n += 1
        eid = f"poison-{self.n}"
        ingest_one(self.store, {"event_id": eid, "customer_id": "acme", "product_id": "p",
                                "metric": "unknown", "quantity": 1,
                                "timestamp": PERIODS["2026-10"]})
        self.poison.add(eid)

    @rule(limit=st.integers(1, 20))
    def process(self, limit):
        process_batch(self.store, limit=limit)

    @rule(limit=st.integers(1, 20))
    def crash_after_claim(self, limit):
        self.store.claim_batch(limit=limit)
        self.store._conn().execute("UPDATE inbox SET claimed_at=NULL")

    @rule()
    def drain_all(self):
        drain(self.store)

    @rule(cust=st.sampled_from(CUSTOMERS), per=st.sampled_from(sorted(PERIODS)))
    def close(self, cust, per):
        out = close_period(self.store, cust, per, export_dir=f"{self.dir}/exports")
        self.frozen[(cust, per)] = out["bill"]["total_amount"]

    def _expected(self):
        return expected_totals(self.events.values())

    @invariant()
    def never_double_counts(self):
        if not hasattr(self, "store"):
            return
        exp = self._expected()
        for k, (qty, n) in stored_totals(self.store, exp).items():
            assert qty <= exp[k][0] and n <= exp[k][1]

    @invariant()
    def drained_state_is_exact_and_reconciles(self):
        if not hasattr(self, "store") or self.store.pending_count():
            return
        exp = self._expected()
        assert stored_totals(self.store, exp) == {k: (v[0], v[1]) for k, v in exp.items()}
        rec = reconcile(self.store)
        assert rec["match"], rec["mismatches"]
        assert rec["dlq_excluded"] == len(self.poison)

    @invariant()
    def closed_bills_are_frozen(self):
        if not hasattr(self, "store"):
            return
        for (cust, per), total in self.frozen.items():
            b = get_bill(self.store, cust, per)
            live = get_bill(self.store, cust, per, live=True)["total_amount"]
            assert b["status"] == "closed" and b["total_amount"] == total
            assert b["post_close_delta"] == round(live - total, 2)


BillingLifecycle.TestCase.settings = settings(max_examples=40, stateful_step_count=30,
                                              deadline=None)
TestBillingLifecycle = BillingLifecycle.TestCase
TestBillingLifecycle.pytestmark = [pytest.mark.property,
                                   pytest.mark.req("FR2", "FR4", "FR5", "NFR-CORRECTNESS")]
