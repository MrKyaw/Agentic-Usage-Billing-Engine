from datetime import datetime, timedelta, timezone

import pytest
from conftest import make_event
from pydantic import ValidationError

from billing_engine import rating
from billing_engine.models import BatchIngestRequest, UsageEvent, coerce_event

FR1 = pytest.mark.req("FR1")
FR3 = pytest.mark.req("FR3")


@FR1
def test_naive_timestamp_assumed_utc():
    ev = UsageEvent.model_validate(make_event("n-1", timestamp="2026-10-09T10:00:00"))
    assert ev.timestamp.tzinfo == timezone.utc and ev.billing_period() == "2026-10"


@FR1
def test_datetime_instance_converted_to_utc():
    est = timezone(timedelta(hours=-5))
    ev = UsageEvent.model_validate(
        make_event("d-1", timestamp=datetime(2026, 10, 31, 23, 0, tzinfo=est)))
    assert ev.timestamp == datetime(2026, 11, 1, 4, 0, tzinfo=timezone.utc)
    assert ev.billing_period() == "2026-11"


@FR1
def test_event_id_is_stripped_and_blank_rejected():
    assert coerce_event(make_event("  e-1  ")).event_id == "e-1"
    with pytest.raises(ValidationError):
        coerce_event(make_event("   "))


@FR1
@pytest.mark.parametrize("field,value", [("quantity", 0), ("quantity", -1),
                                         ("quantity", 2_000_000_000),
                                         ("timestamp", "not-a-date"), ("metric", "")])
def test_invalid_fields_rejected(field, value):
    with pytest.raises(ValidationError):
        coerce_event({**make_event("bad"), field: value})


@pytest.mark.req("FR6", "NFR-SECURITY")
def test_log_safe_dict_redacts_metadata_values():
    ev = coerce_event({**make_event("m-1"), "metadata": {"email": "a@b.c", "seat": 3}})
    d = ev.log_safe_dict()
    assert d["metadata_keys"] == ["email", "seat"]
    assert "a@b.c" not in str(d)


@FR1
def test_batch_request_limits():
    with pytest.raises(ValidationError):
        BatchIngestRequest(events=[])
    assert len(BatchIngestRequest(events=[make_event("b-1")]).events) == 1


@FR3
def test_unit_price_for_flat_and_tiered():
    book = rating.get_book()
    assert book.unit_price_for("cloud_credits") == pytest.approx(0.1)
    assert book.unit_price_for("render_minutes") == pytest.approx(0.05)


@FR3
def test_tiered_rate_unit_uses_first_tier_price():
    amt, _ = rating.get_book().rate_unit("render_minutes", 10)
    assert amt == pytest.approx(0.5)


@FR3
@pytest.mark.parametrize("tiers", [[], [{"up_to": 10, "unit_price": 1.0},
                                         {"up_to": 20, "unit_price": 0.5}]])
def test_tiered_plan_without_open_ended_tier_rejected(tiers):
    with pytest.raises(rating.RatingError):
        rating.PriceBook({"plan_version": "t",
                          "plans": {"m": {"type": "tiered", "tiers": tiers}}})


@FR3
def test_tiered_plan_crossing_every_tier():
    book = rating.PriceBook({"plan_version": "t", "plans": {"m": {
        "type": "tiered", "tiers": [{"up_to": 10, "unit_price": 1.0},
                                    {"up_to": 20, "unit_price": 0.5},
                                    {"up_to": None, "unit_price": 0.1}]}}})
    assert book.rate_period("m", 30)["amount"] == 16.0  # 10@1 + 10@.5 + 10@.1


@FR3
def test_book_cache_reset():
    b1 = rating.get_book()
    assert rating.get_book() is b1
    rating.reset_book_cache()
    assert rating.get_book() is not b1
