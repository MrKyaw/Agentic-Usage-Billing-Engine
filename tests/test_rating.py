import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from billing_engine import rating  # noqa: E402


@pytest.fixture()
def book():
    return rating.get_book(str(ROOT / "src" / "billing_engine" / "price_plans.json"))


def test_flat_rating(book):
    r = book.rate_period("cloud_credits", 100)
    assert r["amount"] == 10.0
    assert r["billable_quantity"] == 100


def test_tiered_with_allowance(book):
    # 100 included; next 1000 @ .05
    r = book.rate_period("render_minutes", 1100)
    assert r["billable_quantity"] == 1000
    assert r["amount"] == 50.0


def test_tiered_second_tier(book):
    r = book.rate_period("render_minutes", 2100)  # billable 2000: 1000@.05 + 1000@.04
    assert r["amount"] == pytest.approx(90.0)


def test_allowance_covers_all(book):
    r = book.rate_period("render_minutes", 50)
    assert r["amount"] == 0.0


def test_inference_allowance(book):
    r = book.rate_period("inference_tokens", 500_000)
    assert r["amount"] == 0.0
    r2 = book.rate_period("inference_tokens", 2_000_000)
    assert r2["billable_quantity"] == 1_000_000


def test_unknown_metric(book):
    with pytest.raises(rating.RatingError):
        book.rate_period("nope", 10)


def test_unit_rating_versioned(book):
    amt, ver = book.rate_unit("cloud_credits", 5)
    assert ver == book.plan_version and amt == pytest.approx(0.5)
