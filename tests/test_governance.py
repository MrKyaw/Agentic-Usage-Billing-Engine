"""Agent-output verification: gates, risk tiers, signed verdicts, review capacity."""
from __future__ import annotations

import pytest

from billing_engine.governance import (
    DeploymentDecision,
    Verdict,
    classify_paths,
    deployment_allowed,
    evaluate,
    required_reviewers,
    review_capacity,
    sign,
    verify,
)

pytestmark = pytest.mark.req("NFR-GOVERNANCE", "NFR-CORRECTNESS")

SECRET = "ci-signing-key"


def _green(**overrides):
    facts = dict(tests_passed=207, tests_failed=0, coverage_pct=100.0,
                 reconcile_match=True, todo_count=0, lint_errors=0)
    facts.update(overrides)
    return evaluate("cc14c3c", "agent", ["src/billing_engine/rating.py"], **facts)


def test_classify_paths_by_highest_risk_file():
    assert classify_paths([]) == "docs"
    assert classify_paths(["README.md", "docs/AGENTIC.md"]) == "docs"
    assert classify_paths(["src/billing_engine/api.py"]) == "standard"
    assert classify_paths(["README.md", "src/billing_engine/rating.py"]) == "money_path"
    assert classify_paths(["src/billing_engine/datalake.py"]) == "money_path"
    assert classify_paths(["src/billing_engine/partitioning.py"]) == "money_path"


def test_required_reviewers_matrix():
    assert required_reviewers("money_path", "agent") == 2
    assert required_reviewers("standard", "agent") == 1
    assert required_reviewers("docs", "agent") == 0
    assert required_reviewers("money_path", "human") == 1
    assert required_reviewers("standard", "human") == 0
    assert required_reviewers("docs", "human") == 0
    with pytest.raises(ValueError, match="risk"):
        required_reviewers("critical", "agent")
    with pytest.raises(ValueError, match="author"):
        required_reviewers("docs", "robot")


def test_green_verdict_passes_every_gate_and_requires_two_humans():
    verdict = _green()
    assert verdict.passed is True
    assert verdict.failed() == ()
    assert verdict.risk == "money_path"
    assert verdict.reviewers_required == 2
    payload = verdict.payload()
    assert payload["commit"] == "cc14c3c" and payload["passed"] is True
    assert {g["name"] for g in payload["gates"]} == {
        "tests", "coverage", "reconcile", "todos", "lint"}


def test_each_failing_fact_blocks_the_verdict():
    verdict = _green(tests_passed=0, tests_failed=1, coverage_pct=99.99,
                     reconcile_match=False, todo_count=2, lint_errors=3)
    assert verdict.passed is False
    assert verdict.failed() == ("tests", "coverage", "reconcile", "todos", "lint")
    details = {g.name: g.detail for g in verdict.gates}
    assert details["tests"] == "0 passed, 1 failed"
    assert details["coverage"] == "99.99% >= 100.00%"
    assert "mismatch" in details["reconcile"]
    assert details["todos"] == "2 TODO/FIXME in src"
    assert details["lint"] == "3 lint errors"


def test_signed_verdict_cannot_be_tampered_with():
    verdict = _green()
    signature = sign(verdict, SECRET)
    assert len(signature) == 64
    assert verify(verdict, signature, SECRET) is True
    assert verify(verdict, "0" * 64, SECRET) is False
    tampered = _green(todo_count=1)
    assert verify(tampered, signature, SECRET) is False
    with pytest.raises(ValueError, match="secret"):
        sign(verdict, "")
    with pytest.raises(ValueError, match="secret"):
        verify(verdict, signature, "")


def test_deployment_requires_green_gates_and_a_valid_signature():
    verdict = _green()
    signature = sign(verdict, SECRET)
    allowed = deployment_allowed(verdict, signature, SECRET)
    assert allowed == DeploymentDecision(allowed=True, signature_valid=True, reasons=())

    bad_signature = deployment_allowed(verdict, "bad", SECRET)
    assert bad_signature.allowed is False and bad_signature.signature_valid is False
    assert bad_signature.reasons == ("verdict signature is missing or invalid",)

    red = _green(tests_failed=1, coverage_pct=90.0)
    red_signature = sign(red, SECRET)
    blocked = deployment_allowed(red, red_signature, SECRET)
    assert blocked.allowed is False and blocked.signature_valid is True
    assert blocked.reasons == ("gates failed: tests, coverage",)

    both = deployment_allowed(red, "bad", SECRET)
    assert both.allowed is False
    assert both.reasons == ("gates failed: tests, coverage",
                            "verdict signature is missing or invalid")


def test_review_capacity_math_answers_whether_humans_can_absorb_agent_prs():
    feasible = review_capacity(prs_per_day=20, minutes_per_review=15,
                               reviewers=3, hours_per_reviewer=2)
    assert feasible == {"required_hours": 5.0, "available_hours": 6.0,
                        "max_reviews_per_day": 24, "utilization": 0.8333,
                        "feasible": True}

    overloaded = review_capacity(prs_per_day=40, minutes_per_review=30,
                                 reviewers=1, hours_per_reviewer=2)
    assert overloaded["feasible"] is False
    assert overloaded["required_hours"] == 20.0
    assert overloaded["max_reviews_per_day"] == 4

    no_reviewers = review_capacity(prs_per_day=1, minutes_per_review=10,
                                   reviewers=0, hours_per_reviewer=8)
    assert no_reviewers["utilization"] is None
    assert no_reviewers["feasible"] is False

    with pytest.raises(ValueError, match="minutes_per_review"):
        review_capacity(1, 0, 1, 1)
    with pytest.raises(ValueError, match="non-negative"):
        review_capacity(-1, 10, 1, 1)


def test_verdict_is_immutable_and_failed_is_a_tuple():
    verdict: Verdict = _green()
    assert isinstance(verdict.failed(), tuple)
    with pytest.raises(AttributeError):
        verdict.risk = "docs"  # type: ignore[misc]
