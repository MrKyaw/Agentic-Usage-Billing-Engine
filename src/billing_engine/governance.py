"""Agent-output verification and deployment gates — governance layer.

Turns the policy in ``docs/AGENTIC.md`` into executable checks, so "the agent
said it is correct" is never the evidence:

- **Gate evaluation** — tests, coverage, reconciliation, TODO/FIXME hygiene and
  lint must all be green before a change can ship.
- **Change-risk classification** — money-path files (rating, ledger, ingest,
  reconciliation, ordering, data lake) get stricter review than docs.
- **Reviewer requirements** — an agent-authored money-path change needs two
  humans plus a reconcile proof; docs changes need none.
- **Signed verdicts** — the verdict is HMAC-signed, so CI cannot be bypassed by
  editing a JSON file or re-running with different flags.
- **Review capacity math** — answers "can humans review every agent PR?" with
  numbers instead of opinion.

This is the mechanical part of oversight. The human part (design ownership,
failure analysis, security review) stays human — see docs/GOVERNANCE.md.
"""
from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from billing_engine.partitioning import canonical_json

MONEY_PATH_FILES = frozenset({
    "rating.py", "processor.py", "store.py", "reconcile.py", "ingest.py",
    "period_close.py", "query.py", "models.py", "partitioning.py", "datalake.py",
})

RISK_LEVELS = ("money_path", "standard", "docs")
AUTHORS = ("agent", "human")

_REVIEW_MATRIX = {
    ("agent", "money_path"): 2,
    ("agent", "standard"): 1,
    ("agent", "docs"): 0,
    ("human", "money_path"): 1,
    ("human", "standard"): 0,
    ("human", "docs"): 0,
}


@dataclass(frozen=True)
class Gate:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class Verdict:
    commit: str
    generated_by: str
    risk: str
    gates: tuple[Gate, ...]
    reviewers_required: int

    @property
    def passed(self) -> bool:
        return all(gate.passed for gate in self.gates)

    def failed(self) -> tuple[str, ...]:
        return tuple(gate.name for gate in self.gates if not gate.passed)

    def payload(self) -> dict[str, Any]:
        return {
            "commit": self.commit,
            "generated_by": self.generated_by,
            "risk": self.risk,
            "reviewers_required": self.reviewers_required,
            "passed": self.passed,
            "gates": [{"name": g.name, "passed": g.passed, "detail": g.detail}
                      for g in self.gates],
        }


@dataclass(frozen=True)
class DeploymentDecision:
    allowed: bool
    signature_valid: bool
    reasons: tuple[str, ...]


def classify_paths(paths: Iterable[str]) -> str:
    """Classify a change set by the highest-risk file it touches."""
    files = list(paths)
    if not files:
        return "docs"
    if all(p.endswith((".md", ".txt", ".rst")) for p in files):
        return "docs"
    if any(Path(p).name in MONEY_PATH_FILES for p in files):
        return "money_path"
    return "standard"


def required_reviewers(risk: str, generated_by: str) -> int:
    if risk not in RISK_LEVELS:
        raise ValueError(f"unknown risk level: {risk}")
    if generated_by not in AUTHORS:
        raise ValueError(f"unknown author: {generated_by}")
    return _REVIEW_MATRIX[(generated_by, risk)]


def evaluate(commit: str, generated_by: str, paths: Iterable[str], *,
             tests_passed: int, tests_failed: int, coverage_pct: float,
             reconcile_match: bool, todo_count: int, lint_errors: int,
             coverage_threshold: float = 100.0) -> Verdict:
    """Build a verdict from raw CI facts. No defaults that could hide a failure."""
    risk = classify_paths(paths)
    gates = (
        Gate("tests", tests_failed == 0 and tests_passed > 0,
             f"{tests_passed} passed, {tests_failed} failed"),
        Gate("coverage", coverage_pct >= coverage_threshold,
             f"{coverage_pct:.2f}% >= {coverage_threshold:.2f}%"),
        Gate("reconcile", bool(reconcile_match),
             "raw-log replay matches aggregates" if reconcile_match else "reconcile mismatch"),
        Gate("todos", todo_count == 0, f"{todo_count} TODO/FIXME in src"),
        Gate("lint", lint_errors == 0, f"{lint_errors} lint errors"),
    )
    return Verdict(commit=commit, generated_by=generated_by, risk=risk, gates=gates,
                   reviewers_required=required_reviewers(risk, generated_by))


def sign(verdict: Verdict, secret: str) -> str:
    if not secret:
        raise ValueError("signing secret must be non-empty")
    return hmac.new(secret.encode("utf-8"),
                    canonical_json(verdict.payload()).encode("utf-8"),
                    hashlib.sha256).hexdigest()


def verify(verdict: Verdict, signature: str, secret: str) -> bool:
    if not secret:
        raise ValueError("signing secret must be non-empty")
    return hmac.compare_digest(sign(verdict, secret), signature)


def deployment_allowed(verdict: Verdict, signature: str, secret: str) -> DeploymentDecision:
    """Deploy only when every gate passed AND the verdict is authentically signed."""
    signature_valid = verify(verdict, signature, secret)
    reasons: list[str] = []
    if not verdict.passed:
        reasons.append("gates failed: " + ", ".join(verdict.failed()))
    if not signature_valid:
        reasons.append("verdict signature is missing or invalid")
    return DeploymentDecision(allowed=verdict.passed and signature_valid,
                              signature_valid=signature_valid, reasons=tuple(reasons))


def review_capacity(prs_per_day: float, minutes_per_review: float,
                    reviewers: int, hours_per_reviewer: float) -> dict[str, Any]:
    """Can the human reviewers absorb this agent PR volume?

    Returns required vs available hours, the daily review ceiling and whether
    the current staffing is feasible. This is the number to show when someone
    asks whether "a human checks every agent change" is realistic.
    """
    if minutes_per_review <= 0:
        raise ValueError("minutes_per_review must be > 0")
    if prs_per_day < 0 or reviewers < 0 or hours_per_reviewer < 0:
        raise ValueError("inputs must be non-negative")
    required_hours = prs_per_day * minutes_per_review / 60.0
    available_hours = reviewers * hours_per_reviewer
    max_reviews = int(available_hours * 60 // minutes_per_review)
    utilization = round(required_hours / available_hours, 4) if available_hours else None
    return {"required_hours": round(required_hours, 2),
            "available_hours": round(available_hours, 2),
            "max_reviews_per_day": max_reviews,
            "utilization": utilization,
            "feasible": required_hours <= available_hours}
