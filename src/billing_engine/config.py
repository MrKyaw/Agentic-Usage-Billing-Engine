"""Runtime config — env-driven, production-first (12-factor)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    api_key: str = _env("BILLING_API_KEY", "dev-key-123")
    db_path: str = _env("BILLING_DB_PATH", str(REPO_ROOT / "data" / "billing.db"))
    raw_log_dir: str = _env("BILLING_RAW_LOG_DIR", str(REPO_ROOT / "data" / "raw-log"))
    export_dir: str = _env("BILLING_EXPORT_DIR", str(REPO_ROOT / "exports"))
    plan_path: str = _env("BILLING_PLAN_PATH",
                          str(REPO_ROOT / "src" / "billing_engine" / "price_plans.json"))
    stage: str = _env("BILLING_STAGE", "local")
    log_level: str = _env("LOG_LEVEL", "INFO")
    max_batch: int = 100
    max_retries: int = 5  # SQS redrive budget before DLQ


SETTINGS = Settings()
