"""Event schema — FR1. Single source of truth for producers and pipeline."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class UsageEvent(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=128,
                          description="Producer-supplied idempotency key (UUID recommended)")
    customer_id: str = Field(..., min_length=1, max_length=128)
    product_id: str = Field(..., min_length=1, max_length=128)
    metric: str = Field(..., min_length=1, max_length=64,
                        description="e.g. cloud_credits, render_minutes, inference_tokens")
    quantity: float = Field(..., gt=0, le=1_000_000_000)
    timestamp: datetime = Field(..., description="Event time (not arrival time); ISO-8601")
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_id")
    @classmethod
    def _event_id_shape(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("event_id must be non-empty")
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_ts(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            dt = v
        else:
            s = str(v).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def billing_period(self) -> str:
        return self.timestamp.strftime("%Y-%m")

    def log_safe_dict(self) -> dict[str, Any]:
        """No PII beyond customer_id (allowed); metadata values are redacted to keys."""
        return {
            "event_id": self.event_id,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
            "metric": self.metric,
            "quantity": self.quantity,
            "timestamp": self.timestamp.isoformat(),
            "metadata_keys": sorted(self.metadata.keys()),
        }


class BatchIngestRequest(BaseModel):
    events: list[UsageEvent] = Field(..., min_length=1, max_length=100)


def coerce_event(payload: dict[str, Any]) -> UsageEvent:
    """Validate one raw payload; raises pydantic.ValidationError on 4xx-worthy input."""
    return UsageEvent.model_validate(payload)


def _unused_uuid_import_guard(v: str) -> str:  # keeps UUID import meaningful for docs
    UUID(v)
    return v
