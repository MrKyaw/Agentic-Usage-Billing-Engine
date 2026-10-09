"""HTTP API — FR1 + FR4 + FR6 (API Gateway + Query Lambda analogue).

Local: FastAPI. AWS: same handler logic behind API Gateway → Lambda (see infra).
Auth: X-API-Key (POC; production = Cognito/SSO per ROADMAP).
Ingest returns 202 immediately; processing happens async (worker) or inline in
tests/demo via ?sync=true for determinism.
"""
from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

from billing_engine.config import SETTINGS
from billing_engine.ingest import IngestRejected, ingest_batch, ingest_one
from billing_engine.observability import METRICS
from billing_engine.period_close import close_period
from billing_engine.query import get_bill, get_usage
from billing_engine.reconcile import reconcile
from billing_engine.store import get_store

app = FastAPI(title="Agentic Usage Billing Engine", version="0.1.0")


def require_key(x_api_key: str | None = Header(default=None)) -> None:
    if x_api_key != SETTINGS.api_key:
        raise HTTPException(status_code=401, detail="invalid API key")


class BatchBody(BaseModel):
    events: list[dict[str, Any]]


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "stage": SETTINGS.stage, "version": "0.1.0"}


@app.post("/v1/usage-events", status_code=202)
def post_event(body: dict[str, Any], sync: bool = False,
               _k: None = Depends(require_key)) -> dict[str, Any]:
    store = get_store()
    try:
        res = ingest_one(store, body)
    except IngestRejected as e:
        raise HTTPException(status_code=400, detail=str(e)[:500])
    if sync:
        from billing_engine.processor import drain
        drain(store)
    return res


@app.post("/v1/usage-events:batch", status_code=202)
def post_batch(body: BatchBody, sync: bool = False,
               _k: None = Depends(require_key)) -> dict[str, Any]:
    store = get_store()
    if len(body.events) > SETTINGS.max_batch:
        raise HTTPException(status_code=400, detail=f"batch exceeds {SETTINGS.max_batch}")
    try:
        res = ingest_batch(store, body.events)
    except IngestRejected as e:
        raise HTTPException(status_code=400, detail=str(e)[:500])
    if sync:
        from billing_engine.processor import drain
        drain(store)
    return res


@app.get("/v1/customers/{cid}/usage")
def usage(cid: str, period: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
          _k: None = Depends(require_key)) -> dict[str, Any]:
    return get_usage(get_store(), cid, period)


@app.get("/v1/customers/{cid}/bill")
def bill(cid: str, period: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
         _k: None = Depends(require_key)) -> dict[str, Any]:
    return get_bill(get_store(), cid, period)


@app.post("/v1/customers/{cid}/close", status_code=200)
def close(cid: str, period: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
          _k: None = Depends(require_key)) -> dict[str, Any]:
    return close_period(get_store(), cid, period)


@app.get("/v1/ops/metrics")
def metrics(_k: None = Depends(require_key)) -> dict[str, Any]:
    store = get_store()
    snap = METRICS.snapshot()
    snap["queue"] = {"pending": store.pending_count(), "dlq": store.dlq_count(),
                     "oldest_pending_age_s": round(store.oldest_pending_age_s(), 2)}
    snap["alarms"] = METRICS.check_alarms(snap["queue"]["oldest_pending_age_s"],
                                          snap["queue"]["dlq"])
    return snap


@app.get("/v1/ops/reconcile")
def ops_reconcile(customer: str | None = None, period: str | None = None,
                  _k: None = Depends(require_key)) -> dict[str, Any]:
    return reconcile(get_store(), customer, period)
