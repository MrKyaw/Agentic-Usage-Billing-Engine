"""HTTP API — FR1 + FR4 + FR6 (API Gateway + Query Lambda analogue).

Local: FastAPI. AWS: same handler logic behind API Gateway → Lambda (see infra).
Auth: X-API-Key (POC; production = Cognito/SSO per ROADMAP).
Ingest returns 202 immediately; processing happens async (worker) or inline in
tests/demo via ?sync=true for determinism.
"""
from __future__ import annotations

import os
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.responses import HTMLResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from billing_engine.config import SETTINGS
from billing_engine.ingest import IngestRejected, ingest_batch, ingest_one
from billing_engine.observability import METRICS
from billing_engine.period_close import close_period
from billing_engine.query import get_bill, get_usage
from billing_engine.reconcile import reconcile
from billing_engine.store import get_store

app = FastAPI(title="Agentic Usage Billing Engine", version="0.1.0")


# declared as an OpenAPI security scheme so /docs offers an "Authorize" button
_api_key = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_key(x_api_key: str | None = Security(_api_key)) -> None:
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
    if not 1 <= len(body.events) <= SETTINGS.max_batch:
        raise HTTPException(status_code=400,
                            detail=f"batch must contain 1..{SETTINGS.max_batch} events")
    try:
        res = ingest_batch(store, body.events)
    except IngestRejected as e:
        raise HTTPException(status_code=400, detail=str(e)[:500])
    if sync:
        from billing_engine.processor import drain
        drain(store)
    return res


PERIOD = r"^\d{4}-(0[1-9]|1[0-2])$"


@app.get("/v1/customers/{cid}/usage")
def usage(cid: str, period: str = Query(..., pattern=PERIOD),
          _k: None = Depends(require_key)) -> dict[str, Any]:
    return get_usage(get_store(), cid, period)


@app.get("/v1/customers/{cid}/bill")
def bill(cid: str, period: str = Query(..., pattern=PERIOD),
         _k: None = Depends(require_key)) -> dict[str, Any]:
    return get_bill(get_store(), cid, period)


@app.post("/v1/customers/{cid}/close", status_code=200)
def close(cid: str, period: str = Query(..., pattern=PERIOD),
          _k: None = Depends(require_key)) -> dict[str, Any]:
    export_dir = os.environ.get("BILLING_EXPORT_DIR", SETTINGS.export_dir)
    return close_period(get_store(), cid, period, export_dir=export_dir)


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
def ops_reconcile(customer: str | None = None,
                  period: str | None = Query(None, pattern=PERIOD),
                  _k: None = Depends(require_key)) -> dict[str, Any]:
    return reconcile(get_store(), customer, period)


@app.get("/ui", include_in_schema=False, response_class=HTMLResponse)
def ui() -> str:
    """Self-contained test console — zero external assets (works offline,
    unlike /docs which needs the jsDelivr CDN)."""
    return """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Billing Engine — test console</title>
<style>body{font-family:system-ui,sans-serif;max-width:860px;margin:2rem auto;
padding:0 1rem}input,button,select{padding:.4rem;margin:.2rem}pre{background:#111;
color:#0e0;border-radius:8px;padding:1rem;overflow:auto;min-height:8rem}
.row{display:flex;flex-wrap:wrap;gap:.4rem;align-items:center}</style></head><body>
<h1>Agentic Usage Billing Engine <small>v0.1.0</small></h1>
<div class="row">API key <input id="k" size="16" value="dev-key-123">
customer <input id="c" size="8" value="acme"> period <input id="p" size="8" value="2026-10"></div>
<div class="row">
<button onclick="go('POST','/v1/usage-events?sync=true',sample())">ingest 1 event</button>
<button onclick="go('POST','/v1/usage-events:batch?sync=true',{events:[sample(),sample()]})">ingest batch</button>
<button onclick="go('GET',u('usage'))">usage</button>
<button onclick="go('GET',u('bill'))">bill</button>
<button onclick="go('POST',u('close')+'&period='+v('p'),null)">close period</button>
<button onclick="go('GET','/v1/ops/metrics')">metrics</button>
<button onclick="go('GET','/v1/ops/reconcile?customer='+v('c')+'&period='+v('p'))">reconcile</button>
</div><pre id="o">output…</pre>
<script>const v=id=>document.getElementById(id).value;
const u=x=>'/v1/customers/'+v('c')+'/'+x+'?period='+v('p');
const sample=()=>({event_id:'evt-'+Math.random().toString(36).slice(2,9),
customer_id:v('c'),product_id:'revit',metric:'cloud_credits',
quantity:10,timestamp:new Date().toISOString()});
async function go(m,p,b){const o=document.getElementById('o');o.textContent=m+' '+p+' …';
try{const r=await fetch(p,{method:m,headers:{'Content-Type':'application/json',
'X-API-Key':v('k')},body:b?JSON.stringify(b):undefined});
o.textContent=r.status+' '+r.statusText+'\\n'+JSON.stringify(await r.json(),null,2);}
catch(e){o.textContent='ERROR: '+e;}}</script></body></html>"""
