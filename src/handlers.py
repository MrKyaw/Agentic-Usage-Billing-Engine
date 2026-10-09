"""Lambda adapters — thin shims so the SAME ingest/processor/query code runs
locally (FastAPI/worker) and in AWS Lambda. No business logic here."""
import json


def _resp(status, body):
    return {"statusCode": status, "body": json.dumps(body),
            "headers": {"Content-Type": "application/json"}}


def ingest_handler(event, context=None):  # API GW proxy → ingest_one/ingest_batch
    from billing_engine.ingest import IngestRejected, ingest_batch, ingest_one
    from billing_engine.store import get_store  # in Lambda, backed by DDB/S3 adapters
    store = get_store()
    try:
        body = json.loads(event.get("body") or "{}")
        path = event.get("path", "")
        if path.endswith(":batch") or "events" in body:
            evs = body["events"] if "events" in body else [body]
            return _resp(202, ingest_batch(store, evs))
        return _resp(202, ingest_one(store, body))
    except IngestRejected as e:
        return _resp(400, {"error": str(e)[:500]})


def processor_handler(event, context=None):  # SQS event source → process_one
    from billing_engine.processor import process_one
    from billing_engine.store import get_store
    store = get_store()
    out = {"accepted": 0, "duplicate": 0, "dlq": 0}
    for rec in event.get("Records", []):
        item = {"seq": rec["messageId"], "event_id": "?",
                "payload": json.loads(rec["body"]), "attempts": int(
                    rec.get("attributes", {}).get("ApproximateReceiveCount", 1))}
        out[process_one(store, item)] += 1
    return out


def query_handler(event, context=None):
    from billing_engine.query import get_bill, get_usage
    from billing_engine.store import get_store
    store = get_store()
    pp = event.get("pathParameters") or {}
    qs = event.get("queryStringParameters") or {}
    cid, period = pp.get("id", ""), qs.get("period", "")
    if event.get("path", "").endswith("/bill"):
        return _resp(200, get_bill(store, cid, period))
    return _resp(200, get_usage(store, cid, period))


# CDK handler paths (handlers.<name>.handler)
ingest = ingest_handler
processor = processor_handler
query = query_handler
