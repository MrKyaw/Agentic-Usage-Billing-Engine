"""OTEL bridge: OTLP/HTTP payload shape, batching, failure isolation, env config."""
from __future__ import annotations

import json

import pytest

from billing_engine.observability import Metrics, OtelBridge

pytestmark = pytest.mark.req("FR6", "NFR-GOVERNANCE")


class Recorder:
    def __init__(self, status: int = 200):
        self.status = status
        self.calls: list[tuple[str, bytes, dict[str, str]]] = []

    def __call__(self, url: str, body: bytes, headers: dict[str, str]) -> int:
        self.calls.append((url, body, headers))
        if isinstance(self.status, Exception):
            raise self.status
        return self.status


def test_bridge_validates_and_normalizes_endpoint():
    with pytest.raises(ValueError, match="endpoint"):
        OtelBridge("")
    with pytest.raises(ValueError, match="max_failures"):
        OtelBridge("http://collector:4318", max_failures=0)
    assert OtelBridge("http://collector:4318/").url == "http://collector:4318/v1/metrics"
    assert OtelBridge("http://collector:4318/v1/metrics").url == \
        "http://collector:4318/v1/metrics"


def test_payload_is_otlp_json_with_cumulative_sums_and_gauges():
    bridge = OtelBridge("http://collector:4318", service_name="billing-test",
                        clock=lambda: 2.0)
    payload = bridge.payload({"ingest_total": 7, "duplicate_total": 2},
                             {"queue_depth": 3}, start_ns=1_000)
    resource = payload["resourceMetrics"][0]
    assert resource["resource"]["attributes"] == [
        {"key": "service.name", "value": {"stringValue": "billing-test"}}]
    metrics = resource["scopeMetrics"][0]["metrics"]
    assert [m["name"] for m in metrics] == [
        "duplicate_total", "ingest_total", "queue_depth"]  # deterministic order
    ingest = next(m for m in metrics if m["name"] == "ingest_total")["sum"]
    assert ingest["aggregationTemporality"] == 2 and ingest["isMonotonic"] is True
    point = ingest["dataPoints"][0]
    assert point["asInt"] == "7" and point["startTimeUnixNano"] == "1000"
    assert point["timeUnixNano"] == str(2_000_000_000)
    gauge = next(m for m in metrics if m["name"] == "queue_depth")["gauge"]
    assert gauge["dataPoints"][0]["asDouble"] == 3.0
    assert bridge.payload({}, {}, 0)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"] == []


def test_export_posts_otlp_json_and_tracks_success():
    recorder = Recorder(202)
    bridge = OtelBridge("http://collector:4318", transport=recorder)
    assert bridge.export({"events": 1}, {"depth": 0}, 0) is True
    assert bridge.last_status == 202 and bridge.failures == 0 and bridge.enabled is True
    url, body, headers = recorder.calls[0]
    assert url == "http://collector:4318/v1/metrics"
    assert headers == {"Content-Type": "application/json"}
    assert json.loads(body)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]


def test_export_failures_are_bounded_and_never_raise():
    recorder = Recorder(500)
    bridge = OtelBridge("http://collector:4318", transport=recorder, max_failures=2)
    assert bridge.export({}, {}, 0) is False
    assert bridge.failures == 1 and bridge.last_status == 500 and bridge.enabled is True
    assert bridge.export({}, {}, 0) is False
    assert bridge.failures == 2 and bridge.enabled is False
    assert bridge.export({}, {}, 0) is False  # short-circuits, no new call
    assert len(recorder.calls) == 2

    exploding = Recorder(RuntimeError("collector down"))
    flaky = OtelBridge("http://collector:4318", transport=exploding, max_failures=5)
    assert flaky.export({}, {}, 0) is False
    assert flaky.failures == 1 and flaky.last_status is None and flaky.enabled is True


def test_metrics_batches_dirty_state_and_exports_on_snapshot():
    recorder = Recorder()
    metrics = Metrics()
    bridge = metrics.enable_otel("http://collector:4318", transport=recorder)
    assert bridge is metrics.otel
    assert metrics.flush_otel() is False  # nothing changed yet
    metrics.inc("processor_accepted_total", 2)
    metrics.set_gauge("queue_pending", 4)
    assert metrics.flush_otel() is True
    assert metrics.flush_otel() is False  # dirty flag cleared
    assert len(recorder.calls) == 1

    metrics.inc("processor_error_total")
    snap = metrics.snapshot()  # scrape is the export point
    assert len(recorder.calls) == 2
    assert snap["otel"] == {"enabled": True, "failures": 0, "last_status": 200}
    assert snap["counters"]["processor_accepted_total"] == 2


def test_metrics_without_otel_reports_disabled():
    metrics = Metrics()
    assert metrics.otel is None
    assert metrics.flush_otel() is False
    snap = metrics.snapshot()
    assert snap["otel"] == {"enabled": False, "failures": 0, "last_status": None}
    assert metrics.check_alarms(dlq_depth=1)[0]["alarm"] == "DLQDepthPositive"


def test_env_configuration_enables_bridge(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://collector:4318/")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "billing-prod")
    metrics = Metrics()
    assert metrics.otel is not None
    assert metrics.otel.url == "http://collector:4318/v1/metrics"
    assert metrics.otel.service_name == "billing-prod"
