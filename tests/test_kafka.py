"""Kafka transport, Kafka worker, streaming ingest mode and the API bridge."""
from __future__ import annotations

import json

import pytest
from conftest import make_event
from fastapi.testclient import TestClient

from billing_engine.api import app
from billing_engine.ingest import _publish, ingest_batch, ingest_one
from billing_engine.kafka_transport import (
    KafkaConfig,
    KafkaEventBus,
    KafkaUnavailable,
    get_event_bus,
    reset_event_bus,
)
from billing_engine.kafka_worker import handle_event, main as worker_main, run as worker_run
from billing_engine.processor import process_event
from billing_engine.store import Store

pytestmark = pytest.mark.req("NFR-STREAMING", "FR2", "NFR-GUARDRAILS")

H = {"X-API-Key": "dev-key-123"}


@pytest.fixture(autouse=True)
def _reset_bus():
    reset_event_bus()
    yield
    reset_event_bus()


class FakeProducer:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.messages: list[tuple[str, bytes | None, bytes | None]] = []
        self.polls = 0
        self.flushes: list[float] = []

    def produce(self, topic: str, key: bytes | None = None,
                value: bytes | None = None) -> None:
        if self.fail:
            raise RuntimeError("broker down")
        self.messages.append((topic, key, value))

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float) -> int:
        self.flushes.append(timeout)
        return len(self.messages)


class FakeError:
    def __init__(self, code: int):
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return f"kafka-error-{self._code}"


class FakeMessage:
    def __init__(self, value: bytes | None, error: FakeError | None = None):
        self._value = value
        self._error = error

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> FakeError | None:
        return self._error


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage | None] | None = None):
        self.messages = list(messages or [])
        self.subscribed: list[list[str]] = []
        self.commits: list[FakeMessage] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed.append(list(topics))

    def poll(self, timeout: float) -> FakeMessage | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, message: FakeMessage, asynchronous: bool = False) -> None:
        self.commits.append(message)

    def close(self) -> None:
        self.closed = True


class FakeModule:
    """Stand-in for the confluent_kafka module."""

    def __init__(self, producer: FakeProducer | None = None,
                 consumer: FakeConsumer | None = None):
        self._producer = producer or FakeProducer()
        self._consumer = consumer or FakeConsumer()
        self.producer_props: dict | None = None
        self.consumer_props: dict | None = None

    def Producer(self, props: dict) -> FakeProducer:
        self.producer_props = props
        return self._producer

    def Consumer(self, props: dict) -> FakeConsumer:
        self.consumer_props = props
        return self._consumer


def _bus(*, producer=None, consumer=None, **config_kwargs) -> KafkaEventBus:
    return KafkaEventBus(KafkaConfig(bootstrap="kafka:9092", **config_kwargs),
                         producer=producer, consumer=consumer)


# --------------------------------------------------------------------- config

def test_config_defaults_and_enabled(monkeypatch):
    for name in ("BILLING_KAFKA_BOOTSTRAP", "BILLING_KAFKA_TOPIC",
                 "BILLING_KAFKA_DLQ_TOPIC", "BILLING_KAFKA_GROUP",
                 "BILLING_KAFKA_OFFSET_RESET", "BILLING_KAFKA_PROPERTIES"):
        monkeypatch.delenv(name, raising=False)
    cfg = KafkaConfig.from_env()
    assert cfg == KafkaConfig()
    assert cfg.enabled is False
    assert KafkaConfig(bootstrap="kafka:9092").enabled is True


def test_config_reads_env_and_extra_properties(monkeypatch):
    monkeypatch.setenv("BILLING_KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv("BILLING_KAFKA_TOPIC", "usage-events-deduped")
    monkeypatch.setenv("BILLING_KAFKA_DLQ_TOPIC", "dead")
    monkeypatch.setenv("BILLING_KAFKA_GROUP", "billing-worker-2")
    monkeypatch.setenv("BILLING_KAFKA_OFFSET_RESET", "latest")
    monkeypatch.setenv("BILLING_KAFKA_PROPERTIES",
                       '{"security.protocol": "SASL_SSL", "sasl.mechanism": "AWS_MSK_IAM"}')
    cfg = KafkaConfig.from_env()
    assert cfg.bootstrap == "kafka:9092"
    assert cfg.topic == "usage-events-deduped"
    assert cfg.dlq_topic == "dead"
    assert cfg.group_id == "billing-worker-2"
    assert cfg.auto_offset_reset == "latest"
    assert cfg.properties["security.protocol"] == "SASL_SSL"
    assert cfg.properties["sasl.mechanism"] == "AWS_MSK_IAM"


def test_config_rejects_invalid_properties(monkeypatch):
    monkeypatch.setenv("BILLING_KAFKA_PROPERTIES", "{not-json")
    with pytest.raises(ValueError, match="BILLING_KAFKA_PROPERTIES"):
        KafkaConfig.from_env()
    monkeypatch.setenv("BILLING_KAFKA_PROPERTIES", '["not", "an", "object"]')
    with pytest.raises(ValueError, match="JSON object"):
        KafkaConfig.from_env()


# -------------------------------------------------------------------- publish

def test_publish_is_disabled_without_bootstrap():
    producer = FakeProducer()
    bus = KafkaEventBus(KafkaConfig(), producer=producer)
    assert bus.publish(make_event("e1")) is False
    assert producer.messages == []


def test_publish_keys_by_customer_and_serializes_canonically():
    producer = FakeProducer()
    bus = _bus(producer=producer)
    event = make_event("e1", customer_id="acme")
    assert bus.publish(event) is True
    topic, key, value = producer.messages[0]
    assert topic == "usage-events"
    assert key == b"acme"
    assert json.loads(value) == event
    assert value == json.dumps(event, sort_keys=True).encode()
    assert producer.polls == 1


def test_publish_failure_is_isolated():
    bus = _bus(producer=FakeProducer(fail=True))
    assert bus.publish(make_event("e1")) is False


def test_lazy_producer_uses_idempotent_producer_config():
    module = FakeModule()
    bus = KafkaEventBus(KafkaConfig(bootstrap="kafka:9092",
                                    properties={"security.protocol": "SASL_SSL"}),
                        loader=lambda: module)
    assert bus.publish(make_event("e1")) is True
    props = module.producer_props
    assert props["bootstrap.servers"] == "kafka:9092"
    assert props["security.protocol"] == "SASL_SSL"
    assert props["enable.idempotence"] is True
    assert props["acks"] == "all"


def test_publish_dlq_disabled_success_and_failure():
    disabled = KafkaEventBus(KafkaConfig(), producer=FakeProducer())
    assert disabled.publish_dlq({"x": 1}, "bad") is False

    producer = FakeProducer()
    bus = _bus(dlq_topic="usage-events-dlq", producer=producer)
    assert bus.publish_dlq({"event_id": "e1"}, "unratable") is True
    topic, key, value = producer.messages[0]
    assert topic == "usage-events-dlq" and key is None
    assert json.loads(value) == {"payload": {"event_id": "e1"},
                                 "error": "unratable"}

    broken = _bus(producer=FakeProducer(fail=True))
    assert broken.publish_dlq({"x": 1}, "bad") is False


# -------------------------------------------------------------------- consume

def test_consume_requires_bootstrap():
    with pytest.raises(KafkaUnavailable, match="BILLING_KAFKA_BOOTSTRAP"):
        KafkaEventBus(KafkaConfig()).consume(lambda p: "accepted")


def test_consume_handler_results_commit_and_counts():
    consumer = FakeConsumer([
        FakeMessage(json.dumps(make_event("e1")).encode()),
        FakeMessage(json.dumps(make_event("e2")).encode()),
    ])
    bus = _bus(consumer=consumer)
    counts = bus.consume(lambda payload: "accepted" if payload["event_id"] == "e1"
                         else "duplicate", max_messages=2)
    assert counts == {"accepted": 1, "duplicate": 1, "dlq": 0, "errors": 0}
    assert consumer.subscribed == [["usage-events"]]
    assert len(consumer.commits) == 2


def test_consume_poison_payloads_go_to_dlq_and_commit():
    producer = FakeProducer()
    consumer = FakeConsumer([FakeMessage(b"{not-json"), FakeMessage(b"[]")])
    bus = _bus(producer=producer, consumer=consumer)
    counts = bus.consume(lambda payload: "accepted", max_messages=2)
    assert counts["dlq"] == 2 and counts["accepted"] == 0
    assert len(consumer.commits) == 2
    bodies = [json.loads(value) for topic, _, value in producer.messages]
    assert all(topic == "usage-events-dlq" for topic, _, _ in producer.messages)
    assert bodies[0]["payload"]["raw"] == "{not-json"
    assert bodies[1]["payload"]["raw"] == "[]"


def test_consume_handler_exception_and_dlq_status_publish_once():
    producer = FakeProducer()
    consumer = FakeConsumer([
        FakeMessage(json.dumps(make_event("boom")).encode()),
        FakeMessage(json.dumps(make_event("poison")).encode()),
    ])
    bus = _bus(producer=producer, consumer=consumer)

    def handler(payload: dict) -> str:
        if payload["event_id"] == "boom":
            raise RuntimeError("handler exploded")
        return "dlq"

    counts = bus.consume(handler, max_messages=2)
    assert counts["dlq"] == 2 and counts["errors"] == 0
    assert len(producer.messages) == 2  # exactly one DLQ record per failure
    assert len(consumer.commits) == 2


def test_consume_unknown_status_counts_as_error():
    consumer = FakeConsumer([FakeMessage(json.dumps(make_event("e1")).encode())])
    producer = FakeProducer()
    counts = _bus(producer=producer, consumer=consumer).consume(
        lambda payload: "nonsense", max_messages=1)
    assert counts["errors"] == 1
    assert producer.messages == []
    assert len(consumer.commits) == 1


def test_consume_error_messages_and_idle_polls():
    consumer = FakeConsumer([
        FakeMessage(b"", error=FakeError(-191)),  # partition EOF: skip
        FakeMessage(b"", error=FakeError(1)),     # real error: count
        None,                                     # idle -> break
    ])
    counts = _bus(consumer=consumer).consume(lambda p: "accepted",
                                             max_messages=1, max_idle_polls=1)
    assert counts["errors"] == 1 and counts["accepted"] == 0

    consumer2 = FakeConsumer([None, FakeMessage(json.dumps(make_event("e1")).encode())])
    counts2 = _bus(consumer=consumer2).consume(lambda p: "accepted",
                                               max_messages=1, max_idle_polls=2)
    assert counts2["accepted"] == 1


def test_lazy_consumer_uses_group_and_manual_commit_config():
    module = FakeModule(consumer=FakeConsumer())
    bus = KafkaEventBus(KafkaConfig(bootstrap="kafka:9092", group_id="g1",
                                    auto_offset_reset="latest"),
                        loader=lambda: module)
    counts = bus.consume(lambda p: "accepted", max_messages=1, max_idle_polls=1)
    assert counts == {"accepted": 0, "duplicate": 0, "dlq": 0, "errors": 0}
    props = module.consumer_props
    assert props["group.id"] == "g1"
    assert props["auto.offset.reset"] == "latest"
    assert props["enable.auto.commit"] is False


def test_flush_and_close_are_idempotent():
    producer = FakeProducer()
    consumer = FakeConsumer()
    bus = _bus(producer=producer, consumer=consumer)
    assert bus.flush() == 0
    bus.publish(make_event("e1"))
    assert bus.flush(1.5) == 1
    bus.close()
    assert consumer.closed is True
    assert producer.flushes[-1] == 5.0
    bus.close()  # second close is a no-op
    assert KafkaEventBus(KafkaConfig()).flush() == 0


def test_event_bus_singleton_caches_and_resets(monkeypatch):
    monkeypatch.delenv("BILLING_KAFKA_BOOTSTRAP", raising=False)
    first = get_event_bus()
    assert get_event_bus() is first
    assert first.config.enabled is False
    reset_event_bus()
    monkeypatch.setenv("BILLING_KAFKA_BOOTSTRAP", "kafka:9092")
    assert get_event_bus().config.enabled is True
    reset_event_bus()


# --------------------------------------------------------------- process_event

def test_process_event_accepts_duplicates_and_dlq(tmp_path):
    store = Store(tmp_path / "events.db", tmp_path / "raw")
    event = make_event("stream-1", quantity=2.0)
    assert process_event(store, event) == ("accepted", None)
    assert process_event(store, event) == ("duplicate", None)
    status, reason = process_event(store, {"event_id": "bad", "metric": "unknown",
                                           "quantity": 1.0})
    assert status == "dlq" and reason is not None and "unratable" in reason
    status, reason = process_event(store, {"event_id": "bad2", "metric": "cloud_credits",
                                           "quantity": "not-a-number"})
    assert status == "dlq" and reason is not None


# ------------------------------------------------------------------ kafka_worker

class FakeBus:
    def __init__(self):
        self.config = KafkaConfig(bootstrap="kafka:9092", topic="usage-events-deduped")
        self.calls: list[dict] = []
        self.closed = False

    def consume(self, handler, **kwargs):
        self.calls.append({"handler": handler, **kwargs})
        return {"accepted": 1, "duplicate": 0, "dlq": 0, "errors": 0}

    def close(self):
        self.closed = True


def test_kafka_worker_handles_and_runs(tmp_path):
    store = Store(tmp_path / "worker.db", tmp_path / "raw")
    assert handle_event(store, make_event("w1")) == "accepted"
    assert handle_event(store, make_event("w1")) == "duplicate"
    assert handle_event(store, {"event_id": "w2", "metric": "nope",
                                "quantity": 1.0}) == "dlq"

    bus = FakeBus()
    counts = worker_run(bus, store, max_messages=1, max_idle_polls=1)
    assert counts["accepted"] == 1
    assert bus.calls[0]["max_messages"] == 1
    assert bus.calls[0]["max_idle_polls"] == 1
    # the captured handler still processes real events
    assert bus.calls[0]["handler"](make_event("w3")) == "accepted"


def test_kafka_worker_main_closes_bus(monkeypatch):
    bus = FakeBus()
    monkeypatch.setattr("billing_engine.kafka_worker.get_event_bus", lambda: bus)
    assert worker_main(["--max-messages", "2", "--timeout-s", "0.01"]) == 0
    assert bus.closed is True
    assert bus.calls[0]["max_messages"] == 2


def test_kafka_worker_resolves_default_bus_and_store(monkeypatch, tmp_path):
    store = Store(tmp_path / "default-worker.db", tmp_path / "raw")
    bus = FakeBus()
    monkeypatch.setattr("billing_engine.kafka_worker.get_event_bus", lambda: bus)
    monkeypatch.setattr("billing_engine.kafka_worker.get_store", lambda: store)
    assert worker_run()["accepted"] == 1


# ------------------------------------------------------- streaming ingest mode

def test_ingest_kafka_mode_writes_raw_log_without_inbox(tmp_path):
    store = Store(tmp_path / "kafka.db", tmp_path / "raw")
    result = ingest_one(store, make_event("k-1"), enqueue=False)
    assert result["status"] == "accepted"
    assert store.pending_count() == 0
    assert [e["event_id"] for e in store.all_raw_payloads()] == ["k-1"]


def test_ingest_batch_kafka_mode_writes_raw_log_without_inbox(tmp_path):
    store = Store(tmp_path / "batch.db", tmp_path / "raw")
    result = ingest_batch(store, [make_event("b-1"), make_event("b-2")],
                          enqueue=False)
    assert result["accepted_count"] == 2
    assert store.pending_count() == 0
    assert {e["event_id"] for e in store.all_raw_payloads()} == {"b-1", "b-2"}


def test_publish_failure_is_swallowed_after_durability(tmp_path):
    store = Store(tmp_path / "pub.db", tmp_path / "raw")

    class BrokenPublisher:
        def publish(self, event):
            raise RuntimeError("broker down")

    result = ingest_one(store, make_event("p-1"), publisher=BrokenPublisher())
    assert result["status"] == "accepted"
    assert store.pending_count() == 1
    _publish(None, make_event("ignored"))  # no publisher is a no-op


class ApiBus:
    def __init__(self, enabled: bool = True):
        self.config = KafkaConfig(bootstrap="kafka:9092" if enabled else "")
        self.published: list[dict] = []

    def publish(self, event):
        if not self.config.enabled:
            return False
        self.published.append(event)
        return True


def test_api_streaming_mode_publishes_and_skips_inbox(default_store, monkeypatch):
    bus = ApiBus(enabled=True)
    monkeypatch.setattr("billing_engine.api.get_event_bus", lambda: bus)
    client = TestClient(app)
    r = client.post("/v1/usage-events", json=make_event("api-k-1"), headers=H)
    assert r.status_code == 202
    assert [e["event_id"] for e in bus.published] == ["api-k-1"]
    assert default_store.pending_count() == 0
    assert [e["event_id"] for e in default_store.all_raw_payloads()] == ["api-k-1"]

    r = client.post("/v1/usage-events:batch",
                    json={"events": [make_event("api-k-2"), make_event("api-k-3")]},
                    headers=H)
    assert r.status_code == 202
    assert {e["event_id"] for e in bus.published} == {"api-k-1", "api-k-2", "api-k-3"}
    assert default_store.pending_count() == 0


def test_api_default_mode_keeps_inbox(default_store, monkeypatch):
    bus = ApiBus(enabled=False)
    monkeypatch.setattr("billing_engine.api.get_event_bus", lambda: bus)
    client = TestClient(app)
    assert client.post("/v1/usage-events", json=make_event("api-sqs-1"),
                       headers=H).status_code == 202
    assert default_store.pending_count() == 1
    assert bus.published == []
