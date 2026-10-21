"""Kafka transport and worker runtime — Kafka replaces SQS as the queue.

The billing core stays transport-agnostic. In the default local path the API
writes the raw log + inbox in one transaction (SQS analogue). When
``BILLING_KAFKA_BOOTSTRAP`` is set, the API writes the raw log and publishes the
normalized event to Kafka instead; a Kafka worker consumes the raw topic (or an
optionally Flink-deduped topic) and applies the same idempotent processor. The raw
log is still the source of truth, so a broker outage is recoverable by replay.

Design choices that matter:

- **At-least-once + idempotent insert.** Kafka delivery is at-least-once; the
  conditional insert on ``event_id`` makes duplicates harmless.
- **Key = customer_id.** All events for one customer land on one partition, so
  per-customer order is preserved through 20 → 40 partition scaling.
- **Producer idempotence.** ``enable.idempotence`` prevents broker-side
  duplicate writes from retries.
- **Poison messages go to ``usage-events-dlq``** with the original payload and
  error, then the offset is committed so one bad record cannot stall the topic.
- **Bounded failure isolation.** A missing broker/client never breaks ingest:
  the raw log is already durable, ``publish`` returns False and emits a metric.

Environment:
  BILLING_KAFKA_BOOTSTRAP       broker list (empty disables Kafka)
  BILLING_KAFKA_TOPIC           produce topic (default usage-events)
  BILLING_KAFKA_DLQ_TOPIC       poison topic (default usage-events-dlq)
  BILLING_KAFKA_GROUP           consumer group (default billing-worker)
  BILLING_KAFKA_OFFSET_RESET    earliest|latest (default earliest)
  BILLING_KAFKA_PROPERTIES      JSON object of extra librdkafka properties
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Mapping

from billing_engine.observability import METRICS, jlog

if TYPE_CHECKING:  # pragma: no cover - typing-only protocols
    from typing import Protocol, Sequence

    class ProducerLike(Protocol):
        def produce(self, topic: str, key: bytes | None = None,
                    value: bytes | None = None) -> None: ...

        def poll(self, timeout: float) -> int: ...

        def flush(self, timeout: float) -> int: ...

    class MessageLike(Protocol):
        def value(self) -> bytes: ...

        def error(self) -> Any: ...

    class ConsumerLike(Protocol):
        def subscribe(self, topics: Sequence[str]) -> None: ...

        def poll(self, timeout: float) -> MessageLike | None: ...

        def commit(self, message: MessageLike, asynchronous: bool = False) -> Any: ...

        def close(self) -> None: ...

PARTITION_EOF = -191  # confluent_kafka.KafkaError._PARTITION_EOF


class KafkaUnavailable(RuntimeError):
    """Kafka was selected but the client library or broker is unavailable."""


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap: str = ""
    topic: str = "usage-events"
    dlq_topic: str = "usage-events-dlq"
    group_id: str = "billing-worker"
    auto_offset_reset: str = "earliest"
    properties: Mapping[str, str] = field(default_factory=dict)

    @property
    def enabled(self) -> bool:
        return bool(self.bootstrap)

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        raw_properties = os.environ.get("BILLING_KAFKA_PROPERTIES", "")
        properties: dict[str, str] = {}
        if raw_properties:
            try:
                parsed = json.loads(raw_properties)
                if not isinstance(parsed, dict):
                    raise ValueError("must be a JSON object")
                properties = {str(k): str(v) for k, v in parsed.items()}
            except ValueError as e:
                raise ValueError(f"BILLING_KAFKA_PROPERTIES invalid: {e}") from e
        return cls(
            bootstrap=os.environ.get("BILLING_KAFKA_BOOTSTRAP", ""),
            topic=os.environ.get("BILLING_KAFKA_TOPIC", "usage-events"),
            dlq_topic=os.environ.get("BILLING_KAFKA_DLQ_TOPIC", "usage-events-dlq"),
            group_id=os.environ.get("BILLING_KAFKA_GROUP", "billing-worker"),
            auto_offset_reset=os.environ.get("BILLING_KAFKA_OFFSET_RESET", "earliest"),
            properties=properties,
        )


def _load_confluent() -> Any:  # pragma: no cover - import/environment boundary
    try:
        import confluent_kafka
    except ImportError as e:  # pragma: no cover
        raise KafkaUnavailable(
            "confluent-kafka is not installed; pip install -r requirements.txt"
        ) from e
    return confluent_kafka


class KafkaEventBus:
    """Small producer/consumer wrapper with injectable clients for tests."""

    def __init__(self, config: KafkaConfig | None = None,
                 loader: Callable[[], Any] | None = None,
                 producer: ProducerLike | None = None,
                 consumer: ConsumerLike | None = None) -> None:
        self.config = config or KafkaConfig()
        self._loader = loader or _load_confluent
        self._producer = producer
        self._consumer = consumer

    def _client_props(self) -> dict[str, Any]:
        return {"bootstrap.servers": self.config.bootstrap,
                **dict(self.config.properties)}

    def _get_producer(self) -> ProducerLike:
        if self._producer is None:
            module = self._loader()
            self._producer = module.Producer({
                **self._client_props(),
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
            })
        return self._producer

    def _get_consumer(self) -> ConsumerLike:
        if self._consumer is None:
            module = self._loader()
            self._consumer = module.Consumer({
                **self._client_props(),
                "group.id": self.config.group_id,
                "auto.offset.reset": self.config.auto_offset_reset,
                "enable.auto.commit": False,
            })
        return self._consumer

    def publish(self, event: Mapping[str, Any]) -> bool:
        """Best-effort produce. The raw log is already durable, so a False return
        is an operational signal (metric/alert), not a lost event."""
        if not self.config.enabled:
            return False
        try:
            producer = self._get_producer()
            producer.produce(
                self.config.topic,
                key=str(event.get("customer_id", "")).encode("utf-8"),
                value=json.dumps(dict(event), sort_keys=True,
                                 default=str).encode("utf-8"),
            )
            producer.poll(0)
        except Exception as e:  # noqa: BLE001 - broker/client failures never break ingest
            METRICS.inc("kafka_publish_failed_total")
            jlog("ERROR", "kafka_publish_failed", error=str(e)[:200],
                 event_id=event.get("event_id"))
            return False
        METRICS.inc("kafka_published_total")
        return True

    def publish_dlq(self, payload: Any, error: str) -> bool:
        if not self.config.enabled:
            return False
        body = json.dumps({"payload": payload, "error": error},
                          sort_keys=True, default=str).encode("utf-8")
        try:
            producer = self._get_producer()
            producer.produce(self.config.dlq_topic, value=body)
            producer.poll(0)
        except Exception as e:  # noqa: BLE001 - DLQ is best-effort too
            METRICS.inc("kafka_dlq_failed_total")
            jlog("ERROR", "kafka_dlq_failed", error=str(e)[:200])
            return False
        METRICS.inc("kafka_dlq_total")
        return True

    def consume(self, handler: Callable[[dict[str, Any]], str],
                max_messages: int | None = None,
                max_idle_polls: int | None = None,
                timeout_s: float = 1.0) -> dict[str, int]:
        """Consume and commit. ``max_messages``/``max_idle_polls`` bound the loop
        for smoke tests; both ``None`` means run until the process is stopped."""
        if not self.config.enabled:
            raise KafkaUnavailable(
                "Kafka is not configured (set BILLING_KAFKA_BOOTSTRAP)")
        consumer = self._get_consumer()
        consumer.subscribe([self.config.topic])
        counts = {"accepted": 0, "duplicate": 0, "dlq": 0, "errors": 0}
        processed = 0
        idle = 0
        while max_messages is None or processed < max_messages:
            message = consumer.poll(timeout_s)
            if message is None:
                idle += 1
                if max_idle_polls is not None and idle >= max_idle_polls:
                    break
                continue
            idle = 0
            error = message.error()
            if error is not None:
                if getattr(error, "code", lambda: None)() == PARTITION_EOF:
                    continue
                counts["errors"] += 1
                jlog("ERROR", "kafka_consume_error", error=str(error)[:200])
                continue
            raw = message.value()
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    raise ValueError("Kafka message must be a JSON object")
            except (TypeError, ValueError) as e:
                counts["dlq"] += 1
                self.publish_dlq(
                    {"raw": raw.decode("utf-8", errors="replace")
                     if isinstance(raw, bytes) else str(raw)}, str(e))
                consumer.commit(message=message, asynchronous=False)
                processed += 1
                continue
            dlq_error: str | None = None
            try:
                status = handler(payload)
            except Exception as e:  # noqa: BLE001 - poison must not stop the stream
                status, dlq_error = "dlq", f"handler error: {e}"
            if status == "dlq":
                self.publish_dlq(payload, dlq_error or "handler returned dlq")
            if status not in counts:
                status = "errors"
            counts[status] += 1
            consumer.commit(message=message, asynchronous=False)
            processed += 1
        return counts

    def flush(self, timeout: float = 5.0) -> int:
        if self._producer is None:
            return 0
        return int(self._producer.flush(timeout))

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        if self._producer is not None:
            self._producer.flush(5.0)
            self._producer = None


_BUS: KafkaEventBus | None = None


def get_event_bus() -> KafkaEventBus:
    """Process-wide bus configured from the environment (lazy, injectable)."""
    global _BUS
    if _BUS is None:
        _BUS = KafkaEventBus(KafkaConfig.from_env())
    return _BUS


def reset_event_bus() -> None:
    global _BUS
    _BUS = None
