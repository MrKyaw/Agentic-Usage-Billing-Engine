"""The whole solution is one Compose project: monolith + Kafka + Flink.

These are configuration guardrails. They parse the real ``docker-compose.yml``
and assert the wiring the runtime depends on:

* one switch (``BILLING_KAFKA_BOOTSTRAP``) moves the API *and* the worker
  between the SQLite inbox and the Kafka queue,
* the streaming tier is opt-in (a Compose profile), so the default stack stays
  small,
* topics exist before the API publishes, Kafka has 20 partitions, and the Flink
  SQL job reads and writes the documented topics.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.yml"
STREAMING_DIR = ROOT / "infra" / "streaming"

pytestmark = pytest.mark.req("NFR-STREAMING", "NFR-GUARDRAILS")


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text())


def test_one_compose_file_defines_the_whole_solution(compose):
    """The monolith and the streaming tier live in the same project: one
    `docker compose` up starts the complete solution."""
    assert compose["services"]
    for name in ("api", "worker", "kafka", "kafka-init", "jobmanager",
                 "taskmanager", "flink-submit"):
        assert name in compose["services"], name


def test_streaming_tier_is_opt_in_but_the_monolith_always_runs(compose):
    services = compose["services"]
    for name in ("kafka", "kafka-init", "jobmanager", "taskmanager", "flink-submit"):
        assert services[name].get("profiles") == ["streaming"], name
    for name in ("api", "worker"):
        assert "profiles" not in services[name], name


def test_one_switch_moves_api_and_worker_to_kafka(compose):
    api_env = compose["services"]["api"]["environment"]
    worker_env = compose["services"]["worker"]["environment"]
    assert api_env["BILLING_KAFKA_BOOTSTRAP"] == "${BILLING_KAFKA_BOOTSTRAP:-}"
    assert worker_env["BILLING_KAFKA_BOOTSTRAP"] == api_env["BILLING_KAFKA_BOOTSTRAP"]
    assert worker_env["BILLING_KAFKA_TOPIC"] == api_env["BILLING_KAFKA_TOPIC"]
    assert worker_env["BILLING_KAFKA_DLQ_TOPIC"] == api_env["BILLING_KAFKA_DLQ_TOPIC"]
    assert worker_env["BILLING_KAFKA_GROUP"] == api_env["BILLING_KAFKA_GROUP"]


def test_worker_uses_the_unified_entry_point(compose):
    """`python -m billing_engine.worker` is the single worker command: it polls
    the inbox or consumes Kafka, depending on the transport switch."""
    worker = compose["services"]["worker"]
    assert worker["command"] == ["python", "-m", "billing_engine.worker"]
    assert worker["image"] == compose["services"]["api"]["image"]
    assert worker["volumes"] == compose["services"]["api"]["volumes"]


def test_topics_exist_before_the_api_and_worker_start(compose):
    for name in ("api", "worker"):
        dep = compose["services"][name]["depends_on"]["kafka-init"]
        assert dep["condition"] == "service_completed_successfully"
        # required:false keeps the monolith valid without the Kafka profile
        assert dep["required"] is False
    kafka_init = compose["services"]["kafka-init"]
    assert kafka_init["depends_on"]["kafka"]["condition"] == "service_healthy"


def test_kafka_broker_keeps_the_20_partition_plan(compose):
    kafka = compose["services"]["kafka"]
    assert kafka["environment"]["KAFKA_NUM_PARTITIONS"] == 20
    assert kafka["environment"]["KAFKA_PROCESS_ROLES"] == "broker,controller"
    assert "9092" in kafka["environment"]["KAFKA_ADVERTISED_LISTENERS"]


def test_topic_script_creates_every_documented_topic():
    script = (STREAMING_DIR / "kafka" / "create-topics.sh").read_text()
    for topic in ("usage-events", "usage-events-deduped",
                  "usage-window-aggregates", "usage-events-dlq"):
        assert topic in script, topic
    assert "cleanup.policy=compact" in script


def test_flink_sql_job_reads_the_raw_topic_and_writes_the_read_models():
    sql = (STREAMING_DIR / "flink" / "jobs" / "window_aggregates.sql").read_text()
    assert "'topic' = 'usage-events'" in sql
    assert "'topic' = 'usage-events-deduped'" in sql
    assert "'topic' = 'usage-window-aggregates'" in sql
    assert "PRIMARY KEY (event_id) NOT ENFORCED" in sql


def test_flink_image_is_built_from_the_repository():
    jobmanager = yaml.safe_load(COMPOSE_FILE.read_text())["services"]["jobmanager"]
    assert jobmanager["build"]["context"] == "./infra/streaming/flink"
    assert (STREAMING_DIR / "flink" / "Dockerfile").is_file()
