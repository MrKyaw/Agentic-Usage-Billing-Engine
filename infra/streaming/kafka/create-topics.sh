#!/usr/bin/env bash
# Create the streaming topics. 20 partitions mirrors the 20 -> 40 scaling plan
# in docs/STREAMING.md; keying by customer_id keeps per-customer order.
set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

for topic in usage-events usage-events-deduped usage-window-aggregates usage-events-dlq; do
  echo "creating topic $topic"
  if [ "$topic" = "usage-events-deduped" ]; then
    # Upsert-kafka writes a keyed changelog; compaction keeps the latest value
    # per event_id for downstream readers.
    "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" \
      --create --if-not-exists --topic "$topic" \
      --partitions 20 --replication-factor 1 \
      --config cleanup.policy=compact
  else
    "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" \
      --create --if-not-exists --topic "$topic" \
      --partitions 20 --replication-factor 1
  fi
done

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
