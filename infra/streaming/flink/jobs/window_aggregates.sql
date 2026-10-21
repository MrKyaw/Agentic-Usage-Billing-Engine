-- Flink SQL streaming job for the billing engine.
--
-- Topology:
--   usage-events (raw, 20 partitions, key=customer_id)
--     -> dedupe by event_id -> usage-events-deduped (upsert-kafka, compacted)
--     -> 10-second processing-time operational aggregates -> usage-window-aggregates
--
-- The billing worker consumes usage-events and relies on the ledger's
-- conditional insert for authoritative dedupe. The Flink deduped topic is a
-- compacted analytics read model: it is keyed by event_id and may contain
-- changelog updates for late duplicates, so consumers must treat it as an
-- upsert stream (latest value wins), not as an append-only audit log.
--
-- State TTL bounds the dedupe/aggregation state (24h). Exactly-once delivery is
-- not claimed for the Kafka sinks; the Python ledger is idempotent.

SET 'execution.target' = 'remote';
SET 'execution.attached' = 'false';
SET 'parallelism.default' = '2';
SET 'table.exec.state.ttl' = '1 d';

CREATE TABLE usage_events (
  event_id STRING,
  customer_id STRING,
  product_id STRING,
  metric STRING,
  quantity DOUBLE,
  `timestamp` STRING,
  event_time AS TO_TIMESTAMP(SUBSTRING(REPLACE(`timestamp`, 'T', ' '), 1, 19)),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'usage-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-dedupe',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Separate consumer group: two Flink jobs reading the same topic with one group
-- would split partitions instead of each seeing every event.
CREATE TABLE usage_events_agg (
  event_id STRING,
  customer_id STRING,
  product_id STRING,
  metric STRING,
  quantity DOUBLE,
  `timestamp` STRING,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'usage-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-aggregate',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE usage_events_deduped (
  event_id STRING,
  customer_id STRING,
  product_id STRING,
  metric STRING,
  quantity DOUBLE,
  `timestamp` STRING,
  event_time TIMESTAMP(3),
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'usage-events-deduped',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE usage_window_aggregates (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  customer_id STRING,
  metric STRING,
  event_count BIGINT,
  total_quantity DOUBLE,
  max_quantity DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'usage-window-aggregates',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO usage_events_deduped
SELECT event_id, customer_id, product_id, metric, quantity, `timestamp`, event_time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time ASC) AS rn
  FROM usage_events
)
WHERE rn = 1;

INSERT INTO usage_window_aggregates
SELECT
  TUMBLE_START(proc_time, INTERVAL '10' SECOND) AS window_start,
  TUMBLE_END(proc_time, INTERVAL '10' SECOND) AS window_end,
  customer_id,
  metric,
  COUNT(*) AS event_count,
  SUM(quantity) AS total_quantity,
  MAX(quantity) AS max_quantity
FROM usage_events_agg
GROUP BY TUMBLE(proc_time, INTERVAL '10' SECOND), customer_id, metric;
