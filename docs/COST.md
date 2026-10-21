# Cost (per 1M events, us-west-2 list, POC rate)

| Component | Assumption | ≈ cost |
|---|---|---|
| API Gateway REST | 1M requests | $3.50 |
| Lambda ingest (256MB, 50ms) | 1M invocations | ~$1.10 |
| SQS (standard) | 1M sends + receives | ~$0.80 |
| Lambda processor (512MB, 100ms) | 1M | ~$4.20 |
| DynamoDB on-demand | 1M writes + 3M read units | ~$3.75 |
| S3 raw log (250B/ev → 250MB) | PUT + storage 90d | ~$0.60 |
| CloudWatch logs/metrics | JSON logs, 7d retention | ~$1.50 |
| **Total** | | **≈ $15–16 / 1M events** |

Serverless pay-per-use: $0 at idle. Optimization levers: batch ingest (fewer
API/Lambda calls), log sampling, DDB autoscaling at pilot, S3 lifecycle already on.

The local Kafka + Flink tier (the `streaming` Compose profile, assets in
`infra/streaming/`) adds no cloud cost; this table
does not include Amazon MSK or Managed Service for Apache Flink, which are a
production blueprint rather than part of the POC estimate.
