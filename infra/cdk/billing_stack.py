"""Billing platform stack — IaC for everything (PRD §7).

Producers → API GW → Ingest Lambda → SQS (+DLQ) → Processor Lambda
  ├─ raw log → S3 (immutable, partitioned dt=YYYY-MM-DD)
  ├─ dedupe/aggregates → DynamoDB (PAY_PER_REQUEST,ondemand; point-in-time recovery)
  ├─ Query Lambda + Period-close Lambda (scheduled/manual)
  └─ CloudWatch: dashboard (ingest rate, queue age, p50/95/99, dup rate, DLQ),
     alarms (DLQ>0 page, queue age>30s, error rate>1%).

Local prototype maps 1:1: inbox table=SQS, dlq table=DLQ, raw-log dir=S3,
events/aggregates tables=DynamoDB. Same handler code runs in Lambda (see
src/billing_engine/{ingest,processor,query}.py — Lambda adapters in handlers/).
"""
from aws_cdk import (Duration, RemovalPolicy, Stack,
                     aws_apigateway as apigw,
                     aws_cloudwatch as cw,
                     aws_dynamodb as ddb,
                     aws_lambda as lam,
                     aws_lambda_event_sources as src,
                     aws_s3 as s3,
                     aws_sqs as sqs)
from constructs import Construct


class BillingStack(Stack):
    def __init__(self, scope: Construct, cid: str, stage: str = "dev", **kw) -> None:
        super().__init__(scope, cid, **kw)
        p = f"billing-{stage}"

        dlq = sqs.Queue(self, "Dlq", queue_name=f"{p}-dlq", retention_period=Duration.days(14))
        queue = sqs.Queue(self, "UsageQueue", queue_name=f"{p}-usage",
                          visibility_timeout=Duration.seconds(90),
                          dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5,
                                                                queue=dlq))

        raw = s3.Bucket(self, "RawLog", bucket_name=f"{p}-raw-log-{self.account}",
                        versioned=True, event_bridge_enabled=False,
                        removal_policy=RemovalPolicy.RETAIN,
                        lifecycle_rules=[s3.LifecycleRule(transitions=[
                            s3.Transition(storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                                          transition_after=Duration.days(30))])])

        events = ddb.Table(self, "Events", table_name=f"{p}-events",
                           partition_key=ddb.Attribute(name="event_id", type=ddb.AttributeType.STRING),
                           billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
                           point_in_time_recovery_specification=ddb.PointInTimeRecoverySpecification(
                               point_in_time_recovery_enabled=True),
                           removal_policy=RemovalPolicy.RETAIN)
        events.add_global_secondary_index(
            index_name="by-customer-period",
            partition_key=ddb.Attribute(name="customer_id", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="period", type=ddb.AttributeType.STRING),
            projection_type=ddb.ProjectionType.ALL)

        aggs = ddb.Table(self, "Aggregates", table_name=f"{p}-aggregates",
                         partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
                         billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
                         removal_policy=RemovalPolicy.RETAIN)
        # pk = customer#period#metric — avoids hot partition per customer in prod via
        # shard suffix customer#period#metric#shard (see ARCHITECTURE §hot partitions).

        code = lam.Code.from_asset("../../src")
        env = {"STAGE": stage, "RAW_BUCKET": raw.bucket_name,
               "EVENTS_TABLE": events.table_name, "AGGS_TABLE": aggs.table_name,
               "QUEUE_URL": queue.queue_url}
        ingest = lam.Function(self, "Ingest", function_name=f"{p}-ingest",
                              runtime=lam.Runtime.PYTHON_3_12, handler="handlers.ingest.handler",
                              code=code, environment=env, timeout=Duration.seconds(10),
                              memory_size=256, reserved_concurrent_executions=100)
        processor = lam.Function(self, "Processor", function_name=f"{p}-processor",
                                 runtime=lam.Runtime.PYTHON_3_12,
                                 handler="handlers.processor.handler",
                                 code=code, environment=env, timeout=Duration.seconds(60),
                                 memory_size=512, reserved_concurrent_executions=50)
        processor.add_event_source(src.SqsEventSource(queue, batch_size=50,
                                                      max_batching_window=Duration.seconds(1)))
        query = lam.Function(self, "Query", function_name=f"{p}-query",
                             runtime=lam.Runtime.PYTHON_3_12, handler="handlers.query.handler",
                             code=code, environment=env, timeout=Duration.seconds(10),
                             memory_size=256)

        api = apigw.RestApi(self, "Api", rest_api_name=f"{p}-api",
                            default_cors_preflight_options=apigw.CorsOptions(
                                allow_origins=apigw.Cors.ALL_ORIGINS,
                                allow_methods=apigw.Cors.ALL_METHODS))
        v1 = api.root.add_resource("v1")
        usage = v1.add_resource("usage-events")
        usage.add_method("POST", apigw.LambdaIntegration(ingest),
                         api_key_required=True)
        usage.add_resource("batch").add_method("POST", apigw.LambdaIntegration(ingest),
                                               api_key_required=True)
        plan = api.add_usage_plan("UsagePlan", throttle=apigw.ThrottleSettings(rate_limit=2000,
                                                                               burst_limit=5000))
        key = api.add_api_key("ProdKey")
        plan.add_api_key(key)
        cust = v1.add_resource("customers").add_resource("{id}")
        cust.add_resource("usage").add_method("GET", apigw.LambdaIntegration(query),
                                              api_key_required=True)
        cust.add_resource("bill").add_method("GET", apigw.LambdaIntegration(query),
                                             api_key_required=True)

        queue.grant_send_messages(ingest)
        queue.grant_consume_messages(processor)
        raw.grant_write(ingest)
        events.grant_write_data(processor)
        aggs.grant_write_data(processor)
        events.grant_read_data(query)
        aggs.grant_read_data(query)

        # ---- alarms (FR6) ----
        cw.Alarm(self, "DlqAlarm", alarm_name=f"{p}-dlq-depth",
                 metric=dlq.metric_approximate_number_of_messages_visible(),
                 threshold=1, evaluation_periods=1,
                 comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                 treat_missing_data=cw.TreatMissingData.NOT_BREACHING)
        cw.Alarm(self, "QueueAgeAlarm", alarm_name=f"{p}-queue-age",
                 metric=queue.metric_approximate_age_of_oldest_message(),
                 threshold=30, evaluation_periods=2,
                 comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD)
        cw.Alarm(self, "ProcErrAlarm", alarm_name=f"{p}-processor-errors",
                 metric=processor.metric_errors(), threshold=10, evaluation_periods=1,
                 comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD)
