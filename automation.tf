# ------------------------------------------------------------------------------
# Week 3 (Day 13): Event-Driven Pipeline Automation (S3 -> EventBridge -> Iceberg ETL)
# Automatically runs the Apache Iceberg ACID MERGE INTO when DMS writes new Parquet files
# ------------------------------------------------------------------------------

# 1. Enable Amazon EventBridge Notifications on the Data Lake S3 Bucket
resource "aws_s3_bucket_notification" "datalake_eventbridge" {
  bucket      = aws_s3_bucket.datalake.id
  eventbridge = true
}

# 2. EventBridge Rule Matching New Parquet Files Under raw/public/legacy_transactions/
resource "aws_cloudwatch_event_rule" "s3_raw_parquet_created" {
  name        = "${var.project_name}-s3-raw-cdc-rule-${var.environment}"
  description = "Captures S3 Object Created events in raw/public/legacy_transactions/ from AWS DMS"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.datalake.id]
      }
      object = {
        key = [
          {
            prefix = "raw/public/legacy_transactions/"
          }
        ]
      }
    }
  })

  tags = {
    Name        = "${var.project_name}-s3-raw-cdc-rule"
    Environment = var.environment
  }
}

# 3. Route S3 Object Created Events to the Serverless Iceberg Upsert Orchestrator
resource "aws_cloudwatch_event_target" "trigger_iceberg_upsert" {
  rule      = aws_cloudwatch_event_rule.s3_raw_parquet_created.name
  target_id = "TriggerIcebergUpsertOrchestrator"
  arn       = aws_lambda_function.iceberg_upsert_orchestrator.arn
}

resource "aws_lambda_permission" "allow_eventbridge_iceberg_upsert" {
  statement_id  = "AllowExecutionFromEventBridgeS3Raw"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.iceberg_upsert_orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.s3_raw_parquet_created.arn
}
