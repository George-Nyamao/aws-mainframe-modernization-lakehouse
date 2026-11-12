# ------------------------------------------------------------------------------
# Week 3 (Days 11–12): Apache Iceberg Catalog, Athena Workgroup & Upsert Engine
# ------------------------------------------------------------------------------

locals {
  glue_database_name = "${replace(var.project_name, "-", "_")}_lakehouse_${var.environment}"
  iceberg_table_name = "legacy_transactions_iceberg"
}

# ------------------------------------------------------------------------------
# 1. Day 11: AWS Glue Data Catalog Database & Athena Serverless Workgroup
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "lakehouse" {
  name         = local.glue_database_name
  description  = "Apache Iceberg Silver/Gold Lakehouse database for modernized mainframe transactions"
  location_uri = "s3://${aws_s3_bucket.datalake.id}/iceberg/"

  tags = {
    Name        = "${var.project_name}-glue-db"
    Environment = var.environment
    Layer       = "LakehouseCatalog"
  }
}

resource "aws_athena_workgroup" "lakehouse" {
  name          = "${var.project_name}-athena-wg-${var.environment}"
  description   = "Serverless SQL analytics workgroup for querying and merging Apache Iceberg v2 tables"
  state         = "ENABLED"
  force_destroy = var.environment != "prod"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Name        = "${var.project_name}-athena-wg"
    Environment = var.environment
    Layer       = "Analytics"
  }
}

# ------------------------------------------------------------------------------
# 2. Day 12A: Upload PySpark Script to S3 & Optional Dedicated Glue Spark Job
# ------------------------------------------------------------------------------
resource "aws_s3_object" "glue_iceberg_script" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/glue_iceberg_upsert.py"
  source = "${path.module}/scripts/glue_iceberg_upsert.py"
  etag   = filemd5("${path.module}/scripts/glue_iceberg_upsert.py")

  tags = {
    Name        = "${var.project_name}-glue-iceberg-script"
    Environment = var.environment
  }
}

resource "aws_glue_job" "iceberg_upsert" {
  count       = var.enable_glue_spark_job ? 1 : 0
  name        = "${var.project_name}-iceberg-upsert-${var.environment}"
  description = "PySpark 4.0 ETL job that merges raw DMS Parquet (Full Load + CDC) into Apache Iceberg v2"
  role_arn    = aws_iam_role.glue_etl.arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  timeout     = 15
  max_retries = 1

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.datalake.id}/${aws_s3_object.glue_iceberg_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    "--TempDir"                          = "s3://${aws_s3_bucket.datalake.id}/temp/"
    "--DATALAKE_BUCKET"                  = aws_s3_bucket.datalake.id
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.lakehouse.name
    "--ICEBERG_TABLE"                    = local.iceberg_table_name
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.datalake.id}/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  }

  tags = {
    Name        = "${var.project_name}-iceberg-upsert-job"
    Environment = var.environment
    Layer       = "LakehouseETL"
  }
}

# ------------------------------------------------------------------------------
# 3. Day 12B: Serverless Apache Iceberg ACID MERGE Orchestrator (Athena Engine v3)
# Executes Window-deduplicated MERGE INTO on Glue Catalog Iceberg v2 tables at $0 DPU cost
# ------------------------------------------------------------------------------
data "aws_iam_policy_document" "iceberg_orchestrator_policy" {
  statement {
    sid    = "AllowAthenaAndGlueCatalogIceberg"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "AllowS3DataLakeReadWriteForIcebergAndAthena"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload"
    ]
    resources = [
      aws_s3_bucket.datalake.arn,
      "${aws_s3_bucket.datalake.arn}/*"
    ]
  }
}

resource "aws_iam_role" "iceberg_orchestrator" {
  name               = "${var.project_name}-iceberg-orchestrator-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.lambda_seeder_assume_role.json

  tags = {
    Name        = "${var.project_name}-iceberg-orchestrator-role"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "iceberg_orchestrator_logs" {
  role       = aws_iam_role.iceberg_orchestrator.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "iceberg_orchestrator" {
  name   = "${var.project_name}-iceberg-orchestrator-policy"
  role   = aws_iam_role.iceberg_orchestrator.id
  policy = data.aws_iam_policy_document.iceberg_orchestrator_policy.json
}

resource "aws_lambda_function" "iceberg_upsert_orchestrator" {
  function_name    = "${var.project_name}-iceberg-upsert-${var.environment}"
  description      = "Serverless ETL orchestrator that deduplicates and merges raw S3 CDC Parquet into Apache Iceberg v2"
  role             = aws_iam_role.iceberg_orchestrator.arn
  handler          = "iceberg_upsert_orchestrator.lambda_handler"
  runtime          = "python3.12"
  filename         = "${path.module}/scripts/iceberg_upsert_orchestrator.zip"
  source_code_hash = filebase64sha256("${path.module}/scripts/iceberg_upsert_orchestrator.zip")

  timeout     = 180
  memory_size = 256

  environment {
    variables = {
      DATALAKE_BUCKET  = aws_s3_bucket.datalake.id
      GLUE_DATABASE    = aws_glue_catalog_database.lakehouse.name
      ICEBERG_TABLE    = local.iceberg_table_name
      BRONZE_TABLE     = "legacy_transactions_raw"
      ATHENA_WORKGROUP = aws_athena_workgroup.lakehouse.name
    }
  }

  depends_on = [
    aws_iam_role_policy.iceberg_orchestrator,
    aws_glue_catalog_database.lakehouse,
    aws_athena_workgroup.lakehouse
  ]

  tags = {
    Name        = "${var.project_name}-iceberg-upsert-orchestrator"
    Environment = var.environment
    Layer       = "LakehouseETL"
  }
}
