#!/usr/bin/env python3
"""
Day 12: AWS Glue 4.0 PySpark ETL Job — S3 Raw Parquet (Full Load + CDC) to Apache Iceberg (Silver Zone).

Key Engineering Capabilities:
1. Schema Evolution & Merge: Reads both Full Load (`LOAD*.parquet`) and ongoing CDC delta
   (`2026*.parquet`) files from S3 Bronze (`raw/public/legacy_transactions/`).
2. Window-Based CDC Deduplication: Resolves multiple mutations per `transaction_id` using
   `dms_commit_ts` / `updated_at` so Iceberg `MERGE INTO` never hits cardinality violations.
3. ACID Transactional Upsert (`MERGE INTO`): Handles `I` (Insert), `U` (Update), and `D` (Delete)
   operations into an Apache Iceberg v2 table partitioned by `region`.
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------------------------
# 1. Resolve Job Arguments
# ------------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATALAKE_BUCKET",
        "GLUE_DATABASE",
        "ICEBERG_TABLE",
    ],
)

datalake_bucket = args["DATALAKE_BUCKET"]
glue_database = args["GLUE_DATABASE"]
iceberg_table = args["ICEBERG_TABLE"]

raw_parquet_path = f"s3://{datalake_bucket}/raw/public/legacy_transactions/"
iceberg_warehouse_path = f"s3://{datalake_bucket}/iceberg/"
full_table_identifier = f"glue_catalog.{glue_database}.{iceberg_table}"

# ------------------------------------------------------------------------------
# 2. Initialize Spark & Glue Contexts (Configured via Job Args for Iceberg)
# ------------------------------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
logger = glue_context.get_logger()

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

logger.info(f"Starting Iceberg CDC Upsert Job for table: {full_table_identifier}")
logger.info(f"Reading raw Parquet files from: {raw_parquet_path}")

# ------------------------------------------------------------------------------
# 3. Read Raw Parquet Files (Full Load + CDC Deltas) with Schema Merging
# ------------------------------------------------------------------------------
raw_df = (
    spark.read.option("mergeSchema", "true")
    .parquet(raw_parquet_path)
)

# Normalize CDC metadata columns across Full Load (which may omit 'Op') and CDC Deltas
if "Op" not in raw_df.columns:
    raw_df = raw_df.withColumn("Op", F.lit("I"))
else:
    raw_df = raw_df.withColumn("Op", F.coalesce(F.col("Op"), F.lit("I")))

if "dms_commit_ts" not in raw_df.columns:
    raw_df = raw_df.withColumn("dms_commit_ts", F.col("updated_at").cast("timestamp"))
else:
    raw_df = raw_df.withColumn(
        "dms_commit_ts",
        F.coalesce(F.col("dms_commit_ts").cast("timestamp"), F.col("updated_at").cast("timestamp")),
    )

# Standardize data types for the Silver Lakehouse Iceberg schema
typed_df = (
    raw_df.select(
        F.col("transaction_id").cast("string").alias("transaction_id"),
        F.col("account_number").cast("string").alias("account_number"),
        F.col("customer_name").cast("string").alias("customer_name"),
        F.col("customer_ssn").cast("string").alias("customer_ssn"),
        F.col("transaction_type").cast("string").alias("transaction_type"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("currency").cast("string").alias("currency"),
        F.col("branch_code").cast("string").alias("branch_code"),
        F.col("region").cast("string").alias("region"),
        F.col("status").cast("string").alias("status"),
        F.col("cobol_batch_id").cast("string").alias("cobol_batch_id"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        F.col("dms_commit_ts").cast("timestamp").alias("dms_commit_ts"),
        F.col("Op").cast("string").alias("Op"),
    )
    .filter(F.col("transaction_id").isNotNull())
)

# ------------------------------------------------------------------------------
# 4. Deduplicate by Primary Key (`transaction_id`) Ordered by Commit Timestamp
# Ensures the latest CDC mutation wins over initial Full Load records
# ------------------------------------------------------------------------------
cdc_window = Window.partitionBy("transaction_id").orderBy(
    F.col("dms_commit_ts").desc_nulls_last(),
    F.col("updated_at").desc_nulls_last(),
)

deduped_df = (
    typed_df.withColumn("_row_num", F.row_number().over(cdc_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

deduped_df.createOrReplaceTempView("staged_cdc_updates")

# ------------------------------------------------------------------------------
# 5. Create Apache Iceberg v2 Table in Glue Data Catalog (If Not Exists)
# ------------------------------------------------------------------------------
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{glue_database}")

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {full_table_identifier} (
    transaction_id   STRING,
    account_number   STRING,
    customer_name    STRING,
    customer_ssn     STRING,
    transaction_type STRING,
    amount           DECIMAL(12, 2),
    currency         STRING,
    branch_code      STRING,
    region           STRING,
    status           STRING,
    cobol_batch_id   STRING,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    dms_commit_ts    TIMESTAMP
)
USING iceberg
PARTITIONED BY (region)
LOCATION '{iceberg_warehouse_path}{iceberg_table}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
"""
spark.sql(create_table_sql)

# ------------------------------------------------------------------------------
# 6. Execute ACID Transactional MERGE INTO (Upsert + Delete)
# ------------------------------------------------------------------------------
merge_sql = f"""
MERGE INTO {full_table_identifier} AS target
USING staged_cdc_updates AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED AND source.Op = 'D' THEN
    DELETE
WHEN MATCHED AND source.Op != 'D' THEN
    UPDATE SET
        target.account_number   = source.account_number,
        target.customer_name    = source.customer_name,
        target.customer_ssn     = source.customer_ssn,
        target.transaction_type = source.transaction_type,
        target.amount           = source.amount,
        target.currency         = source.currency,
        target.branch_code      = source.branch_code,
        target.region           = source.region,
        target.status           = source.status,
        target.cobol_batch_id   = source.cobol_batch_id,
        target.created_at       = source.created_at,
        target.updated_at       = source.updated_at,
        target.dms_commit_ts    = source.dms_commit_ts
WHEN NOT MATCHED AND source.Op != 'D' THEN
    INSERT (
        transaction_id,
        account_number,
        customer_name,
        customer_ssn,
        transaction_type,
        amount,
        currency,
        branch_code,
        region,
        status,
        cobol_batch_id,
        created_at,
        updated_at,
        dms_commit_ts
    )
    VALUES (
        source.transaction_id,
        source.account_number,
        source.customer_name,
        source.customer_ssn,
        source.transaction_type,
        source.amount,
        source.currency,
        source.branch_code,
        source.region,
        source.status,
        source.cobol_batch_id,
        source.created_at,
        source.updated_at,
        source.dms_commit_ts
    )
"""

logger.info("Executing Iceberg ACID MERGE INTO...")
spark.sql(merge_sql)

final_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_table_identifier}").collect()[0]["cnt"]
logger.info(f"Iceberg MERGE complete. Total active rows in {full_table_identifier}: {final_count}")

job.commit()
