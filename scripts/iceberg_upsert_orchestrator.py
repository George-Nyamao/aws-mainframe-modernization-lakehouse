#!/usr/bin/env python3
"""
Day 12 & Day 13: Serverless Apache Iceberg CDC Upsert Orchestrator.

Triggered automatically by Amazon EventBridge when AWS DMS lands Parquet files in
`s3://<datalake-bucket>/raw/public/legacy_transactions/` (or invoked manually via CLI).

Executes 3 ACID Lakehouse steps via Amazon Athena Engine v3 & AWS Glue Data Catalog:
1. Ensures the Bronze External Table (`legacy_transactions_raw`) points to S3 `raw/`
   (handling DMS's UTF-8 STRING representation of `dms_commit_ts`).
2. Ensures the Silver Apache Iceberg v2 Table (`legacy_transactions_iceberg`) exists
   and is partitioned by `region`.
3. Executes a Window-deduplicated ACID `MERGE INTO` (`INSERT`, `UPDATE`, `DELETE`)
   from Bronze Parquet into Silver Apache Iceberg v2.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any
import boto3


def run_athena_query(
    athena_client: Any,
    sql: str,
    database: str,
    workgroup: str,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Starts an Athena SQL statement and waits for completion."""
    resp = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_execution_id = resp["QueryExecutionId"]

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status_resp = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        execution = status_resp["QueryExecution"]
        state = execution["Status"]["State"]

        if state == "SUCCEEDED":
            return execution
        if state in ("FAILED", "CANCELLED"):
            reason = execution["Status"].get("StateChangeReason", "Unknown Athena error")
            raise RuntimeError(f"Athena query {query_execution_id} {state}: {reason}\nSQL:\n{sql}")

        time.sleep(1.5)

    raise TimeoutError(f"Athena query {query_execution_id} timed out after {timeout_seconds}s")


def fetch_scalar_result(athena_client: Any, query_execution_id: str) -> str:
    """Fetches the first cell from the first data row of an Athena SELECT query."""
    res = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    rows = res.get("ResultSet", {}).get("Rows", [])
    if len(rows) > 1:
        return rows[1]["Data"][0].get("VarCharValue", "0")
    return "0"


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    start_time = time.time()
    datalake_bucket = os.environ["DATALAKE_BUCKET"]
    glue_database = os.environ["GLUE_DATABASE"]
    iceberg_table = os.environ.get("ICEBERG_TABLE", "legacy_transactions_iceberg")
    bronze_table = os.environ.get("BRONZE_TABLE", "legacy_transactions_raw")
    athena_workgroup = os.environ["ATHENA_WORKGROUP"]

    athena = boto3.client("athena")

    # 1. Refresh Bronze External Table metadata over AWS DMS S3 Raw Parquet files
    # Note: AWS DMS writes `dms_commit_ts` as a UTF-8 STRING (BINARY in Parquet)
    run_athena_query(
        athena,
        f"DROP TABLE IF EXISTS {glue_database}.{bronze_table}",
        glue_database,
        athena_workgroup,
    )

    create_bronze_sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {glue_database}.{bronze_table} (
        Op               STRING,
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
        dms_commit_ts    STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{datalake_bucket}/raw/public/legacy_transactions/'
    """
    run_athena_query(athena, create_bronze_sql, glue_database, athena_workgroup)

    # 2. Create Silver Apache Iceberg v2 Table partitioned by `region`
    create_iceberg_sql = f"""
    CREATE TABLE IF NOT EXISTS {glue_database}.{iceberg_table} (
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
    PARTITIONED BY (region)
    LOCATION 's3://{datalake_bucket}/iceberg/{iceberg_table}/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet',
        'write_compression' = 'snappy'
    )
    """
    run_athena_query(athena, create_iceberg_sql, glue_database, athena_workgroup)

    # 3. Execute Window-Deduplicated ACID MERGE INTO on the Apache Iceberg v2 Table
    merge_sql = f"""
    MERGE INTO {glue_database}.{iceberg_table} AS target
    USING (
        SELECT
            transaction_id,
            account_number,
            customer_name,
            customer_ssn,
            transaction_type,
            CAST(amount AS DECIMAL(12, 2)) AS amount,
            currency,
            branch_code,
            region,
            status,
            cobol_batch_id,
            CAST(created_at AS TIMESTAMP(6)) AS created_at,
            CAST(updated_at AS TIMESTAMP(6)) AS updated_at,
            COALESCE(TRY(CAST(dms_commit_ts AS TIMESTAMP(6))), CAST(updated_at AS TIMESTAMP(6))) AS dms_commit_ts,
            COALESCE(Op, 'I') AS Op
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY transaction_id
                    ORDER BY COALESCE(TRY(CAST(dms_commit_ts AS TIMESTAMP(6))), CAST(updated_at AS TIMESTAMP(6))) DESC,
                             updated_at DESC
                ) AS _rn
            FROM {glue_database}.{bronze_table}
            WHERE transaction_id IS NOT NULL
        )
        WHERE _rn = 1
    ) AS source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED AND source.Op = 'D' THEN
        DELETE
    WHEN MATCHED AND source.Op <> 'D' THEN
        UPDATE SET
            account_number   = source.account_number,
            customer_name    = source.customer_name,
            customer_ssn     = source.customer_ssn,
            transaction_type = source.transaction_type,
            amount           = source.amount,
            currency         = source.currency,
            branch_code      = source.branch_code,
            region           = source.region,
            status           = source.status,
            cobol_batch_id   = source.cobol_batch_id,
            created_at       = source.created_at,
            updated_at       = source.updated_at,
            dms_commit_ts    = source.dms_commit_ts
    WHEN NOT MATCHED AND source.Op <> 'D' THEN
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
    merge_exec = run_athena_query(athena, merge_sql, glue_database, athena_workgroup)

    # 4. Verify row count & status breakdown in the Apache Iceberg table
    count_exec = run_athena_query(
        athena,
        f"SELECT COUNT(*) FROM {glue_database}.{iceberg_table}",
        glue_database,
        athena_workgroup,
    )
    total_iceberg_rows = int(fetch_scalar_result(athena, count_exec["QueryExecutionId"]))

    settled_exec = run_athena_query(
        athena,
        f"SELECT COUNT(*) FROM {glue_database}.{iceberg_table} WHERE status = 'SETTLED'",
        glue_database,
        athena_workgroup,
    )
    settled_rows = int(fetch_scalar_result(athena, settled_exec["QueryExecutionId"]))

    elapsed = round(time.time() - start_time, 2)
    summary = {
        "status": "SUCCESS",
        "glue_database": glue_database,
        "iceberg_table": iceberg_table,
        "merge_query_execution_id": merge_exec["QueryExecutionId"],
        "total_iceberg_rows": total_iceberg_rows,
        "settled_rows": settled_rows,
        "elapsed_seconds": elapsed,
    }
    return {
        "statusCode": 200,
        "body": json.dumps(summary, indent=2),
    }
