#!/usr/bin/env python3
"""
Day 15: Cross-Role Athena Validation for AWS Lake Formation Row-Level Security (RLS) & PII Masking.

Compares Athena query results on `legacy_transactions_iceberg` between:
1. Data Lake Admin Identity (sees all 4 regions = 10,010 rows + `customer_ssn` PII column)
2. Regional Analyst Role (`us-east-analyst`) governed by Lake Formation Data Cells Filter
   (sees ONLY `region = 'US-EAST'` rows and has `customer_ssn` automatically stripped from `SELECT *`).
"""

from __future__ import annotations

import json
import time
from typing import Any
import boto3


def execute_athena_and_fetch(
    athena_client: Any,
    sql: str,
    database: str,
    workgroup: str,
) -> tuple[list[str], list[list[str]]]:
    resp = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", "Query failed"))
        time.sleep(1.0)

    res = athena_client.get_query_results(QueryExecutionId=qid)["ResultSet"]
    rows = res.get("Rows", [])
    if not rows:
        return [], []
    headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
    data = [[col.get("VarCharValue", "") for col in r["Data"]] for r in rows[1:]]
    return headers, data


def main() -> None:
    region = "us-east-1"
    database = "mainframe_modernization_lakehouse_dev"
    table = "legacy_transactions_iceberg"
    workgroup = "mainframe-modernization-athena-wg-dev"

    sts = boto3.client("sts", region_name=region)
    account_id = sts.get_caller_identity()["Account"]
    analyst_role_arn = f"arn:aws:iam::{account_id}:role/mainframe-modernization-us-east-analyst-dev"

    # 1. Query as Lakehouse Admin
    admin_athena = boto3.client("athena", region_name=region)
    _, admin_regions = execute_athena_and_fetch(
        admin_athena,
        f"SELECT region, COUNT(*) AS cnt FROM {database}.{table} GROUP BY region ORDER BY region",
        database,
        workgroup,
    )
    admin_cols, _ = execute_athena_and_fetch(
        admin_athena,
        f"SELECT * FROM {database}.{table} LIMIT 1",
        database,
        workgroup,
    )

    # 2. Assume the Regional Analyst Role & Query via Lake Formation Data Cells Filter
    assumed = sts.assume_role(
        RoleArn=analyst_role_arn,
        RoleSessionName="LakeFormationRLSValidation",
    )["Credentials"]

    analyst_athena = boto3.client(
        "athena",
        region_name=region,
        aws_access_key_id=assumed["AccessKeyId"],
        aws_secret_access_key=assumed["SecretAccessKey"],
        aws_session_token=assumed["SessionToken"],
    )

    _, analyst_regions = execute_athena_and_fetch(
        analyst_athena,
        f"SELECT region, COUNT(*) AS cnt FROM {database}.{table} GROUP BY region ORDER BY region",
        database,
        workgroup,
    )
    analyst_cols, _ = execute_athena_and_fetch(
        analyst_athena,
        f"SELECT * FROM {database}.{table} LIMIT 1",
        database,
        workgroup,
    )

    # 3. Verify explicit PII column query is blocked for Analyst
    pii_blocked = False
    pii_error_message = ""
    try:
        execute_athena_and_fetch(
            analyst_athena,
            f"SELECT customer_ssn FROM {database}.{table} LIMIT 1",
            database,
            workgroup,
        )
    except Exception as exc:
        pii_blocked = True
        pii_error_message = str(exc)

    report = {
        "status": "PASSED" if (len(analyst_regions) == 1 and "customer_ssn" not in analyst_cols and pii_blocked) else "CHECK",
        "admin_view": {
            "visible_regions": {r[0]: int(r[1]) for r in admin_regions},
            "total_rows": sum(int(r[1]) for r in admin_regions),
            "customer_ssn_visible": "customer_ssn" in admin_cols,
            "visible_column_count": len(admin_cols),
        },
        "us_east_analyst_view": {
            "visible_regions": {r[0]: int(r[1]) for r in analyst_regions},
            "total_rows": sum(int(r[1]) for r in analyst_regions),
            "customer_ssn_visible": "customer_ssn" in analyst_cols,
            "visible_column_count": len(analyst_cols),
            "explicit_ssn_query_blocked": pii_blocked,
            "block_reason": pii_error_message,
        },
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
