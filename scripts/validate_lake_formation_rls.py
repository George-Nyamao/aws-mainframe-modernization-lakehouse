#!/usr/bin/env python3
"""
Day 15: Zero-Dependency Cross-Role Athena Validator for AWS Lake Formation RLS & PII Masking.

Uses the local AWS CLI via Python's standard library (`subprocess` + `json`) so no external
pip packages (`boto3`) are required in WSL.

Compares Athena query results on `legacy_transactions_iceberg` between:
1. Data Lake Admin Identity (sees all 4 regions = 10,010 rows + `customer_ssn` PII column)
2. Regional Analyst Role (`us-east-analyst`) governed by Lake Formation Data Cells Filter
   (sees ONLY `region = 'US-EAST'` rows and has `customer_ssn` automatically stripped from `SELECT *`).
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from typing import Any


def aws_cli_json(args: list[str], env_overrides: dict[str, str] | None = None) -> dict[str, Any]:
    """Executes an AWS CLI command and parses its JSON output using only the Python stdlib."""
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)

    cmd = ["aws", *args, "--output", "json"]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"Command failed: {' '.join(cmd)}")
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def execute_athena_and_fetch(
    sql: str,
    database: str,
    workgroup: str,
    region: str = "us-east-1",
    env_overrides: dict[str, str] | None = None,
) -> tuple[list[str], list[list[str]]]:
    """Starts an Athena SQL query via AWS CLI and returns (column_names, data_rows)."""
    start_resp = aws_cli_json(
        [
            "athena",
            "start-query-execution",
            "--query-string",
            sql,
            "--query-execution-context",
            f"Database={database}",
            "--work-group",
            workgroup,
            "--region",
            region,
        ],
        env_overrides=env_overrides,
    )
    qid = start_resp["QueryExecutionId"]

    while True:
        exec_resp = aws_cli_json(
            ["athena", "get-query-execution", "--query-execution-id", qid, "--region", region],
            env_overrides=env_overrides,
        )
        status = exec_resp["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", "Query failed"))
        time.sleep(1.0)

    res = aws_cli_json(
        ["athena", "get-query-results", "--query-execution-id", qid, "--region", region],
        env_overrides=env_overrides,
    )["ResultSet"]
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

    identity = aws_cli_json(["sts", "get-caller-identity", "--region", region])
    account_id = identity["Account"]
    analyst_role_arn = f"arn:aws:iam::{account_id}:role/mainframe-modernization-us-east-analyst-dev"

    # 1. Query as Lakehouse Admin
    _, admin_regions = execute_athena_and_fetch(
        f"SELECT region, COUNT(*) AS cnt FROM {database}.{table} GROUP BY region ORDER BY region",
        database,
        workgroup,
        region,
    )
    admin_cols, _ = execute_athena_and_fetch(
        f"SELECT * FROM {database}.{table} LIMIT 1",
        database,
        workgroup,
        region,
    )

    # 2. Assume the Regional Analyst Role & Query via Lake Formation Data Cells Filter
    assumed = aws_cli_json(
        [
            "sts",
            "assume-role",
            "--role-arn",
            analyst_role_arn,
            "--role-session-name",
            "LakeFormationRLSValidation",
            "--region",
            region,
        ]
    )["Credentials"]

    analyst_env = {
        "AWS_ACCESS_KEY_ID": assumed["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": assumed["SecretAccessKey"],
        "AWS_SESSION_TOKEN": assumed["SessionToken"],
    }

    _, analyst_regions = execute_athena_and_fetch(
        f"SELECT region, COUNT(*) AS cnt FROM {database}.{table} GROUP BY region ORDER BY region",
        database,
        workgroup,
        region,
        env_overrides=analyst_env,
    )
    analyst_cols, _ = execute_athena_and_fetch(
        f"SELECT * FROM {database}.{table} LIMIT 1",
        database,
        workgroup,
        region,
        env_overrides=analyst_env,
    )

    # 3. Verify explicit PII column query (`SELECT customer_ssn`) is blocked for Analyst
    pii_blocked = False
    pii_error_message = ""
    try:
        execute_athena_and_fetch(
            f"SELECT customer_ssn FROM {database}.{table} LIMIT 1",
            database,
            workgroup,
            region,
            env_overrides=analyst_env,
        )
    except Exception as exc:
        pii_blocked = True
        pii_error_message = str(exc)

    report = {
        "status": "PASSED"
        if (len(analyst_regions) == 1 and "customer_ssn" not in analyst_cols and pii_blocked)
        else "CHECK",
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
