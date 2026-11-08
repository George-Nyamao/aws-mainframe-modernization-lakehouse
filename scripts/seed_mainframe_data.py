#!/usr/bin/env python3
"""
Day 5 & Day 10: Legacy Mainframe RDS Data Seeder & CDC Simulator.

Supports two execution modes:
1. Serverless VPC Execution (AWS Lambda inside Private Subnets - Zero NAT Cost):
   Uses a built-in, zero-dependency PostgreSQL v3 wire-protocol client (TLS +
   SCRAM-SHA-256) or psycopg2 if installed.
2. Local / Docker Container Execution:
   `python scripts/seed_mainframe_data.py --action seed --rows 10000`
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import hmac
import json
import os
import random
import socket
import ssl
import struct
import time
from typing import Any

# Optional psycopg2 for local/Docker usage; falls back to stdlib PgWireClient in Lambda
try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None


# ==============================================================================
# Zero-Dependency PostgreSQL v3 Wire Protocol Client (TLS + SCRAM-SHA-256 / MD5)
# Allows AWS Lambda to seed Private Subnet RDS PostgreSQL 16 with zero external wheels
# ==============================================================================
class PgWireClient:
    """Minimal PostgreSQL v3 protocol client supporting SSL, SCRAM-SHA-256, and Simple Query."""

    def __init__(self, host: str, port: int, user: str, password: str, database: str, timeout: int = 30) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sock = self._connect(timeout)
        self._startup_and_authenticate()

    def _connect(self, timeout: int) -> socket.socket | ssl.SSLSocket:
        raw_sock = socket.create_connection((self.host, self.port), timeout=timeout)
        # Send SSLRequest (length=8, code=80877103)
        raw_sock.sendall(struct.pack("!II", 8, 80877103))
        resp = raw_sock.recv(1)
        if resp == b"S":
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx.wrap_socket(raw_sock, server_hostname=self.host)
        return raw_sock

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Unexpected EOF from PostgreSQL server")
            buf.extend(chunk)
        return bytes(buf)

    def _read_message(self) -> tuple[str, bytes]:
        header = self._recv_exact(5)
        msg_type = chr(header[0])
        length = struct.unpack("!I", header[1:5])[0]
        payload = self._recv_exact(length - 4) if length > 4 else b""
        return msg_type, payload

    def _send_message(self, msg_type: bytes, payload: bytes) -> None:
        length = len(payload) + 4
        self.sock.sendall(msg_type + struct.pack("!I", length) + payload)

    def _startup_and_authenticate(self) -> None:
        params = (
            b"user\x00" + self.user.encode("utf-8") + b"\x00"
            b"database\x00" + self.database.encode("utf-8") + b"\x00"
            b"application_name\x00mainframe_seeder\x00\x00"
        )
        startup_pkt = struct.pack("!II", len(params) + 8, 196608) + params
        self.sock.sendall(startup_pkt)

        while True:
            msg_type, payload = self._read_message()
            if msg_type == "R":
                auth_code = struct.unpack("!I", payload[:4])[0]
                if auth_code == 0:
                    continue
                elif auth_code == 3:
                    # Cleartext password
                    self._send_message(b"p", self.password.encode("utf-8") + b"\x00")
                elif auth_code == 5:
                    # MD5 password
                    salt = payload[4:8]
                    inner = hashlib.md5((self.password + self.user).encode("utf-8")).hexdigest().encode("ascii")
                    digest = "md5" + hashlib.md5(inner + salt).hexdigest()
                    self._send_message(b"p", digest.encode("ascii") + b"\x00")
                elif auth_code == 10:
                    # SASL (SCRAM-SHA-256)
                    self._scram_sha256_auth()
                else:
                    raise RuntimeError(f"Unsupported PostgreSQL auth code: {auth_code}")
            elif msg_type == "Z":
                break
            elif msg_type == "E":
                raise RuntimeError(f"PostgreSQL startup error: {self._parse_error(payload)}")

    def _scram_sha256_auth(self) -> None:
        client_nonce = base64.b64encode(os.urandom(18)).decode("ascii")
        client_first_bare = f"n=,r={client_nonce}"
        gs2_header = "n,,"
        client_first_msg = (gs2_header + client_first_bare).encode("utf-8")

        sasl_init = b"SCRAM-SHA-256\x00" + struct.pack("!I", len(client_first_msg)) + client_first_msg
        self._send_message(b"p", sasl_init)

        msg_type, payload = self._read_message()
        if msg_type == "E":
            raise RuntimeError(f"SCRAM init error: {self._parse_error(payload)}")
        auth_code = struct.unpack("!I", payload[:4])[0]
        if auth_code != 11:
            raise RuntimeError(f"Expected SASLContinue (11), got {auth_code}")

        server_first_str = payload[4:].decode("utf-8")
        parts = dict(item.split("=", 1) for item in server_first_str.split(","))
        server_nonce = parts["r"]
        salt = base64.b64decode(parts["s"])
        iterations = int(parts["i"])

        salted_password = hashlib.pbkdf2_hmac("sha256", self.password.encode("utf-8"), salt, iterations)
        client_key = hmac.new(salted_password, b"Client Key", hashlib.sha256).digest()
        stored_key = hashlib.sha256(client_key).digest()

        client_final_without_proof = f"c={base64.b64encode(gs2_header.encode()).decode()},r={server_nonce}"
        auth_message = f"{client_first_bare},{server_first_str},{client_final_without_proof}".encode("utf-8")

        client_signature = hmac.new(stored_key, auth_message, hashlib.sha256).digest()
        client_proof = bytes(a ^ b for a, b in zip(client_key, client_signature))
        proof_b64 = base64.b64encode(client_proof).decode("ascii")
        client_final_msg = f"{client_final_without_proof},p={proof_b64}".encode("utf-8")

        self._send_message(b"p", client_final_msg)

        msg_type, payload = self._read_message()
        if msg_type == "E":
            raise RuntimeError(f"SCRAM final error: {self._parse_error(payload)}")

    @staticmethod
    def _parse_error(payload: bytes) -> str:
        fields = payload.split(b"\x00")
        return " | ".join(f[1:].decode("utf-8", errors="replace") for f in fields if len(f) > 1)

    def execute(self, sql: str) -> list[tuple[str, ...]]:
        self._send_message(b"Q", sql.encode("utf-8") + b"\x00")
        rows: list[tuple[str, ...]] = []
        error_msg: str | None = None

        while True:
            msg_type, payload = self._read_message()
            if msg_type == "D":
                num_cols = struct.unpack("!H", payload[:2])[0]
                offset = 2
                row: list[str] = []
                for _ in range(num_cols):
                    col_len = struct.unpack("!i", payload[offset : offset + 4])[0]
                    offset += 4
                    if col_len == -1:
                        row.append("")
                    else:
                        row.append(payload[offset : offset + col_len].decode("utf-8", errors="replace"))
                        offset += col_len
                rows.append(tuple(row))
            elif msg_type == "E":
                error_msg = self._parse_error(payload)
            elif msg_type == "Z":
                break

        if error_msg:
            raise RuntimeError(f"SQL Error: {error_msg}")
        return rows

    def close(self) -> None:
        try:
            self._send_message(b"X", b"")
        except Exception:
            pass
        finally:
            self.sock.close()


# ==============================================================================
# Mainframe Schema & Deterministic Mock Transaction Generator
# ==============================================================================
SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS public.legacy_transactions (
    transaction_id   VARCHAR(24) PRIMARY KEY,
    account_number   VARCHAR(20) NOT NULL,
    customer_name    VARCHAR(100) NOT NULL,
    customer_ssn     VARCHAR(11) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    amount           NUMERIC(12, 2) NOT NULL,
    currency         CHAR(3) NOT NULL DEFAULT 'USD',
    branch_code      VARCHAR(12) NOT NULL,
    region           VARCHAR(20) NOT NULL,
    status           VARCHAR(20) NOT NULL,
    cobol_batch_id   VARCHAR(24) NOT NULL,
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL
);

-- Required by AWS DMS for PostgreSQL logical replication to capture full before/after images on UPDATE/DELETE
ALTER TABLE public.legacy_transactions REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS idx_legacy_tx_region ON public.legacy_transactions(region);
CREATE INDEX IF NOT EXISTS idx_legacy_tx_updated_at ON public.legacy_transactions(updated_at);
"""

FIRST_NAMES = [
    "JAMES", "MARY", "ROBERT", "PATRICIA", "JOHN", "JENNIFER", "MICHAEL", "LINDA",
    "DAVID", "ELIZABETH", "WILLIAM", "BARBARA", "RICHARD", "SUSAN", "JOSEPH", "JESSICA",
    "THOMAS", "SARAH", "CHARLES", "KAREN", "DANIEL", "LISA", "MATTHEW", "NANCY",
]

LAST_NAMES = [
    "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS",
    "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ", "WILSON", "ANDERSON", "THOMAS",
    "TAYLOR", "MOORE", "JACKSON", "MARTIN", "LEE", "PEREZ", "THOMPSON", "WHITE",
]

REGIONS = ["US-EAST", "US-WEST", "EU-CENTRAL", "AP-SOUTH"]
TX_TYPES = ["WIRE_TRANSFER", "ACH_CREDIT", "ACH_DEBIT", "POS_SETTLEMENT", "CORE_LEDGER_ADJ"]
STATUSES = ["SETTLED", "SETTLED", "SETTLED", "PENDING", "CLEARED"]
BRANCHES = ["CICS-NY-01", "CICS-CHI-02", "CICS-SF-03", "CICS-LON-04", "CICS-SG-05"]


def generate_batch_insert_sql(start_idx: int, batch_size: int, rng: random.Random) -> str:
    """Builds a single multi-row INSERT ... ON CONFLICT DO NOTHING statement for fast seeding."""
    base_time = dt.datetime(2026, 1, 1, 8, 0, 0)
    values_list: list[str] = []

    for i in range(start_idx, start_idx + batch_size):
        tx_id = f"TXN-{i:08d}"
        acct = f"ACCT-{rng.randint(10000000, 99999999)}"
        cname = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        ssn = f"{rng.randint(100, 899)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}"
        tx_type = rng.choice(TX_TYPES)
        amount = round(rng.uniform(15.50, 25000.00), 2)
        branch = rng.choice(BRANCHES)
        region = REGIONS[i % len(REGIONS)]
        status = rng.choice(STATUSES)
        batch_id = f"JCL-BATCH-{(i // 500) + 1:04d}"
        ts = (base_time + dt.timedelta(seconds=i * 30)).strftime("%Y-%m-%d %H:%M:%S")

        values_list.append(
            f"('{tx_id}','{acct}','{cname}','{ssn}','{tx_type}',{amount:.2f},'USD',"
            f"'{branch}','{region}','{status}','{batch_id}','{ts}','{ts}')"
        )

    values_sql = ",\n".join(values_list)
    return (
        "INSERT INTO public.legacy_transactions ("
        "transaction_id, account_number, customer_name, customer_ssn, transaction_type, "
        "amount, currency, branch_code, region, status, cobol_batch_id, created_at, updated_at"
        f") VALUES\n{values_sql}\n"
        "ON CONFLICT (transaction_id) DO NOTHING;"
    )


def run_seeder(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    action: str = "seed",
    total_rows: int = 10000,
    batch_size: int = 500,
) -> dict[str, Any]:
    """Executes the requested database action ('seed', 'cdc_simulate', 'verify', or 'reset')."""
    start_time = time.time()
    client = PgWireClient(host=host, port=port, user=user, password=password, database=database)

    try:
        # Ensure schema exists
        client.execute(SCHEMA_DDL)

        if action == "reset":
            client.execute("TRUNCATE TABLE public.legacy_transactions;")

        if action in ("seed", "reset"):
            rng = random.Random(42)  # Deterministic seed for reproducible analytics
            for start_idx in range(1, total_rows + 1, batch_size):
                current_batch = min(batch_size, total_rows - start_idx + 1)
                sql = generate_batch_insert_sql(start_idx, current_batch, rng)
                client.execute(sql)

        elif action == "cdc_simulate":
            # Day 10: Perform live UPDATEs, INSERTs, and DELETEs to trigger AWS DMS CDC
            now_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            # 1. Update 25 PENDING transactions to SETTLED
            client.execute(
                f"""
                UPDATE public.legacy_transactions
                SET status = 'SETTLED',
                    amount = amount + 100.00,
                    updated_at = '{now_str}'
                WHERE transaction_id IN (
                    SELECT transaction_id FROM public.legacy_transactions
                    WHERE status = 'PENDING'
                    ORDER BY transaction_id
                    LIMIT 25
                );
                """
            )
            # 2. Insert 10 new real-time CDC transactions
            max_row = client.execute("SELECT COUNT(*) FROM public.legacy_transactions;")[0][0]
            base_idx = int(max_row) + 100000
            rng = random.Random(int(time.time()))
            cdc_insert_sql = generate_batch_insert_sql(base_idx, 10, rng)
            client.execute(cdc_insert_sql)

        # Gather verification telemetry
        total_count = int(client.execute("SELECT COUNT(*) FROM public.legacy_transactions;")[0][0])
        logical_rep = client.execute("SHOW rds.logical_replication;")[0][0]
        region_rows = client.execute(
            "SELECT region, COUNT(*) FROM public.legacy_transactions GROUP BY region ORDER BY region;"
        )
        status_rows = client.execute(
            "SELECT status, COUNT(*) FROM public.legacy_transactions GROUP BY status ORDER BY status;"
        )

        elapsed = round(time.time() - start_time, 2)
        return {
            "status": "SUCCESS",
            "action": action,
            "database": database,
            "host": host,
            "total_rows": total_count,
            "rds_logical_replication": logical_rep,
            "region_distribution": {r[0]: int(r[1]) for r in region_rows},
            "status_distribution": {s[0]: int(s[1]) for s in status_rows},
            "elapsed_seconds": elapsed,
        }
    finally:
        client.close()


# ==============================================================================
# AWS Lambda Entrypoint (for Zero-Trust Private Subnet Execution)
# ==============================================================================
def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    event = event or {}
    action = str(event.get("action", "seed"))
    total_rows = int(event.get("rows", os.environ.get("SEED_ROWS", "10000")))

    host = os.environ["DB_HOST"]
    port = int(os.environ.get("DB_PORT", "5432"))
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    database = os.environ["DB_NAME"]

    result = run_seeder(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        action=action,
        total_rows=total_rows,
    )
    return {
        "statusCode": 200,
        "body": json.dumps(result, indent=2),
    }


# ==============================================================================
# CLI / Docker Entrypoint
# ==============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed or simulate CDC on the Mainframe RDS PostgreSQL instance.")
    parser.add_argument("--action", choices=["seed", "cdc_simulate", "verify", "reset"], default="seed")
    parser.add_argument("--rows", type=int, default=int(os.environ.get("SEED_ROWS", "10000")))
    parser.add_argument("--host", default=os.environ.get("DB_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DB_PORT", "5432")))
    parser.add_argument("--user", default=os.environ.get("DB_USER", "mainframe_admin"))
    parser.add_argument("--password", default=os.environ.get("DB_PASSWORD", ""))
    parser.add_argument("--database", default=os.environ.get("DB_NAME", "mainframe_core"))
    args = parser.parse_args()

    summary = run_seeder(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        action=args.action,
        total_rows=args.rows,
    )
    print(json.dumps(summary, indent=2))
