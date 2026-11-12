# AWS Data Lakehouse: Legacy Mainframe Modernization

[![Infrastructure: Terraform](https://img.shields.io/badge/Infrastructure-Terraform-623CE4?logo=terraform)](https://www.terraform.io/)
[![Data Layer: Apache Iceberg](https://img.shields.io/badge/Data_Layer-Apache_Iceberg-blue?logo=apache)](https://iceberg.apache.org/)
[![Cloud: AWS](https://img.shields.io/badge/Cloud-AWS-FF9900?logo=amazon-aws)](https://aws.amazon.com/)

## 📌 Executive Summary
This repository contains the end-to-end architecture for migrating a legacy monolithic environment (simulated via RDS PostgreSQL 16) to a modern **AWS Data Lakehouse**. The project emphasizes **Change Data Capture (CDC)**, **ACID transactions on S3 with Apache Iceberg v2**, and **Security-as-Code with AWS Lake Formation**.

As a Lead-level demonstration, this project moves beyond "Lift and Shift" to showcase **Modernization (Re-platforming)** using a Lakehouse pattern to provide high-performance analytics with lower operational overhead.

---

## 🏗 Architecture
> 📄 **Deep-Dive Technical Blueprint & Diagrams:** See [**ARCHITECTURE.md**](./ARCHITECTURE.md) for the full Mermaid system schematic, ASCII network topology, sequence diagrams, data model mapping, and FinOps cost analysis.

The solution implements a "Medallion Architecture" (Bronze/Silver/Gold) within AWS:

1.  **Ingestion (CDC):** AWS DMS captures real-time WAL changes from a private legacy source (`RDS PostgreSQL 16`) and lands them in S3 (`raw/`) as Apache Parquet v2.0.
2.  **Storage Layer:** Amazon S3 managed by **Apache Iceberg v2** for schema evolution, partition pruning (`region`), and ACID transactional consistency.
3.  **Processing & Orchestration:** Event-driven **Amazon EventBridge** pipeline triggers a window-deduplicated **ACID `MERGE INTO`** upsert (supporting both **Amazon Athena Engine v3** and **AWS Glue 4.0 PySpark**).
4.  **Governance:** **AWS Lake Formation** enforces fine-grained **Row-Level Security (`region = 'US-EAST'`)** and **Column-Level PII Masking (`customer_ssn` excluded)** via Data Cells Filters.
5.  **Analytics:** Amazon Athena provides a serverless SQL interface for governed end-user analytics.

---

## 📊 Project Status

**Current Phase:** ✅ All 3 Weeks (Days 1–15) Completed & Verified End-to-End

| Phase | Milestone | Deliverable / Terraform Module | Status |
| :--- | :--- | :--- | :--- |
| **Week 1: Day 1** | **Backend & State Setup** | S3 Remote State with Native S3 Locking (`use_lockfile = true`) (`providers.tf`, `versions.tf`) | ✅ Completed |
| **Week 1: Day 2** | **Zero-Trust Network Architecture** | Multi-AZ VPC, 2 Public / 2 Private Subnets, IGW, S3 Gateway Endpoint (`networking.tf`) | ✅ Completed |
| **Week 1: Day 3** | **Data Lake Storage & Least-Privilege IAM** | Medallion S3 Bucket (Encryption, Versioning, FinOps Lifecycle Rules) + Scoped DMS & Glue IAM Roles (`storage.tf`, `iam.tf`) | ✅ Completed |
| **Week 1: Day 4** | **Mainframe Source Simulation** | Private Multi-AZ RDS PostgreSQL 16 with Logical Replication (`rds.logical_replication = 1`) for CDC (`database.tf`) | ✅ Completed |
| **Week 1: Day 5** | **Legacy Data Seeding** | Python/Docker + Serverless VPC Seeder for 10,000 mock legacy transaction records (`seeder.tf`, `scripts/seed_mainframe_data.py`) | ✅ Completed |
| **Week 2: Days 6–8** | **DMS CDC Pipeline** | DMS Replication Instance (`dms.t3.small`), Source (RDS) & Target (S3 Parquet) Endpoints, Full Load + CDC Task (`dms.tf`) | ✅ Completed |
| **Week 2: Days 9–10** | **Migration & CDC Validation** | Initial Full Load (`LOAD00000001.parquet` in 4.49s) & real-time CDC delta (`20261110-092300883.parquet`) verified in S3 `raw/` zone | ✅ Completed |
| **Week 3: Days 11–13** | **Apache Iceberg & Event-Driven ETL** | Glue Catalog Iceberg v2 table, Athena Workgroup, Window-Deduplicated `MERGE INTO` Upsert, S3 -> EventBridge automation (`lakehouse.tf`, `automation.tf`, `scripts/`) | ✅ Completed |
| **Week 3: Days 14–15** | **Governance & FinOps Teardown** | AWS Lake Formation Row-Level Security (`region = 'US-EAST'` -> `2,503` of `10,010` rows) & PII Masking (`customer_ssn` excluded), Cross-Role Athena Validation (`governance.tf`) | ✅ Completed |

---

## 🚀 Key Engineering Highlights

### 1. Infrastructure as Code (IaC)
The entire stack is provisioned via **Terraform**. This ensures environment parity and incorporates:
* **Remote State Locking:** S3 backend with **Native S3 State Locking (`use_lockfile = true`)** for collaborative, corruption-free deployments without external DynamoDB overhead.
* **Modular Design:** Decoupled networking, storage, IAM, database, DMS, lakehouse, automation, and governance modules.

### 2. The Lakehouse Advantage (Apache Iceberg v2)
Unlike traditional Data Lakes, this architecture implements Apache Iceberg v2 to solve:
* **ACID CDC Upserts (`MERGE INTO`):** Deduplicates full-load (`10,000` rows) and ongoing WAL CDC mutations (`25` updates + `10` inserts) by `transaction_id` ordered by `dms_commit_ts` into `10,010` consistent rows.
* **Schema Evolution & Time Travel:** Updating column types and querying historical snapshots without breaking downstream consumers.
* **Partition Pruning:** Partitioned by `region` (`US-EAST`, `US-WEST`, `EU-CENTRAL`, `AP-SOUTH`) with Snappy-compressed Parquet storage.

### 3. Lead-Level Security & FinOps
* **Zero-Trust Networking:** RDS, DMS, and the VPC Data Seeder reside strictly in private subnets (`publicly_accessible = false`, eliminating `$0.005/hr` Public IPv4 charges); S3 traffic stays within the AWS backbone via **Gateway Endpoints** (eliminating `$0.045/hr` NAT Gateway costs).
* **Row-Level Security & PII Masking:** AWS Lake Formation Data Cells Filters (`us_east_no_pii_filter`) restrict regional analysts strictly to `region = 'US-EAST'` (`2,503` rows) while stripping the `customer_ssn` PII column.
* **Cost Optimization:** S3 lifecycle policies automatically transition raw CDC data (`raw/`) to `STANDARD_IA` (30 days) and `GLACIER` (90 days).

---

## 🛠 Tech Stack
* **Cloud:** AWS (VPC, DMS, RDS PostgreSQL 16, S3, Glue Data Catalog, Athena Engine v3, EventBridge, Lambda, Lake Formation)
* **IaC:** Terraform (`>= 1.6.0`, `hashicorp/aws ~> 5.0`)
* **Data Formats:** Apache Iceberg v2, Apache Parquet v2.0
* **Languages:** SQL, Python 3.12 / PySpark, HCL (Terraform)

---

## 📖 End-to-End Deployment & Verification Guide

1. **Deploy Infrastructure:**
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```
2. **Seed 10,000 Mainframe Transactions (Day 5) & Simulate CDC Deltas (Day 10):**
   ```bash
   aws lambda invoke \
     --function-name mainframe-modernization-data-seeder-dev \
     --cli-binary-format raw-in-base64-out \
     --payload '{"action": "seed", "rows": 10000}' \
     --region us-east-1 seed_output.json

   aws lambda invoke \
     --function-name mainframe-modernization-data-seeder-dev \
     --cli-binary-format raw-in-base64-out \
     --payload '{"action": "cdc_simulate"}' \
     --region us-east-1 cdc_output.json
   ```
3. **Trigger or Verify Apache Iceberg ACID `MERGE INTO` (Days 11–13):**
   ```bash
   aws lambda invoke \
     --function-name mainframe-modernization-iceberg-upsert-dev \
     --cli-binary-format raw-in-base64-out \
     --payload '{}' \
     --region us-east-1 iceberg_output.json
   ```
4. **Validate Lake Formation Row-Level Security & PII Masking (Days 14–15):**
   ```bash
   python3 scripts/validate_lake_formation_rls.py
   ```
5. **FinOps Teardown (Day 15):**
   ```bash
   terraform destroy
   ```

---

## 📈 Lessons Learned & Production Design Trade-offs
* **DMS vs. Glue Blueprints:** Chose AWS DMS with PostgreSQL logical replication (`rds.logical_replication = 1` and `REPLICA IDENTITY FULL`) for sub-second WAL capture into S3 Parquet.
* **Handling DMS Parquet `dms_commit_ts` Serialization:** AWS DMS serializes `dms_commit_ts` as a UTF-8 `BINARY`/`STRING` inside Parquet files; casting via `TRY(CAST(dms_commit_ts AS TIMESTAMP(6)))` inside the Bronze-to-Silver `MERGE INTO` window prevents Hive/Trino Parquet type mismatches.
* **Lake Formation `IAM_ALLOWED_PRINCIPALS` Lockdown:** Disabled default `IAM_ALLOWED_PRINCIPALS` (`Super`) grants in `aws_lakeformation_data_lake_settings` so fine-grained Data Cells Filters (`region = 'US-EAST'` and `customer_ssn` column exclusion) cannot be bypassed by IAM-level Glue catalog permissions.

---

**Author:** George Nyamao  
**Role:** Lead Data Engineer  
**Contact:** https://www.linkedin.com/in/george-nyamao/
