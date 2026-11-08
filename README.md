# AWS Data Lakehouse: Legacy Mainframe Modernization

[![Infrastructure: Terraform](https://img.shields.io/badge/Infrastructure-Terraform-623CE4?logo=terraform)](https://www.terraform.io/)
[![Data Layer: Apache Iceberg](https://img.shields.io/badge/Data_Layer-Apache_Iceberg-blue?logo=apache)](https://iceberg.apache.org/)
[![Cloud: AWS](https://img.shields.io/badge/Cloud-AWS-FF9900?logo=amazon-aws)](https://aws.amazon.com/)

## 📌 Executive Summary
This repository contains the end-to-end architecture for migrating a legacy monolithic environment (simulated via RDS) to a modern **AWS Data Lakehouse**. The project emphasizes **Change Data Capture (CDC)**, **ACID transactions on S3**, and **Security-as-Code**.

As a Lead-level demonstration, this project moves beyond "Lift and Shift" to showcase **Modernization (Re-platforming)** using a Lakehouse pattern to provide high-performance analytics with lower operational overhead.

---

## 🏗 Architecture
The solution implements a "Medallion Architecture" (Bronze/Silver/Gold) within AWS:



1.  **Ingestion (CDC):** AWS DMS captures real-time changes from a legacy source and lands them in S3 (Parquet).
2.  **Storage Layer:** Amazon S3 managed by **Apache Iceberg** for table evolution and transactional consistency.
3.  **Processing:** AWS Glue (PySpark) handles the transformation logic and Iceberg cataloging.
4.  **Governance:** AWS Lake Formation manages fine-grained access control (Row-level security).
5.  **Analytics:** Amazon Athena provides a serverless SQL interface for end-users.

---

## 📊 Project Status

**Current Phase:** Week 1 — Foundations & Infrastructure (IaC Layer)

| Phase | Milestone | Deliverable / Terraform Module | Status |
| :--- | :--- | :--- | :--- |
| **Week 1: Day 1** | **Backend & State Setup** | S3 Remote State with Native S3 Locking (`use_lockfile = true`) (`providers.tf`, `versions.tf`) | ✅ Completed |
| **Week 1: Day 2** | **Zero-Trust Network Architecture** | Multi-AZ VPC, 2 Public / 2 Private Subnets, IGW, S3 Gateway Endpoint (`networking.tf`) | ✅ Completed |
| **Week 1: Day 3** | **Data Lake Storage & Least-Privilege IAM** | Medallion S3 Bucket (Encryption, Versioning, FinOps Lifecycle Rules) + Scoped DMS & Glue IAM Roles (`storage.tf`, `iam.tf`) | ✅ Completed |
| **Week 1: Day 4** | **Mainframe Source Simulation** | Private Multi-AZ RDS PostgreSQL 16 with Logical Replication (`rds.logical_replication = 1`) for CDC (`database.tf`) | ✅ Completed |
| **Week 1: Day 5** | **Legacy Data Seeding** | Python/Docker + Serverless VPC Seeder for 10,000 mock legacy transaction records (`seeder.tf`, `scripts/seed_mainframe_data.py`) | ✅ Completed |
| **Week 2: Days 6–8** | **DMS CDC Pipeline** | DMS Replication Instance, Source (RDS) & Target (S3 Parquet) Endpoints, Full Load + CDC Task | 🔄 Next Up |
| **Week 2: Days 9–10** | **Migration & CDC Validation** | Initial Full Load execution & real-time CDC delta verification in S3 Raw (`raw/`) zone | ⏳ Planned |
| **Week 3: Days 11–13** | **Apache Iceberg & Glue ETL** | Glue Catalog Iceberg tables, PySpark Upsert Job, EventBridge automation | ⏳ Planned |
| **Week 3: Days 14–15** | **Governance & FinOps Teardown** | AWS Lake Formation Row-Level Security, Athena validation, `terraform destroy` compliance | ⏳ Planned |

---

## 🚀 Key Engineering Highlights

### 1. Infrastructure as Code (IaC)
The entire stack is provisioned via **Terraform**. This ensures environment parity and incorporates:
* **Remote State Locking:** S3 backend with **Native S3 State Locking (`use_lockfile = true`)** for collaborative, corruption-free deployments without external DynamoDB overhead.
* **Modular Design:** Decoupled networking, storage, IAM, and database configurations.

### 2. The Lakehouse Advantage (Apache Iceberg)
Unlike traditional Data Lakes, this architecture implements Apache Iceberg to solve:
* **Schema Evolution:** Updating column types without breaking downstream queries.
* **Time Travel:** Querying historical versions of the data for auditing and debugging.
* **Compaction:** Managing small-file problems common in high-frequency CDC ingestion.

### 3. Lead-Level Security & FinOps
* **Zero-Trust Networking:** Resources reside in private subnets; S3 traffic stays within the AWS backbone via **Gateway Endpoints** (eliminating NAT Gateway costs).
* **Row-Level Security:** AWS Lake Formation policies ensure sensitive data (e.g., PII) is only visible to authorized IAM roles.
* **Cost Optimization:** Automated Glue job retries and lifecycle policies for raw data archiving.

---

## 🛠 Tech Stack
* **Cloud:** AWS (VPC, DMS, RDS, S3, Glue, Athena, Lake Formation)
* **IaC:** Terraform
* **Data Formats:** Apache Iceberg, Parquet
* **Languages:** SQL, PySpark, HCL (Terraform)

---

## 📖 Deployment Guide
1.  **Initialize Backend:** Ensure your S3 bucket for Terraform state exists.
2.  **Auth:** `export AWS_PROFILE=your-profile`
3.  **Deploy:**
    ```bash
    terraform init
    terraform plan
    terraform apply
    ```
4.  **Verification:** Check the Glue Data Catalog for the Iceberg table and query via Athena.

---

## 📈 Lessons Learned & Design Trade-offs
* **DMS vs. Glue Blueprints:** Chose DMS for CDC due to its lower latency and superior handling of high-transaction legacy databases.
* **Iceberg vs. Hudi:** Opted for Iceberg due to its seamless integration with the AWS Glue Data Catalog and superior performance for Athena-heavy workloads.

---

**Author:** George Nyamao  
**Role:** Lead Data Engineer  
**Contact:** https://www.linkedin.com/in/george-nyamao/
