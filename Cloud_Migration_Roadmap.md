# 3-Week AWS Cloud Migration Roadmap: Legacy to Lakehouse
**Role Focus:** Lead Data Engineer / Cloud Architect
**Goal:** Reach $500k/year TC by demonstrating Architectural Rigor, IaC, and Modernization.

---

## 📅 Week 1: Foundations & Infrastructure (The IaC Layer)
*Focus: Establishing the 'Source of Truth' and baseline network security.*

| Day | Task | Description |
| :--- | :--- | :--- |
| **1** | **Backend Setup** | Initialize Git, .gitignore. Set up S3 Backend with **Native S3 Locking** (use_lockfile). |
| **2** | **Network Architecture** | Write Terraform for VPC, 2 Public/2 Private Subnets, and S3 Gateway Endpoint. |
| **3** | **Storage & IAM** | Provision Data Lake S3 bucket and IAM Roles for DMS/Glue with Least Privilege. |
| **4** | **Mainframe Target** | Deploy RDS instance (MySQL/Postgres) to simulate legacy mainframe source. |
| **5** | **Data Seeding** | Python/Docker script to seed RDS with 10k rows of mock transaction data. |

---

## 📅 Week 2: Migration & CDC (The Data Engineering Layer)
*Focus: Implementing real-time Change Data Capture (CDC).*

| Day | Task | Description |
| :--- | :--- | :--- |
| **6** | **DMS Replication** | Terraform provisioning of the DMS Replication Instance (t3.micro). |
| **7** | **Migration Endpoints** | Configure Source (RDS) and Target (S3) Endpoints with Parquet conversion. |
| **8** | **DMS Replication Task** | Create the 'Full Load + CDC' task with specific table mapping rules. |
| **9** | **Initial Migration** | Execute Full Load. Verify Parquet files land in the S3 Raw zone. |
| **10** | **CDC Validation** | Perform updates on RDS; verify delta files appear in S3 within minutes. |

---

## 📅 Week 3: Modernization & Security (The Lakehouse Layer)
*Focus: Transactional consistency with Apache Iceberg and fine-grained access.*

| Day | Task | Description |
| :--- | :--- | :--- |
| **11** | **Iceberg Setup** | Enable Iceberg in Glue Catalog and configure Athena for querying. |
| **12** | **Glue ETL Spark Job** | Write PySpark job to upsert raw Parquet files into **Apache Iceberg** tables. |
| **13** | **Pipeline Automation** | Set up EventBridge triggers to run ETL automatically on S3 object creation. |
| **14** | **Lake Formation** | Implement Row-Level filtering (e.g., masking PII or filtering by Region). |
| **15** | **Validation & Cleanup** | Cross-role testing in Athena. Final `terraform destroy` for FinOps compliance. |

---

## 💡 Lead Engineer Success Principles
1. **FinOps:** Always use S3 Gateway Endpoints to avoid NAT Gateway charges ($0.045/hr).
2. **Modernization:** Don't just move data; use Iceberg to provide ACID compliance and Schema Evolution.
3. **Architecture:** Documentation is half the job. Ensure every architectural decision is in your README.
4. **Resiliency:** Use Multi-AZ deployments for your RDS and subnets.
