# ------------------------------------------------------------------------------
# Week 3 (Days 14–15): AWS Lake Formation Governance (Row-Level Security & PII Masking)
# Enforces Row-Level Filtering (`region = 'US-EAST'`) and Column-Level PII Exclusion
# (`customer_ssn` hidden) on the Silver Apache Iceberg v2 table.
# ------------------------------------------------------------------------------

# 1. Configure Lake Formation Data Lake Administrators & Disable IAMAllowedPrincipals Bypass
resource "aws_lakeformation_data_lake_settings" "main" {
  admins = [
    data.aws_caller_identity.current.arn,
    aws_iam_role.iceberg_orchestrator.arn
  ]

  # Critical Lead-Level Security Config: Disable default IAMAllowedPrincipals ("Super")
  # so all new Databases & Tables strictly enforce Lake Formation Data Cells Filters (RLS)
  create_database_default_permissions {
    permissions = []
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }

  create_table_default_permissions {
    permissions = []
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
}

# 2. Register the S3 Iceberg Warehouse Path with AWS Lake Formation
resource "aws_lakeformation_resource" "datalake_iceberg" {
  arn                     = "${aws_s3_bucket.datalake.arn}/iceberg"
  use_service_linked_role = true

  depends_on = [
    aws_lakeformation_data_lake_settings.main
  ]
}

# 3. Ensure Lake Formation Admins Hold Explicit Catalog Permissions on the Database & Tables
resource "aws_lakeformation_permissions" "admin_database" {
  principal   = data.aws_caller_identity.current.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }

  depends_on = [
    aws_lakeformation_data_lake_settings.main
  ]
}

resource "aws_lakeformation_permissions" "admin_tables" {
  principal   = data.aws_caller_identity.current.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.lakehouse.name
    wildcard      = true
  }

  depends_on = [
    aws_lakeformation_permissions.admin_database
  ]
}

resource "aws_lakeformation_permissions" "orchestrator_database" {
  principal   = aws_iam_role.iceberg_orchestrator.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }

  depends_on = [
    aws_lakeformation_data_lake_settings.main
  ]
}

resource "aws_lakeformation_permissions" "orchestrator_tables" {
  principal   = aws_iam_role.iceberg_orchestrator.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.lakehouse.name
    wildcard      = true
  }

  depends_on = [
    aws_lakeformation_permissions.orchestrator_database
  ]
}

# ------------------------------------------------------------------------------
# 4. Create the Regional Analyst IAM Role (Zero Direct S3 Access to Iceberg Data)
# Queries must go through Lake Formation Data Cells Filter vended credentials
# ------------------------------------------------------------------------------
data "aws_iam_policy_document" "analyst_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
}

resource "aws_iam_role" "us_east_analyst" {
  name               = "${var.project_name}-us-east-analyst-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.analyst_assume_role.json

  tags = {
    Name        = "${var.project_name}-us-east-analyst-role"
    Environment = var.environment
    Role        = "RegionalAnalystRLS"
  }
}

data "aws_iam_policy_document" "us_east_analyst_policy" {
  statement {
    sid    = "AllowAthenaAndLakeFormationDataAccess"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartitions",
      "lakeformation:GetDataAccess"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "AllowAthenaQueryResultsOnly"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.datalake.arn,
      "${aws_s3_bucket.datalake.arn}/athena-results/*"
    ]
  }
}

resource "aws_iam_role_policy" "us_east_analyst" {
  name   = "${var.project_name}-us-east-analyst-athena-policy"
  role   = aws_iam_role.us_east_analyst.id
  policy = data.aws_iam_policy_document.us_east_analyst_policy.json
}

# ------------------------------------------------------------------------------
# 5. Day 14: Lake Formation Data Cells Filter (Row-Level Security + PII Masking)
# - Row Filter: `region = 'US-EAST'`
# - Column Masking: Excludes `customer_ssn` PII column
# ------------------------------------------------------------------------------
resource "aws_lakeformation_data_cells_filter" "us_east_no_pii" {
  table_data {
    database_name    = aws_glue_catalog_database.lakehouse.name
    table_catalog_id = data.aws_caller_identity.current.account_id
    table_name       = local.iceberg_table_name
    name             = "us_east_no_pii_filter"

    row_filter {
      filter_expression = "region = 'US-EAST'"
    }

    column_wildcard {
      excluded_column_names = ["customer_ssn"]
    }
  }

  depends_on = [
    aws_lakeformation_permissions.admin_tables
  ]
}

# 6. Grant Regional Analyst Role DESCRIBE on Database & SELECT on the Data Cells Filter
resource "aws_lakeformation_permissions" "analyst_db_describe" {
  principal   = aws_iam_role.us_east_analyst.arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }

  depends_on = [
    aws_lakeformation_data_lake_settings.main
  ]
}

resource "aws_lakeformation_permissions" "analyst_filtered_select" {
  principal   = aws_iam_role.us_east_analyst.arn
  permissions = ["SELECT"]

  data_cells_filter {
    database_name    = aws_glue_catalog_database.lakehouse.name
    table_catalog_id = data.aws_caller_identity.current.account_id
    table_name       = local.iceberg_table_name
    name             = aws_lakeformation_data_cells_filter.us_east_no_pii.table_data[0].name
  }

  depends_on = [
    aws_lakeformation_data_cells_filter.us_east_no_pii,
    aws_lakeformation_permissions.analyst_db_describe
  ]
}
