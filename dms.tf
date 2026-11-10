# ------------------------------------------------------------------------------
# Week 2 (Days 6–8): AWS DMS Real-Time Change Data Capture (CDC) Pipeline
# Source: Private RDS PostgreSQL 16 -> Target: S3 Raw Zone (Apache Parquet)
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 1. Required AWS DMS Account Service Roles (dms-vpc-role & dms-cloudwatch-logs-role)
# Required by AWS DMS in new accounts before creating a Replication Subnet Group
# ------------------------------------------------------------------------------
resource "aws_iam_role" "dms_vpc_role" {
  name               = "dms-vpc-role"
  assume_role_policy = data.aws_iam_policy_document.dms_assume_role.json

  tags = {
    Name        = "dms-vpc-role"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "dms_vpc_role" {
  role       = aws_iam_role.dms_vpc_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}

resource "aws_iam_role" "dms_cloudwatch_logs_role" {
  name               = "dms-cloudwatch-logs-role"
  assume_role_policy = data.aws_iam_policy_document.dms_assume_role.json

  tags = {
    Name        = "dms-cloudwatch-logs-role"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "dms_cloudwatch_logs_role" {
  role       = aws_iam_role.dms_cloudwatch_logs_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole"
}

# ------------------------------------------------------------------------------
# 2. Day 6: DMS Replication Subnet Group, Security Group & Instance (dms.t3.micro)
# ------------------------------------------------------------------------------
resource "aws_dms_replication_subnet_group" "mainframe" {
  replication_subnet_group_id          = "${var.project_name}-dms-subnet-group-${var.environment}"
  replication_subnet_group_description = "Private Multi-AZ subnet group for AWS DMS CDC replication"
  subnet_ids                           = aws_subnet.private[*].id

  depends_on = [
    aws_iam_role_policy_attachment.dms_vpc_role
  ]

  tags = {
    Name        = "${var.project_name}-dms-subnet-group"
    Environment = var.environment
  }
}

resource "aws_security_group" "dms" {
  name        = "${var.project_name}-dms-sg-${var.environment}"
  description = "Security group for AWS DMS Replication Instance in Private Subnets"
  vpc_id      = aws_vpc.main.id

  # Outbound to Private RDS PostgreSQL 16 (TCP 5432)
  egress {
    description = "PostgreSQL 5432 to Private RDS Source"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  # Outbound HTTPS (443) to S3 via the free VPC S3 Gateway Endpoint
  egress {
    description = "HTTPS 443 to Amazon S3 via VPC Gateway Endpoint"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-dms-sg"
    Environment = var.environment
  }
}

resource "aws_dms_replication_instance" "mainframe_cdc" {
  replication_instance_id    = "${var.project_name}-dms-instance-${var.environment}"
  replication_instance_class = var.dms_instance_class
  allocated_storage          = 20

  replication_subnet_group_id = aws_dms_replication_subnet_group.mainframe.id
  vpc_security_group_ids      = [aws_security_group.dms.id]

  # FinOps & Zero-Trust: Private Subnet only ($0 public IPv4 cost; S3 via Gateway Endpoint)
  publicly_accessible        = false
  multi_az                   = false
  apply_immediately          = true
  auto_minor_version_upgrade = true

  tags = {
    Name        = "${var.project_name}-dms-instance"
    Environment = var.environment
    Layer       = "Ingestion"
  }
}

# ------------------------------------------------------------------------------
# 3. Day 7: DMS Source Endpoint (RDS PostgreSQL 16) & Target Endpoint (S3 Parquet)
# ------------------------------------------------------------------------------
resource "aws_dms_endpoint" "rds_source" {
  endpoint_id   = "${var.project_name}-rds-source-${var.environment}"
  endpoint_type = "source"
  engine_name   = "postgres"

  server_name   = aws_db_instance.mainframe_source.address
  port          = aws_db_instance.mainframe_source.port
  database_name = var.db_name
  username      = var.db_username
  password      = var.db_password

  # PostgreSQL 16 on RDS enforces SSL by default
  ssl_mode = "require"

  tags = {
    Name        = "${var.project_name}-rds-source-endpoint"
    Environment = var.environment
  }
}

resource "aws_dms_s3_endpoint" "datalake_raw" {
  endpoint_id             = "${var.project_name}-s3-raw-target-${var.environment}"
  endpoint_type           = "target"
  bucket_name             = aws_s3_bucket.datalake.id
  bucket_folder           = "raw"
  service_access_role_arn = aws_iam_role.dms_s3.arn

  # Write directly as Apache Parquet for downstream Glue PySpark & Iceberg ingestion
  data_format                      = "parquet"
  parquet_version                  = "parquet-2-0"
  compression_type                 = "NONE"
  parquet_timestamp_in_millisecond = true

  # Include CDC metadata columns: Op ('I', 'U', 'D') and commit timestamp ('dms_commit_ts')
  timestamp_column_name   = "dms_commit_ts"
  cdc_inserts_and_updates = true
  add_column_name         = true

  depends_on = [
    aws_iam_role_policy.dms_s3,
    aws_vpc_endpoint.s3
  ]

  tags = {
    Name        = "${var.project_name}-s3-raw-target-endpoint"
    Environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# 4. Day 8: DMS Replication Task (Full Load + Continuous WAL CDC)
# ------------------------------------------------------------------------------
resource "aws_dms_replication_task" "mainframe_cdc" {
  replication_task_id      = "${var.project_name}-full-load-cdc-${var.environment}"
  migration_type           = "full-load-and-cdc"
  replication_instance_arn = aws_dms_replication_instance.mainframe_cdc.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.rds_source.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.datalake_raw.endpoint_arn

  # Automatically starts Full Load + CDC upon creation
  start_replication_task = true

  table_mappings = jsonencode({
    rules = [
      {
        rule-type = "selection"
        rule-id   = "1"
        rule-name = "replicate-legacy-transactions"
        object-locator = {
          schema-name = "public"
          table-name  = "legacy_transactions"
        }
        rule-action = "include"
      }
    ]
  })

  replication_task_settings = jsonencode({
    TargetMetadata = {
      TargetSchema                 = ""
      SupportLobs                  = true
      FullLobMode                  = false
      LobChunkSize                 = 64
      LimitedSizeLobMode           = true
      LobMaxSize                   = 32
      InlineLobMaxSize             = 0
      LoadMaxFileSize              = 0
      ParallelLoadThreads          = 0
      ParallelLoadBufferSize       = 0
      BatchApplyEnabled            = false
      TaskRecoveryTableEnabled     = false
      ParallelApplyThreads         = 0
      ParallelApplyBufferSize      = 0
      ParallelApplyQueuesPerThread = 0
    }
    FullLoadSettings = {
      TargetTablePrepMode             = "DROP_AND_CREATE"
      CreatePkAfterFullLoad           = false
      StopTaskCachedChangesApplied    = false
      StopTaskCachedChangesNotApplied = false
      MaxFullLoadSubTasks             = 8
      TransactionConsistencyTimeout   = 600
      CommitRate                      = 10000
    }
    Logging = {
      EnableLogging = true
    }
  })

  depends_on = [
    aws_iam_role_policy_attachment.dms_cloudwatch_logs_role
  ]

  tags = {
    Name        = "${var.project_name}-full-load-cdc-task"
    Environment = var.environment
  }
}
