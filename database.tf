# ------------------------------------------------------------------------------
# Day 4: Legacy Mainframe Source Simulation (Amazon RDS PostgreSQL 16)
# ------------------------------------------------------------------------------

# 1. DB Subnet Group (Strictly Private Subnets Across 2 AZs)
resource "aws_db_subnet_group" "mainframe" {
  name        = "${var.project_name}-rds-subnet-group-${var.environment}"
  description = "Private Multi-AZ subnet group for legacy mainframe RDS simulation"
  subnet_ids  = aws_subnet.private[*].id

  tags = {
    Name        = "${var.project_name}-rds-subnet-group"
    Environment = var.environment
  }
}

# 2. Zero-Trust Security Group (PostgreSQL 5432 restricted strictly to VPC CIDR)
resource "aws_security_group" "rds_mainframe" {
  name        = "${var.project_name}-rds-sg-${var.environment}"
  description = "Allow PostgreSQL inbound traffic strictly from within the VPC (DMS, Glue, Seeder)"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "PostgreSQL access from internal VPC subnets only"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    description = "Allow outbound traffic within VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  tags = {
    Name        = "${var.project_name}-rds-sg"
    Environment = var.environment
  }
}

# 3. Parameter Group (Enable Logical Replication for AWS DMS CDC)
resource "aws_db_parameter_group" "mainframe_pg16" {
  name        = "${var.project_name}-pg16-cdc-${var.environment}"
  family      = "postgres16"
  description = "PostgreSQL 16 parameter group with logical replication enabled for AWS DMS CDC"

  # Static parameter: enables WAL logical decoding for ongoing CDC
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  # Dynamic parameter: prevents WAL sender timeouts during DMS full load + CDC
  parameter {
    name         = "wal_sender_timeout"
    value        = "0"
    apply_method = "immediate"
  }

  tags = {
    Name        = "${var.project_name}-pg16-cdc-params"
    Environment = var.environment
  }
}

# 4. RDS PostgreSQL Instance (Simulated Legacy Mainframe Database)
resource "aws_db_instance" "mainframe_source" {
  identifier     = "${var.project_name}-source-db-${var.environment}"
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password
  port     = 5432

  db_subnet_group_name   = aws_db_subnet_group.mainframe.name
  vpc_security_group_ids = [aws_security_group.rds_mainframe.id]
  parameter_group_name   = aws_db_parameter_group.mainframe_pg16.name

  # Zero-Trust & Resiliency
  publicly_accessible = false
  multi_az            = var.rds_multi_az

  # Required >= 1 day retention for WAL logical replication / CDC
  backup_retention_period = 1
  apply_immediately       = true

  # Clean teardown support for Day 15 FinOps compliance in non-prod
  skip_final_snapshot = var.environment != "prod"
  deletion_protection = var.environment == "prod"

  tags = {
    Name        = "${var.project_name}-source-db"
    Environment = var.environment
    Role        = "LegacyMainframeSource"
  }
}
