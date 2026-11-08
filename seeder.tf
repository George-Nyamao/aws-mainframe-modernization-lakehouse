# ------------------------------------------------------------------------------
# Day 5: Zero-Trust VPC Data Seeder (Serverless AWS Lambda in Private Subnets)
# Connects directly to Private RDS PostgreSQL 16 on TCP 5432 ($0 NAT Gateway cost)
# ------------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_seeder_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "rds_seeder_lambda" {
  name               = "${var.project_name}-rds-seeder-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.lambda_seeder_assume_role.json

  tags = {
    Name        = "${var.project_name}-rds-seeder-role"
    Environment = var.environment
  }
}

# Grants permissions for Lambda to attach to Private VPC Subnets (ENI management) and CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_vpc_access" {
  role       = aws_iam_role.rds_seeder_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Least-Privilege Security Group: Outbound TCP 5432 to VPC CIDR only
resource "aws_security_group" "rds_seeder" {
  name        = "${var.project_name}-seeder-sg-${var.environment}"
  description = "Allow outbound PostgreSQL 5432 from VPC Seeder Lambda to Private RDS"
  vpc_id      = aws_vpc.main.id

  egress {
    description = "PostgreSQL 5432 to Private RDS inside VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  tags = {
    Name        = "${var.project_name}-seeder-sg"
    Environment = var.environment
  }
}

resource "aws_lambda_function" "mainframe_seeder" {
  function_name    = "${var.project_name}-data-seeder-${var.environment}"
  description      = "Seeds 10,000 legacy mainframe transactions into Private RDS and simulates CDC deltas"
  role             = aws_iam_role.rds_seeder_lambda.arn
  handler          = "seed_mainframe_data.lambda_handler"
  runtime          = "python3.12"
  filename         = "${path.module}/scripts/seed_mainframe_data.zip"
  source_code_hash = filebase64sha256("${path.module}/scripts/seed_mainframe_data.zip")

  timeout     = 120
  memory_size = 256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.rds_seeder.id]
  }

  environment {
    variables = {
      DB_HOST     = aws_db_instance.mainframe_source.address
      DB_PORT     = tostring(aws_db_instance.mainframe_source.port)
      DB_NAME     = var.db_name
      DB_USER     = var.db_username
      DB_PASSWORD = var.db_password
      SEED_ROWS   = "10000"
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_vpc_access,
    aws_db_instance.mainframe_source
  ]

  tags = {
    Name        = "${var.project_name}-data-seeder"
    Environment = var.environment
    Role        = "DataSeederAndCDCSimulator"
  }
}
