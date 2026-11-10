variable "aws_region" {
  description = "The AWS region for infrastructure deployment."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "poc", "test", "stage", "prod"], var.environment)
    error_message = "Environment must be one of 'dev', 'poc', 'test', 'stage', or 'prod'."
  }
}

variable "project_name" {
  description = "The name of the project."
  type        = string
  default     = "mainframe-modernization"
}

variable "db_name" {
  description = "Database name for the simulated legacy mainframe RDS instance."
  type        = string
  default     = "mainframe_core"
}

variable "db_username" {
  description = "Master username for the simulated legacy mainframe RDS instance."
  type        = string
  default     = "mainframe_admin"
}

variable "db_password" {
  description = "Master password for the simulated legacy mainframe RDS instance (override via terraform.tfvars or TF_VAR_db_password)."
  type        = string
  sensitive   = true
  default     = "MainframeMod2026!Secure"
}

variable "db_instance_class" {
  description = "RDS instance class (db.t3.micro for Free-Tier/FinOps)."
  type        = string
  default     = "db.t3.micro"
}

variable "rds_multi_az" {
  description = "Enable Multi-AZ standby for RDS (true for prod resiliency, false in dev for FinOps cost savings)."
  type        = bool
  default     = false
}

variable "dms_instance_class" {
  description = "AWS DMS Replication Instance class (dms.t3.small is the minimum class on DMS Engine 3.5.x)."
  type        = string
  default     = "dms.t3.small"
}

locals {
  tags = {
    Project     = var.project_name
    ManagedBy   = "Terraform"
    Environment = var.environment
  }
}
