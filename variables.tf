variable "aws_region" {
  description = "The AWS region for infrastructure deployment."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environmen."
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

locals {
  tags = {
    Project     = var.project_name
    ManagedBy   = "Terraform"
    Environment = var.environment
  }
}