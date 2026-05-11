terraform {
  backend "s3" {
    bucket  = "terraform-state-mainframe-mod-dev-8044"
    key     = "dev/mainframe-modernization/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true

    # REPLACING dynamodb_table with the modern standard:
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      ManagedBy   = "Terraform"
      Environment = var.environment
    }
  }
}