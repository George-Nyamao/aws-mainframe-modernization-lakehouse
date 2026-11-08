output "datalake_bucket_name" {
  description = "Name of the S3 Data Lakehouse bucket"
  value       = aws_s3_bucket.datalake.id
}

output "datalake_bucket_arn" {
  description = "ARN of the S3 Data Lakehouse bucket"
  value       = aws_s3_bucket.datalake.arn
}

output "dms_s3_role_arn" {
  description = "ARN of the IAM role used by AWS DMS to write raw Parquet files to S3"
  value       = aws_iam_role.dms_s3.arn
}

output "glue_etl_role_arn" {
  description = "ARN of the IAM role used by AWS Glue for PySpark ETL and Apache Iceberg table management"
  value       = aws_iam_role.glue_etl.arn
}

output "rds_endpoint" {
  description = "Connection endpoint (host:port) for the simulated legacy mainframe RDS instance"
  value       = aws_db_instance.mainframe_source.endpoint
}

output "rds_address" {
  description = "Hostname of the simulated legacy mainframe RDS instance"
  value       = aws_db_instance.mainframe_source.address
}

output "rds_database_name" {
  description = "Database name of the simulated legacy mainframe RDS instance"
  value       = aws_db_instance.mainframe_source.db_name
}

output "seeder_lambda_name" {
  description = "Name of the VPC Lambda function used to seed 10k rows (Day 5) and simulate CDC updates (Day 10)"
  value       = aws_lambda_function.mainframe_seeder.function_name
}
