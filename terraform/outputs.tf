output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "glue_database_name" {
  description = "Name of the Glue catalog database"
  value       = aws_glue_catalog_database.main.name
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.main.name
}

output "athena_results_location" {
  description = "S3 location for Athena query results"
  value       = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"
}
