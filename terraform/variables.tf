variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for the data lake"
  type        = string
  default     = "ai-sharpe-analysis-eric"
}

variable "glue_database_name" {
  description = "AWS Glue catalog database name"
  type        = string
  default     = "stock_analysis_db"
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name"
  type        = string
  default     = "stock_analysis_workgroup"
}

variable "project_name" {
  description = "Project name used for tagging resources"
  type        = string
  default     = "data-engineering-portfolio"
}
