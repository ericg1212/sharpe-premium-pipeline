terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Data Lake

resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name
  tags = {
    Project = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Glue Catalog

resource "aws_glue_catalog_database" "main" {
  name        = var.glue_database_name
  description = "Schema for stocks, crypto, and weather data lake"
}

resource "aws_glue_catalog_table" "stocks" {
  name          = "stocks"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json" }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/stocks/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns { name = "symbol";         type = "string" }
    columns { name = "price";          type = "double" }
    columns { name = "volume";         type = "bigint" }
    columns { name = "change_percent"; type = "double" }
    columns { name = "trading_day";    type = "string" }
    columns { name = "timestamp";      type = "string" }
  }
  partition_keys { name = "date"; type = "string" }
}

resource "aws_glue_catalog_table" "crypto" {
  name          = "crypto"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json" }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/crypto/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns { name = "symbol";    type = "string" }
    columns { name = "price";     type = "double" }
    columns { name = "timestamp"; type = "string" }
  }
  partition_keys { name = "date"; type = "string" }
}

resource "aws_glue_catalog_table" "weather" {
  name          = "weather"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json" }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/weather/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns { name = "city";        type = "string" }
    columns { name = "temperature"; type = "double" }
    columns { name = "feels_like";  type = "double" }
    columns { name = "humidity";    type = "int"    }
    columns { name = "description"; type = "string" }
    columns { name = "wind_speed";  type = "double" }
    columns { name = "timestamp";   type = "string" }
  }
  partition_keys { name = "date"; type = "string" }
}

# Athena Workgroup

resource "aws_athena_workgroup" "main" {
  name        = var.athena_workgroup_name
  description = "Workgroup for stock analysis queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    result_configuration {
      output_location = "s3://${var.s3_bucket_name}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = { Project = var.project_name }
}
