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
  description = "Schema for stocks, crypto, weather, EDGAR fundamentals, FRED macro, and historical price data lake"
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
    columns {
      name = "symbol"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "volume"
      type = "bigint"
    }
    columns {
      name = "change_percent"
      type = "double"
    }
    columns {
      name = "trading_day"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
  }
  partition_keys {
    name = "date"
    type = "string"
  }
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
    columns {
      name = "symbol"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
  }
  partition_keys {
    name = "date"
    type = "string"
  }
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
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "temperature"
      type = "double"
    }
    columns {
      name = "feels_like"
      type = "double"
    }
    columns {
      name = "humidity"
      type = "int"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name = "wind_speed"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
  }
  partition_keys {
    name = "date"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "forecast" {
  name          = "forecast"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json" }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/forecast/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "forecast_time"
      type = "string"
    }
    columns {
      name = "temperature"
      type = "double"
    }
    columns {
      name = "feels_like"
      type = "double"
    }
    columns {
      name = "humidity"
      type = "int"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name = "wind_speed"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
  }
  partition_keys {
    name = "date"
    type = "string"
  }
}

# SEC EDGAR fundamentals — partitioned by cik + year
# S3 path: fundamentals/cik={cik}/year={year}/data.json

resource "aws_glue_catalog_table" "fundamentals" {
  name          = "fundamentals"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json" }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/fundamentals/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns {
      name = "symbol"
      type = "string"
    }
    columns {
      name = "capex_usd"
      type = "bigint"
    }
    columns {
      name = "revenue_usd"
      type = "bigint"
    }
    columns {
      name = "revenue_tag"
      type = "string"
    }
    columns {
      name = "period_end"
      type = "string"
    }
    columns {
      name = "filed"
      type = "string"
    }
    columns {
      name = "extracted_at"
      type = "string"
    }
  }
  partition_keys {
    name = "cik"
    type = "string"
  }
  partition_keys {
    name = "year"
    type = "string"
  }
}

# FRED macro indicators — partitioned by series + year
# S3 path: macro_indicators/series={series_id}/year={year}/data.json

resource "aws_glue_catalog_table" "macro_indicators" {
  name          = "macro_indicators"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json" }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/macro_indicators/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns {
      name = "series_id"
      type = "string"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name = "units"
      type = "string"
    }
    columns {
      name = "date"
      type = "string"
    }
    columns {
      name = "month"
      type = "int"
    }
    columns {
      name = "value"
      type = "double"
    }
    columns {
      name = "extracted_at"
      type = "string"
    }
  }
  partition_keys {
    name = "series"
    type = "string"
  }
  partition_keys {
    name = "year"
    type = "string"
  }
}

# Historical monthly prices — partitioned by symbol
# S3 path: historical_prices/symbol={symbol}/monthly.json

resource "aws_glue_catalog_table" "historical_prices" {
  name          = "historical_prices"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json" }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/historical_prices/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns {
      name = "date"
      type = "string"
    }
    columns {
      name = "close"
      type = "double"
    }
    columns {
      name = "volume"
      type = "bigint"
    }
    columns {
      name = "extracted_at"
      type = "string"
    }
  }
  partition_keys {
    name = "symbol"
    type = "string"
  }
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
