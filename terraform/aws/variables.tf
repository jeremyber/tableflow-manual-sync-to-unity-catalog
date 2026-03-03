variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "tableflow-catalog-sync"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "availability_zones" {
  description = "Availability zones for subnets"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

# Sync configuration
variable "sync_schedule" {
  description = "EventBridge schedule expression (e.g., 'rate(15 minutes)' or 'cron(0/30 * * * ? *)')"
  type        = string
  default     = "rate(15 minutes)"
}

variable "source_type" {
  description = "Catalog source type: glue, s3_discovery, or iceberg_rest"
  type        = string
  default     = "glue"
}

# Glue source
variable "glue_database" {
  description = "Glue database name containing Tableflow Iceberg tables"
  type        = string
  default     = ""
}

# S3 discovery source
variable "iceberg_s3_bucket" {
  description = "S3 bucket containing Iceberg tables (BYOB)"
  type        = string
  default     = ""
}

variable "iceberg_s3_prefix" {
  description = "S3 prefix within the bucket"
  type        = string
  default     = ""
}

# Iceberg REST (future)
variable "iceberg_rest_uri" {
  description = "URI for the Iceberg REST catalog (not available over PN today)"
  type        = string
  default     = ""
}

# Databricks
variable "databricks_workspace_url" {
  description = "Databricks workspace URL"
  type        = string
}

variable "target_catalog" {
  description = "Unity Catalog catalog name to sync tables into"
  type        = string
  default     = "tableflow_catalog"
}

variable "target_schema" {
  description = "Unity Catalog schema name within the target catalog"
  type        = string
  default     = "default"
}
