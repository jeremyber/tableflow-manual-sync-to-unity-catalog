variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (cloud-level, not cluster-level)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret"
  type        = string
  sensitive   = true
}

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

# Databricks
variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://dbc-xxxxx.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token or service principal token"
  type        = string
  sensitive   = true
}

variable "databricks_account_id" {
  description = "Databricks account ID (for IAM trust policy)"
  type        = string
}