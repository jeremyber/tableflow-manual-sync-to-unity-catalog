variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming (must be globally unique for S3)"
  type        = string
  default     = "tableflow-catalog-sync"
}

# ---------- Databricks ----------
variable "databricks_workspace_url" {
  description = "Databricks workspace URL (e.g. https://xxx.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

# ---------- Sync ----------
variable "enable_schedule" {
  description = "Whether to create an EventBridge schedule for recurring sync. When false, invoke the Lambda manually."
  type        = bool
  default     = true
}

variable "sync_schedule" {
  description = "EventBridge schedule expression (only used when enable_schedule = true)"
  type        = string
  default     = "rate(15 minutes)"
}
