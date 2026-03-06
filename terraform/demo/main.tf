# ============================================================
# Demo Deployment — Lambda Sync Engine + Databricks Catalog
# ============================================================
#
# Prerequisites:
#   1. Run `terraform apply` in ../confluent-cloud/ first
#      (creates VPC, Confluent cluster, PrivateLink, S3 bucket)
#   2. Build the Lambda zip: ./scripts/build_lambda.sh
#   3. Copy terraform.tfvars.example to terraform.tfvars and fill in values
#
# What this creates:
#   - Databricks catalog + schema for Tableflow tables
#   - Secrets Manager secret for the Databricks token
#   - Lambda function (runs the Python sync engine)
#   - EventBridge schedule (triggers Lambda every 15 min)
#
# The Lambda runs inside the VPC created by ../confluent-cloud/,
# so it can reach Confluent via PrivateLink and S3 via the
# gateway endpoint already provisioned there.
# ============================================================

# ---------- Providers ----------

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "databricks" {
  host  = var.databricks_workspace_url
  token = var.databricks_token
}

data "aws_caller_identity" "current" {}

# ---------- Import outputs from the confluent-cloud stack ----------
# This reads the terraform.tfstate from ../confluent-cloud/ to get
# VPC IDs, subnet IDs, API keys, etc. without duplicating variables.
data "terraform_remote_state" "confluent_cloud" {
  backend = "local"

  config = {
    path = "../confluent-cloud/terraform.tfstate"
  }
}

locals {
  cc = data.terraform_remote_state.confluent_cloud.outputs
}

# ============================================================
# 1. Databricks Catalog + Schema
# ============================================================
# Creates the Unity Catalog catalog and schema where Tableflow
# tables will be registered as external tables.

resource "databricks_catalog" "main" {
  name    = "tableflow_catalog"
  comment = "Catalog for Confluent Tableflow tables (Delta + Iceberg)"

  force_destroy = true
}

resource "databricks_schema" "main" {
  catalog_name = databricks_catalog.main.name
  name         = "default"
  comment      = "Schema for Tableflow synced tables"

  force_destroy = true
}

# ============================================================
# 2. Secrets Manager — Databricks token
# ============================================================
# The Lambda reads this at runtime to authenticate with Databricks.
# Confluent credentials come from the confluent-cloud stack outputs
# and are passed as environment variables.

resource "aws_secretsmanager_secret" "databricks_token" {
  name                    = "${var.project_name}/databricks-token"
  description             = "Databricks personal access token for catalog sync"
  recovery_window_in_days = 0 # Allow immediate delete for demo

  tags = { Project = var.project_name }
}

resource "aws_secretsmanager_secret_version" "databricks_token" {
  secret_id     = aws_secretsmanager_secret.databricks_token.id
  secret_string = var.databricks_token
}

# ============================================================
# 3. Lambda IAM Role + Policy
# ============================================================

resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  tags = { Project = var.project_name }
}

# Lambda needs to:
#   - Write CloudWatch logs
#   - Create/manage ENIs (required for VPC-attached Lambdas)
#   - Read the Databricks token from Secrets Manager
#   - Read S3 (Tableflow Delta/Iceberg files in the BYOB bucket)
resource "aws_iam_role_policy" "lambda" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.databricks_token.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = ["arn:aws:s3:::${local.cc.s3_bucket_name}", "arn:aws:s3:::${local.cc.s3_bucket_name}/*"]
      },
    ]
  })
}

# ============================================================
# 4. Lambda Function — The sync engine
# ============================================================
# Runs the Python catalog_sync package. Discovers Tableflow topics
# via the Confluent Cloud API, diffs against Unity Catalog, and
# creates/updates/removes external tables.

resource "aws_lambda_function" "catalog_sync" {
  function_name = "${var.project_name}-sync"
  role          = aws_iam_role.lambda.arn
  handler       = "catalog_sync.handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300    # 5 min — sync usually finishes in <30s
  memory_size   = 256

  depends_on = [aws_iam_role_policy.lambda]

  # Built by ./scripts/build_lambda.sh
  filename         = "${path.module}/../../dist/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../dist/lambda.zip")

  # Runs in the VPC from the confluent-cloud stack (PrivateLink + S3 endpoint)
  vpc_config {
    subnet_ids         = local.cc.private_subnet_ids
    security_group_ids = [local.cc.lambda_security_group_id]
  }

  environment {
    variables = {
      SOURCE_TYPE              = "confluent_api"
      S3_BUCKET                = local.cc.s3_bucket_name
      DATABRICKS_HOST          = var.databricks_workspace_url
      DATABRICKS_TOKEN         = var.databricks_token
      TARGET_CATALOG           = databricks_catalog.main.name
      TARGET_SCHEMA            = databricks_schema.main.name
      CONFLUENT_API_KEY        = local.cc.tableflow_api_key
      CONFLUENT_API_SECRET     = local.cc.tableflow_api_secret
      CONFLUENT_CLUSTER_ID     = local.cc.cluster_id
      CONFLUENT_ENVIRONMENT_ID = local.cc.environment_id
    }
  }

  tags = { Project = var.project_name }
}

# ============================================================
# 5. EventBridge Schedule — Trigger sync every 15 minutes
# ============================================================

resource "aws_cloudwatch_event_rule" "sync_schedule" {
  count               = var.enable_schedule ? 1 : 0
  name                = "${var.project_name}-schedule"
  description         = "Trigger catalog sync on a schedule"
  schedule_expression = var.sync_schedule

  tags = { Project = var.project_name }
}

resource "aws_cloudwatch_event_target" "sync_lambda" {
  count     = var.enable_schedule ? 1 : 0
  rule      = aws_cloudwatch_event_rule.sync_schedule[0].name
  target_id = "catalog-sync-lambda"
  arn       = aws_lambda_function.catalog_sync.arn
}

# Allow EventBridge to invoke the Lambda
resource "aws_lambda_permission" "eventbridge" {
  count         = var.enable_schedule ? 1 : 0
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.catalog_sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sync_schedule[0].arn
}
