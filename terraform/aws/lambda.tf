data "aws_caller_identity" "current" {}

resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_role_policy" "lambda" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Resource = [
          aws_secretsmanager_secret.confluent_credentials.arn,
          aws_secretsmanager_secret.databricks_token.arn,
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = var.iceberg_s3_bucket != "" ? [
          "arn:aws:s3:::${var.iceberg_s3_bucket}",
          "arn:aws:s3:::${var.iceberg_s3_bucket}/*",
        ] : ["arn:aws:s3:::*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTables", "glue:GetTable"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_lambda_function" "catalog_sync" {
  function_name = "${var.project_name}-sync"
  role          = aws_iam_role.lambda.arn
  handler       = "catalog_sync.handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 256

  filename         = "${path.module}/../../dist/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../dist/lambda.zip")

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      SOURCE_TYPE            = var.source_type
      GLUE_DATABASE          = var.glue_database
      GLUE_REGION            = var.aws_region
      ICEBERG_REST_URI       = var.iceberg_rest_uri
      ICEBERG_REST_WAREHOUSE = var.iceberg_s3_bucket != "" ? "s3://${var.iceberg_s3_bucket}/${var.iceberg_s3_prefix}" : ""
      S3_BUCKET              = var.iceberg_s3_bucket
      S3_PREFIX              = var.iceberg_s3_prefix
      DATABRICKS_HOST        = var.databricks_workspace_url
      TARGET_CATALOG         = var.target_catalog
      TARGET_SCHEMA          = var.target_schema
      SECRETS_CONFLUENT_ARN  = aws_secretsmanager_secret.confluent_credentials.arn
      SECRETS_DATABRICKS_ARN = aws_secretsmanager_secret.databricks_token.arn
    }
  }

  tags = {
    Project = var.project_name
  }
}
