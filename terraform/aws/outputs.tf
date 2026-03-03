output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.catalog_sync.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.catalog_sync.arn
}

output "sync_schedule" {
  description = "EventBridge schedule expression"
  value       = var.sync_schedule
}
