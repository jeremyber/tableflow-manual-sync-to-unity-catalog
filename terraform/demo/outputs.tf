output "lambda_function_name" {
  description = "Lambda function name — invoke with: aws lambda invoke --function-name <name> /dev/stdout"
  value       = aws_lambda_function.catalog_sync.function_name
}

output "databricks_catalog" {
  description = "Unity Catalog catalog name"
  value       = databricks_catalog.main.name
}

output "databricks_schema" {
  description = "Unity Catalog schema name"
  value       = databricks_schema.main.name
}
