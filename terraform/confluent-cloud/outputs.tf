# ---------- Confluent Cloud ----------
output "cluster_id" {
  description = "Confluent Cloud Kafka cluster ID"
  value       = confluent_kafka_cluster.enterprise.id
}

output "environment_id" {
  description = "Confluent Cloud environment ID"
  value       = confluent_environment.main.id
}

output "kafka_api_key" {
  description = "Kafka API key (for demo producer)"
  value       = confluent_api_key.kafka.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Kafka API secret (for demo producer)"
  value       = confluent_api_key.kafka.secret
  sensitive   = true
}

output "tableflow_api_key" {
  description = "Cloud API key for Tableflow / catalog sync"
  value       = confluent_api_key.tableflow.id
  sensitive   = true
}

output "tableflow_api_secret" {
  description = "Cloud API secret for Tableflow / catalog sync"
  value       = confluent_api_key.tableflow.secret
  sensitive   = true
}

output "bootstrap_server" {
  description = "Kafka bootstrap server endpoint (PNI — use from bastion via NGINX proxy)"
  value       = local.pni_bootstrap_endpoint
}

output "schema_registry_url" {
  description = "Schema Registry URL"
  value       = data.confluent_schema_registry_cluster.main.rest_endpoint
}

# ---------- AWS Networking ----------
output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.main.cidr_block
}

output "private_subnet_ids" {
  description = "Private subnet IDs (for Lambda deployment)"
  value       = aws_subnet.private[*].id
}

output "lambda_security_group_id" {
  description = "Security group ID for Lambda"
  value       = aws_security_group.lambda.id
}

# ---------- S3 ----------
output "s3_bucket_name" {
  description = "S3 bucket name for Tableflow BYOB"
  value       = aws_s3_bucket.tableflow.bucket
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN for Tableflow BYOB"
  value       = aws_s3_bucket.tableflow.arn
}

# ---------- BYOB ----------
output "provider_integration_id" {
  description = "Confluent provider integration ID (for Tableflow BYOB)"
  value       = confluent_provider_integration.aws.id
}

output "kafka_rest_endpoint" {
  description = "Kafka cluster REST endpoint (PNI)"
  value       = local.pni_rest_endpoint
}

# ---------- Databricks ----------
output "databricks_catalog_name" {
  description = "Unity Catalog catalog name for synced tables"
  value       = databricks_catalog.tableflow.name
}

output "databricks_external_location" {
  description = "External location name in Unity Catalog"
  value       = databricks_external_location.tableflow.name
}

# ---------- PrivateLink Attachment (Console Access) ----------
output "private_link_attachment_id" {
  description = "Confluent PrivateLink Attachment ID (for console access)"
  value       = confluent_private_link_attachment.console.id
}

output "private_link_attachment_dns_domain" {
  description = "DNS domain for the PrivateLink Attachment"
  value       = confluent_private_link_attachment.console.dns_domain
}

# ---------- Bastion Host ----------
output "bastion_public_ip" {
  description = "Public IP of the bastion host"
  value       = aws_instance.bastion.public_ip
}

output "bastion_ssh_command" {
  description = "SSH command to connect to the bastion host"
  value       = "ssh -i bastion-key.pem ec2-user@${aws_instance.bastion.public_ip}"
}

# ---------- Generated Env Files ----------
output "topics_env" {
  description = "Auto-generated .env.topics file for the setup script"
  sensitive   = true
  value       = <<-EOT
    CONFLUENT_CLOUD_API_KEY="${var.confluent_cloud_api_key}"
    CONFLUENT_CLOUD_API_SECRET="${var.confluent_cloud_api_secret}"
    TABLEFLOW_API_KEY="${confluent_api_key.tableflow.id}"
    TABLEFLOW_API_SECRET="${confluent_api_key.tableflow.secret}"
    CLUSTER_ID="${confluent_kafka_cluster.enterprise.id}"
    ENVIRONMENT_ID="${confluent_environment.main.id}"
    KAFKA_API_KEY="${confluent_api_key.kafka.id}"
    KAFKA_API_SECRET="${confluent_api_key.kafka.secret}"
    SERVICE_ACCOUNT_ID="${confluent_service_account.sync.id}"
    KAFKA_REST_ENDPOINT="${local.pni_rest_endpoint}"
    SCHEMA_REGISTRY_URL="${data.confluent_schema_registry_cluster.main.rest_endpoint}"
    SCHEMA_REGISTRY_API_KEY="${confluent_api_key.schema_registry.id}"
    SCHEMA_REGISTRY_API_SECRET="${confluent_api_key.schema_registry.secret}"
    S3_BUCKET_NAME="${aws_s3_bucket.tableflow.bucket}"
    PROVIDER_INTEGRATION_ID="${confluent_provider_integration.aws.id}"
    BOOTSTRAP_SERVER="${local.pni_bootstrap_endpoint}"
  EOT
}

output "sync_env" {
  description = "Auto-generated .env.sync file for the catalog sync script"
  sensitive   = true
  value       = <<-EOT
    CONFLUENT_API_KEY=${confluent_api_key.tableflow.id}
    CONFLUENT_API_SECRET=${confluent_api_key.tableflow.secret}
    CONFLUENT_CLUSTER_ID=${confluent_kafka_cluster.enterprise.id}
    CONFLUENT_ENVIRONMENT_ID=${confluent_environment.main.id}
    DATABRICKS_HOST=${var.databricks_host}
    DATABRICKS_TOKEN=${var.databricks_token}
    DATABRICKS_WAREHOUSE_ID=<YOUR_WAREHOUSE_ID>
    TARGET_CATALOG=${databricks_catalog.tableflow.name}
    TARGET_SCHEMA=${confluent_kafka_cluster.enterprise.id}
  EOT
}
