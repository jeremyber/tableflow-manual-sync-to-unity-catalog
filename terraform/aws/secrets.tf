resource "aws_secretsmanager_secret" "confluent_credentials" {
  name        = "${var.project_name}/confluent-credentials"
  description = "Confluent Cloud API key and secret for catalog sync"

  tags = {
    Project = var.project_name
  }
}

resource "aws_secretsmanager_secret" "databricks_token" {
  name        = "${var.project_name}/databricks-token"
  description = "Databricks personal access token or service principal token"

  tags = {
    Project = var.project_name
  }
}
