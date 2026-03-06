# ============================================================
# Confluent Cloud + AWS Networking — Self-contained provisioning
# ============================================================
# Provisions everything needed on the Confluent Cloud side:
# - AWS VPC with private/public subnets, S3 endpoint, bastion host
# - Confluent Cloud environment + dedicated cluster (PrivateLink + INTERNET)
# - AWS PrivateLink endpoint + Route53 DNS
# - Service account, API keys, role bindings
# - BYOB S3 bucket + Confluent provider integration (IAM)
# ============================================================

# ---------- Providers ----------
provider "aws" {
  region = var.aws_region
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# ---------- Data Sources ----------
data "aws_caller_identity" "current" {}

locals {
  azs                  = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c", "${var.aws_region}f"]
  vpc_cidr             = "10.0.0.0/16"
  private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24", "10.0.4.0/24"]
  public_subnet_cidr   = "10.0.100.0/24"
}

# ============================================================
# AWS Networking
# ============================================================

# ---------- VPC ----------
resource "aws_vpc" "main" {
  cidr_block           = local.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name    = "${var.project_name}-vpc"
    Project = var.project_name
  }
}

# ---------- Subnets ----------
resource "aws_subnet" "private" {
  count             = length(local.private_subnet_cidrs)
  vpc_id            = aws_vpc.main.id
  cidr_block        = local.private_subnet_cidrs[count.index]
  availability_zone = local.azs[count.index]

  tags = {
    Name    = "${var.project_name}-private-${count.index}"
    Project = var.project_name
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = local.public_subnet_cidr
  availability_zone       = local.azs[0]
  map_public_ip_on_launch = true

  tags = {
    Name    = "${var.project_name}-public"
    Project = var.project_name
  }
}

# ---------- Internet Gateway ----------
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name    = "${var.project_name}-igw"
    Project = var.project_name
  }
}

# ---------- Route Tables ----------
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name    = "${var.project_name}-public-rt"
    Project = var.project_name
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name    = "${var.project_name}-private-rt"
    Project = var.project_name
  }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ---------- S3 Gateway VPC Endpoint (free) ----------
resource "aws_vpc_endpoint" "s3" {
  vpc_id          = aws_vpc.main.id
  service_name    = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = [aws_route_table.private.id]

  tags = {
    Name    = "${var.project_name}-s3-endpoint"
    Project = var.project_name
  }
}

# ---------- Lambda Security Group ----------
resource "aws_security_group" "lambda" {
  name_prefix = "${var.project_name}-lambda-"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-lambda-sg"
    Project = var.project_name
  }
}

# ============================================================
# BYOB S3 Bucket
# ============================================================

resource "aws_s3_bucket" "tableflow" {
  bucket        = "${var.project_name}-tableflow-byob"
  force_destroy = true

  tags = {
    Name    = "${var.project_name}-tableflow-byob"
    Project = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "tableflow" {
  bucket = aws_s3_bucket.tableflow.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ============================================================
# Confluent Cloud — Environment + Network + Cluster
# ============================================================

resource "confluent_environment" "main" {
  display_name = var.project_name

  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_network" "privatelink" {
  display_name     = "${var.project_name}-network"
  cloud            = "AWS"
  region           = var.aws_region
  connection_types = ["PRIVATELINK"]

  environment {
    id = confluent_environment.main.id
  }

  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_private_link_access" "aws" {
  display_name = "${var.project_name}-pl-access"

  aws {
    account = data.aws_caller_identity.current.account_id
  }

  environment {
    id = confluent_environment.main.id
  }

  network {
    id = confluent_network.privatelink.id
  }
}

resource "confluent_kafka_cluster" "dedicated" {
  display_name = "${var.project_name}-cluster"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.aws_region

  dedicated {
    cku = 1
  }

  environment {
    id = confluent_environment.main.id
  }

  network {
    id = confluent_network.privatelink.id
  }

  lifecycle {
    prevent_destroy = false
  }
}

# ============================================================
# AWS PrivateLink — VPC Endpoint + Route53 DNS
# ============================================================

resource "aws_security_group" "confluent_privatelink" {
  name_prefix = "${var.project_name}-cc-pl-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
  }

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
  }

  tags = {
    Name    = "${var.project_name}-cc-privatelink-sg"
    Project = var.project_name
  }
}

# Look up which AZs the Confluent PrivateLink service supports
data "aws_vpc_endpoint_service" "confluent" {
  service_name = confluent_network.privatelink.aws[0].private_link_endpoint_service

  depends_on = [confluent_kafka_cluster.dedicated]
}

# Filter subnets to only those in AZs supported by the endpoint service
locals {
  confluent_supported_azs = data.aws_vpc_endpoint_service.confluent.availability_zones
  confluent_pl_subnet_ids = [
    for s in aws_subnet.private : s.id
    if contains(local.confluent_supported_azs, s.availability_zone)
  ]
}

resource "aws_vpc_endpoint" "confluent" {
  vpc_id             = aws_vpc.main.id
  service_name       = confluent_network.privatelink.aws[0].private_link_endpoint_service
  vpc_endpoint_type  = "Interface"
  subnet_ids         = local.confluent_pl_subnet_ids
  security_group_ids = [aws_security_group.confluent_privatelink.id]

  tags = {
    Name    = "${var.project_name}-cc-privatelink"
    Project = var.project_name
  }

  depends_on = [confluent_kafka_cluster.dedicated]
}

resource "aws_route53_zone" "confluent" {
  name = confluent_network.privatelink.dns_domain

  vpc {
    vpc_id = aws_vpc.main.id
  }

  tags = {
    Name    = "${var.project_name}-cc-dns"
    Project = var.project_name
  }
}

# Wildcard CNAME for all broker traffic
resource "aws_route53_record" "confluent_wildcard" {
  zone_id = aws_route53_zone.confluent.zone_id
  name    = "*.${confluent_network.privatelink.dns_domain}"
  type    = "CNAME"
  ttl     = 60
  records = [aws_vpc_endpoint.confluent.dns_entry[0]["dns_name"]]
}

# Per-AZ zonal DNS records — use for_each over the known AZs to avoid
# dynamic count (zonal_subdomains isn't known until after apply)
resource "aws_route53_record" "confluent_zonal" {
  for_each = toset(local.azs)

  zone_id = aws_route53_zone.confluent.zone_id
  name    = "*.${each.value}.${confluent_network.privatelink.dns_domain}"
  type    = "CNAME"
  ttl     = 60
  records = [aws_vpc_endpoint.confluent.dns_entry[0]["dns_name"]]
}

# ============================================================
# Service Account + API Keys + Role Bindings
# ============================================================

resource "confluent_service_account" "sync" {
  display_name = "${var.project_name}-sa"
  description  = "Service account for Tableflow catalog sync"
}

resource "confluent_role_binding" "env_admin" {
  principal   = "User:${confluent_service_account.sync.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_role_binding" "cluster_admin" {
  principal   = "User:${confluent_service_account.sync.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.dedicated.rbac_crn
}

# Kafka API key (for producing/consuming — used by demo producer)
resource "confluent_api_key" "kafka" {
  display_name = "${var.project_name}-kafka-key"
  description  = "Kafka API key for ${var.project_name}"

  owner {
    id          = confluent_service_account.sync.id
    api_version = confluent_service_account.sync.api_version
    kind        = confluent_service_account.sync.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.dedicated.id
    api_version = confluent_kafka_cluster.dedicated.api_version
    kind        = confluent_kafka_cluster.dedicated.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  disable_wait_for_ready = true

  depends_on = [
    confluent_role_binding.cluster_admin,
  ]
}

# Cloud API key (for Tableflow / Confluent Cloud API calls)
resource "confluent_api_key" "tableflow" {
  display_name = "${var.project_name}-tableflow-key"
  description  = "Tableflow API key for ${var.project_name}"

  owner {
    id          = confluent_service_account.sync.id
    api_version = confluent_service_account.sync.api_version
    kind        = confluent_service_account.sync.kind
  }

  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [
    confluent_role_binding.env_admin,
  ]
}

# Schema Registry (auto-provisioned with environment)
data "confluent_schema_registry_cluster" "main" {
  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_kafka_cluster.dedicated]
}

# Schema Registry API key (for registering schemas)
resource "confluent_api_key" "schema_registry" {
  display_name = "${var.project_name}-sr-key"
  description  = "Schema Registry API key for ${var.project_name}"

  owner {
    id          = confluent_service_account.sync.id
    api_version = confluent_service_account.sync.api_version
    kind        = confluent_service_account.sync.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = data.confluent_schema_registry_cluster.main.api_version
    kind        = data.confluent_schema_registry_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [
    confluent_role_binding.env_admin,
  ]
}

# ============================================================
# BYOB — Confluent Provider Integration (IAM)
# ============================================================
# Order of operations (follows the reference tableflow-terraform-demo):
# 1. Pre-compute the IAM role ARN from account ID + role name
# 2. Create confluent_provider_integration with that ARN (role doesn't exist yet)
# 3. Create the IAM role AFTER, using the provider integration's iam_role_arn
#    and external_id outputs directly in the trust policy
# This avoids the need for a null_resource hack to update the trust policy.

locals {
  byob_role_name = "${var.project_name}-confluent-byob"
  byob_role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.byob_role_name}"
}

resource "confluent_provider_integration" "aws" {
  display_name = "${var.project_name}-byob"

  aws {
    customer_role_arn = local.byob_role_arn
  }

  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_environment.main, aws_s3_bucket.tableflow]
}

resource "aws_iam_role" "confluent_byob" {
  name = local.byob_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Confluent Tableflow trust
      {
        Effect = "Allow"
        Principal = {
          AWS = confluent_provider_integration.aws.aws[0].iam_role_arn
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = confluent_provider_integration.aws.aws[0].external_id
          }
        }
      },
      {
        Effect = "Allow"
        Principal = {
          AWS = confluent_provider_integration.aws.aws[0].iam_role_arn
        }
        Action = "sts:TagSession"
      },
      # Databricks Unity Catalog trust
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"
        }
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      },
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          AWS = [
            "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
            "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
          ]
        }
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
          ArnEquals = {
            "aws:PrincipalArn" = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.byob_role_name}"
          }
        }
      }
    ]
  })

  tags = {
    Name    = local.byob_role_name
    Project = var.project_name
  }
}

# Self-assume policy (required by Databricks Unity Catalog)
resource "aws_iam_policy" "confluent_byob_self_assume" {
  name        = "${var.project_name}-confluent-byob-self-assume"
  description = "Allow the BYOB role to assume itself for Databricks Unity Catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = local.byob_role_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "confluent_byob_self_assume" {
  role       = aws_iam_role.confluent_byob.name
  policy_arn = aws_iam_policy.confluent_byob_self_assume.arn
}

resource "aws_iam_policy" "confluent_byob_s3" {
  name        = "${var.project_name}-confluent-byob-s3"
  description = "S3 access policy for Confluent Tableflow BYOB"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.tableflow.arn,
          "${aws_s3_bucket.tableflow.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "confluent_byob_s3" {
  role       = aws_iam_role.confluent_byob.name
  policy_arn = aws_iam_policy.confluent_byob_s3.arn
}

# ============================================================
# Databricks Unity Catalog — Storage Credential + External Location
# ============================================================

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "time_sleep" "wait_for_iam_propagation" {
  create_duration = "60s"

  depends_on = [
    aws_iam_role.confluent_byob,
    aws_iam_role_policy_attachment.confluent_byob_s3,
    aws_iam_role_policy_attachment.confluent_byob_self_assume,
  ]
}

resource "databricks_storage_credential" "tableflow" {
  name    = "${var.project_name}-credential"
  comment = "Managed by Terraform — Tableflow BYOB S3 access"

  aws_iam_role {
    role_arn = aws_iam_role.confluent_byob.arn
  }

  depends_on = [time_sleep.wait_for_iam_propagation]
}

resource "databricks_external_location" "tableflow" {
  name            = "${var.project_name}-location"
  url             = "s3://${aws_s3_bucket.tableflow.bucket}/"
  credential_name = databricks_storage_credential.tableflow.id
  comment         = "Managed by Terraform — Tableflow BYOB bucket"
  force_destroy   = true

  depends_on = [databricks_storage_credential.tableflow]
}

resource "databricks_catalog" "tableflow" {
  name          = "tableflow_sync"
  comment       = "Managed by Terraform — Tableflow catalog sync"
  force_destroy = true

  depends_on = [databricks_storage_credential.tableflow]
}

resource "databricks_schema" "tableflow" {
  catalog_name  = databricks_catalog.tableflow.name
  name          = confluent_kafka_cluster.dedicated.id
  comment       = "Managed by Terraform — schema for synced Tableflow tables"
  force_destroy = true
}

# ============================================================
# Bastion Host — Public subnet, SSH access
# ============================================================
# Used to manage Kafka topics via PrivateLink (dedicated clusters
# don't expose topic management through the console or public API).
# Sits in the public subnet for SSH access; reaches the Kafka REST
# endpoint via PrivateLink DNS (Route53 private zone).
#
# SSH in:  ssh -i bastion-key.pem ec2-user@<bastion_public_ip>

resource "tls_private_key" "bastion" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "bastion" {
  key_name   = "${var.project_name}-bastion"
  public_key = tls_private_key.bastion.public_key_openssh
}

resource "local_file" "bastion_key" {
  content         = tls_private_key.bastion.private_key_pem
  filename        = "${path.module}/bastion-key.pem"
  file_permission = "0400"
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "bastion" {
  name_prefix = "${var.project_name}-bastion-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-bastion-sg"
    Project = var.project_name
  }
}

resource "aws_instance" "bastion" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = "t3.micro"
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.bastion.id]
  key_name                    = aws_key_pair.bastion.key_name
  associate_public_ip_address = true

  user_data = <<-EOF
    #!/bin/bash
    set -e
    dnf install -y python3.11 python3.11-pip git curl
  EOF

  tags = {
    Name    = "${var.project_name}-bastion"
    Project = var.project_name
  }
}

