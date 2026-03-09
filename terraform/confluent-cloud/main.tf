# ============================================================
# Confluent Cloud + AWS Networking — Self-contained provisioning
# ============================================================
# Provisions everything needed on the Confluent Cloud side:
# - AWS VPC with private/public subnets, S3 endpoint, bastion host
# - Confluent Cloud environment + enterprise cluster (PNI private networking)
# - PNI gateway, access point, ENIs + permissions
# - Service account, API keys, role bindings
# - BYOB S3 bucket + Confluent provider integration (IAM)
#
# Enterprise clusters use Private Network Interface (PNI) instead of
# PrivateLink. PNI places ENIs in the customer's VPC subnets; Confluent
# attaches to them for private Kafka traffic. No VPC endpoint or Route53
# private zone is needed — an NGINX proxy on the bastion handles DNS.
#
# For dedicated clusters with PrivateLink, see the git history or
# Confluent docs: https://docs.confluent.io/cloud/current/networking/private-links/aws-privatelink.html
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

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "zone-type"
    values = ["availability-zone"]
  }
}

locals {
  # Pick the first 3 available AZ IDs for PNI ENI placement
  az_ids               = slice(data.aws_availability_zones.available.zone_ids, 0, 3)
  vpc_cidr             = "10.0.0.0/16"
  private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
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
# Private subnets use availability_zone_id (AZ ID like use1-az1) for PNI —
# AZ IDs are consistent across AWS accounts, unlike AZ names.
resource "aws_subnet" "private" {
  count                = length(local.private_subnet_cidrs)
  vpc_id               = aws_vpc.main.id
  cidr_block           = local.private_subnet_cidrs[count.index]
  availability_zone_id = local.az_ids[count.index]

  tags = {
    Name    = "${var.project_name}-private-${count.index}"
    Project = var.project_name
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = local.public_subnet_cidr
  availability_zone_id    = local.az_ids[0]
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
# Confluent Cloud — Environment + PNI Network + Enterprise Cluster
# ============================================================

resource "confluent_environment" "main" {
  display_name = var.project_name

  lifecycle {
    prevent_destroy = false
  }
}

# ---------- PNI Gateway ----------
# A gateway represents a connectivity type. For PNI, it specifies the
# region and AZs where ENIs will be placed.
resource "confluent_gateway" "pni" {
  display_name = "${var.project_name}-pni-gateway"

  environment {
    id = confluent_environment.main.id
  }

  aws_private_network_interface_gateway {
    region = var.aws_region
    zones  = local.az_ids
  }
}

# ---------- PNI Security Group ----------
# Controls traffic on the ENIs. Confluent docs recommend blocking all egress
# for production. For this demo we allow egress for dependency downloads.
resource "aws_security_group" "pni" {
  name_prefix = "${var.project_name}-pni-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
    description = "Kafka broker access"
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
    description = "HTTPS access"
  }

  # For production: set egress = [] to block all outbound from Confluent.
  # For demo: allow outbound so bastion/EC2 dependencies can download.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-pni-sg"
    Project = var.project_name
  }
}

# ---------- PNI ENIs ----------
# Create ENIs in each private subnet. Confluent will attach to these ENIs
# for private Kafka traffic. Minimum 17 per subnet (51 total) per docs.
resource "aws_network_interface" "pni" {
  count = length(local.az_ids) * var.num_eni_per_subnet

  subnet_id       = aws_subnet.private[floor(count.index / var.num_eni_per_subnet)].id
  security_groups = [aws_security_group.pni.id]

  private_ips = [
    cidrhost(
      aws_subnet.private[floor(count.index / var.num_eni_per_subnet)].cidr_block,
      10 + (count.index % var.num_eni_per_subnet) + 1
    )
  ]

  description = "Confluent PNI-sub-${floor(count.index / var.num_eni_per_subnet)}-eni-${(count.index % var.num_eni_per_subnet) + 1}"

  tags = {
    Name    = "${var.project_name}-pni-sub-${floor(count.index / var.num_eni_per_subnet)}-eni-${(count.index % var.num_eni_per_subnet) + 1}"
    Project = var.project_name
  }

  depends_on = [confluent_gateway.pni]
}

# Grant Confluent's AWS account permission to attach to our ENIs
resource "aws_network_interface_permission" "pni" {
  count = length(aws_network_interface.pni)

  network_interface_id = aws_network_interface.pni[count.index].id
  permission           = "INSTANCE-ATTACH"
  aws_account_id       = confluent_gateway.pni.aws_private_network_interface_gateway[0].account
}

# ---------- PNI Access Point ----------
# Connects the ENIs to the PNI gateway, establishing the private path.
resource "confluent_access_point" "pni" {
  display_name = "${var.project_name}-pni-access-point"

  environment {
    id = confluent_environment.main.id
  }

  gateway {
    id = confluent_gateway.pni.id
  }

  aws_private_network_interface {
    network_interfaces = aws_network_interface.pni[*].id
    account            = data.aws_caller_identity.current.account_id
  }

  depends_on = [aws_network_interface_permission.pni]
}

# ---------- Enterprise Kafka Cluster ----------
# Enterprise clusters don't take a network{} block — they associate with
# the PNI access point implicitly via the environment + region.
resource "confluent_kafka_cluster" "enterprise" {
  display_name = "${var.project_name}-cluster"
  availability = "HIGH"
  cloud        = "AWS"
  region       = var.aws_region

  enterprise {}

  environment {
    id = confluent_environment.main.id
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_access_point.pni]
}

# PNI-specific endpoints (filtered by access point ID)
locals {
  pni_bootstrap_endpoint = [
    for ep in confluent_kafka_cluster.enterprise.endpoints :
    ep.bootstrap_endpoint if ep.access_point_id == confluent_access_point.pni.id
  ][0]
  pni_rest_endpoint = [
    for ep in confluent_kafka_cluster.enterprise.endpoints :
    ep.rest_endpoint if ep.access_point_id == confluent_access_point.pni.id
  ][0]
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
  crn_pattern = confluent_kafka_cluster.enterprise.rbac_crn
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
    id          = confluent_kafka_cluster.enterprise.id
    api_version = confluent_kafka_cluster.enterprise.api_version
    kind        = confluent_kafka_cluster.enterprise.kind

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

  depends_on = [confluent_kafka_cluster.enterprise]
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
  read_only       = true
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
  name          = confluent_kafka_cluster.enterprise.id
  comment       = "Managed by Terraform — schema for synced Tableflow tables"
  force_destroy = true
}

# ============================================================
# Serverless PrivateLink Attachment — Console / Control-Plane Access
# ============================================================
# Enterprise clusters with PNI don't expose connectors, topics, or
# Tableflow config in the Confluent Cloud console unless a separate
# Serverless PrivateLink Attachment is provisioned. This creates a
# VPC endpoint + Route53 DNS so the console (and CLI) can reach
# the cluster's control-plane services.

resource "confluent_private_link_attachment" "console" {
  cloud        = "AWS"
  region       = var.aws_region
  display_name = "${var.project_name}-console-platt"

  environment {
    id = confluent_environment.main.id
  }
}

# Security group for the PrivateLink VPC endpoint
resource "aws_security_group" "privatelink" {
  name_prefix = "${var.project_name}-platt-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
    description = "HTTPS from VPC"
  }

  tags = {
    Name    = "${var.project_name}-platt-sg"
    Project = var.project_name
  }
}

# VPC endpoint targeting the PrivateLink Attachment's service name
resource "aws_vpc_endpoint" "confluent_console" {
  vpc_id             = aws_vpc.main.id
  service_name       = confluent_private_link_attachment.console.aws[0].vpc_endpoint_service_name
  vpc_endpoint_type  = "Interface"
  security_group_ids = [aws_security_group.privatelink.id]
  subnet_ids         = aws_subnet.private[*].id

  private_dns_enabled = false

  tags = {
    Name    = "${var.project_name}-platt-vpce"
    Project = var.project_name
  }
}

# Route53 private zone for the PrivateLink DNS domain
resource "aws_route53_zone" "confluent_platt" {
  name = confluent_private_link_attachment.console.dns_domain

  vpc {
    vpc_id = aws_vpc.main.id
  }

  tags = {
    Name    = "${var.project_name}-platt-dns"
    Project = var.project_name
  }
}

# Wildcard CNAME record pointing to the VPC endpoint DNS
resource "aws_route53_record" "confluent_platt_wildcard" {
  zone_id = aws_route53_zone.confluent_platt.zone_id
  name    = "*.${confluent_private_link_attachment.console.dns_domain}"
  type    = "CNAME"
  ttl     = 60
  records = [aws_vpc_endpoint.confluent_console.dns_entry[0]["dns_name"]]
}

# Tell Confluent about the VPC endpoint connection
resource "confluent_private_link_attachment_connection" "console" {
  display_name = "${var.project_name}-console-plattc"

  environment {
    id = confluent_environment.main.id
  }

  aws {
    vpc_endpoint_id = aws_vpc_endpoint.confluent_console.id
  }

  private_link_attachment {
    id = confluent_private_link_attachment.console.id
  }
}

# ============================================================
# Bastion Host — Public subnet, SSH + NGINX proxy
# ============================================================
# Used to manage Kafka topics via PNI (enterprise clusters don't expose
# topic management through the console or public API without a proxy).
# Sits in the public subnet for SSH access; runs an NGINX stream proxy
# that forwards Kafka (9092) and HTTPS (443) traffic through the PNI ENIs.
#
# SSH in:  ssh -i bastion-key.pem ec2-user@<bastion_public_ip>
# Kafka:   Clients on the bastion use localhost:9092 as bootstrap

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

    # Install dependencies
    dnf install -y python3.11 python3.11-pip git curl nginx nginx-mod-stream

    # Symlink so `python3` and `pip` work without version suffix
    alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
    ln -sf /usr/bin/pip3.11 /usr/local/bin/pip
    ln -sf /usr/bin/pip3.11 /usr/local/bin/pip3

    # --- NGINX stream proxy for PNI ---
    # PNI does not provide private DNS. This proxy forwards Kafka (9092)
    # and HTTPS (443) traffic using SNI passthrough so that clients on
    # this bastion can reach the enterprise cluster through the PNI ENIs.
    # See: https://docs.confluent.io/cloud/current/networking/ccloud-console-access.html

    # Find the stream module
    if [ -f /usr/lib64/nginx/modules/ngx_stream_module.so ]; then
      MODULE_PATH="/usr/lib64/nginx/modules/ngx_stream_module.so"
    elif [ -f /usr/lib/nginx/modules/ngx_stream_module.so ]; then
      MODULE_PATH="/usr/lib/nginx/modules/ngx_stream_module.so"
    else
      echo "ERROR: ngx_stream_module.so not found" >> /var/log/user-data.log
      exit 1
    fi

    # AWS VPC DNS resolver
    RESOLVER="169.254.169.253"

    cat > /etc/nginx/nginx.conf <<NGINXCONF
    load_module $MODULE_PATH;

    events {}
    stream {
      map \$ssl_preread_server_name \$targetBackend {
        default \$ssl_preread_server_name;
      }

      server {
        listen 9092;
        proxy_connect_timeout 1s;
        proxy_timeout 7200s;
        resolver $RESOLVER;
        proxy_pass \$targetBackend:9092;
        ssl_preread on;
      }

      server {
        listen 443;
        proxy_connect_timeout 1s;
        proxy_timeout 7200s;
        resolver $RESOLVER;
        proxy_pass \$targetBackend:443;
        ssl_preread on;
      }

      log_format stream_routing '[\$time_local] remote=\$remote_addr '
                                'sni="\$ssl_preread_server_name" '
                                'upstream="\$upstream_addr" '
                                '\$protocol \$status \$bytes_sent \$bytes_received '
                                '\$session_time';
      access_log /var/log/nginx/stream-access.log stream_routing;
    }
    NGINXCONF

    # Start and enable NGINX
    systemctl restart nginx
    systemctl enable nginx
    echo "NGINX PNI proxy started" >> /var/log/user-data.log
  EOF

  tags = {
    Name    = "${var.project_name}-bastion"
    Project = var.project_name
  }
}
