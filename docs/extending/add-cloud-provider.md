# Adding Cloud Providers

This guide explains how to deploy the Tableflow Catalog Sync tool on Azure or GCP. It covers Terraform infrastructure patterns, storage integration, and networking for private clusters.

## Overview

### Current Implementation: AWS

**What's built**:
- Terraform in `terraform/confluent-cloud/`
- AWS resources: VPC, subnets, S3 bucket, IAM roles, PNI ENIs, bastion EC2 instance
- Confluent Cloud: Enterprise cluster with PNI (Private Network Interface)
- Databricks: Unity Catalog with storage credential and external location

**Reference**: All AWS Terraform code is in `terraform/confluent-cloud/main.tf`

### Core Principle: Cloud-Agnostic Sync

**`sync.py` stays cloud-agnostic**:
- No `if cloud == "aws"` logic in Python code
- Storage paths come from Tableflow API (already cloud-aware: `s3://`, `abfss://`, `gs://`)
- `sync.py` passes location strings directly to `CREATE TABLE ... LOCATION`
- All cloud differences are in Terraform and catalog configuration

**What needs cloud-specific implementation**:
1. **Terraform infrastructure** — VPC/VNet/network, storage, IAM/managed identities, private networking
2. **Databricks resources** — Storage credentials differ by cloud (IAM role vs managed identity vs service account)
3. **Bastion configuration** — NGINX proxy endpoint URLs change per cloud

## Terraform Patterns

### Directory Structure

```
terraform/
  confluent-cloud/        # AWS (current implementation)
    main.tf
    variables.tf
    outputs.tf
    vpc.tf
    confluent.tf
    databricks.tf
    bastion.tf
  
  confluent-cloud-azure/  # Azure (to be built)
    main.tf
    variables.tf
    outputs.tf
    vnet.tf
    confluent.tf
    databricks.tf
    bastion.tf
  
  confluent-cloud-gcp/    # GCP (to be built)
    main.tf
    variables.tf
    outputs.tf
    network.tf
    confluent.tf
    databricks.tf
    bastion.tf
```

**Pattern**: Keep structure parallel across clouds. Each cloud has:
- Network setup (VPC/VNet/Network)
- Storage (S3/ADLS/GCS)
- Confluent Cloud resources (cluster, BYOB connector config)
- Databricks resources (metastore, storage credential, catalog)
- Bastion for private networking (optional but recommended for demos)

### Resource Mapping Table

| Component | AWS | Azure | GCP |
|-----------|-----|-------|-----|
| **Network** |
| Virtual network | `aws_vpc` | `azurerm_virtual_network` | `google_compute_network` |
| Subnets | `aws_subnet` | `azurerm_subnet` | `google_compute_subnetwork` |
| Route table | `aws_route_table` | `azurerm_route_table` | `google_compute_route` |
| NAT gateway | `aws_nat_gateway` | `azurerm_nat_gateway` | `google_compute_router_nat` |
| Internet gateway | `aws_internet_gateway` | N/A (implicit in Azure) | `google_compute_router` |
| **Storage** |
| Object storage | `aws_s3_bucket` | `azurerm_storage_account` + `azurerm_storage_container` | `google_storage_bucket` |
| Storage IAM | `aws_iam_role` + `aws_iam_policy` | `azurerm_user_assigned_identity` + `azurerm_role_assignment` | `google_service_account` + `google_project_iam_member` |
| **Private Networking** |
| Confluent private networking | `confluent_access_point` + `confluent_gateway` (PNI for enterprise) | `confluent_private_link_access` + Azure Private Endpoint | `confluent_private_service_connect_access` + GCP PSC |
| VPC/VNet endpoint | ENIs (PNI-managed) | `azurerm_private_endpoint` | `google_compute_forwarding_rule` |
| Private DNS | Manual (no PNI DNS) | `azurerm_private_dns_zone` | `google_dns_managed_zone` |
| **Databricks** |
| Metastore | `databricks_metastore` | `databricks_metastore` (same) | `databricks_metastore` (same) |
| Storage credential | `databricks_storage_credential` (IAM role ARN) | `databricks_storage_credential` (managed identity ID) | `databricks_storage_credential` (service account email) |
| External location | `databricks_external_location` | `databricks_external_location` (same) | `databricks_external_location` (same) |
| **Compute** |
| Bastion instance | `aws_instance` (Amazon Linux 2023) | `azurerm_linux_virtual_machine` | `google_compute_instance` |

### Confluent Terraform Provider Resources

The Confluent Terraform provider is **cloud-agnostic** in resource names, but cloud-specific in configuration:

**Common across all clouds**:
- `confluent_environment`
- `confluent_kafka_cluster`
- `confluent_schema_registry_cluster`
- `confluent_connector` (for BYOB configuration)

**Cloud-specific configuration**:

**AWS (PNI)**:
```hcl
resource "confluent_access_point" "main" {
  aws {
    account = var.aws_account_id
  }
  environment {
    id = confluent_environment.main.id
  }
}

resource "confluent_gateway" "main" {
  aws_egress_private_link_gateway {
    region = var.aws_region
  }
  environment {
    id = confluent_environment.main.id
  }
}

resource "confluent_kafka_cluster" "enterprise" {
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.aws_region
  enterprise {
    encryption_key = confluent_byok_key.main.id  # Optional
  }
  gateway {
    id = confluent_gateway.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
}
```

**Azure (Private Link)**:
```hcl
resource "confluent_private_link_access" "azure" {
  azure {
    subscription = var.azure_subscription_id
  }
  environment {
    id = confluent_environment.main.id
  }
  region = var.azure_region
  cloud  = "AZURE"
}

resource "confluent_kafka_cluster" "dedicated" {
  availability = "SINGLE_ZONE"
  cloud        = "AZURE"
  region       = var.azure_region
  dedicated {
    cku = 1
  }
  network {
    id = confluent_network.azure.id
  }
  environment {
    id = confluent_environment.main.id
  }
}

resource "confluent_network" "azure" {
  cloud           = "AZURE"
  region          = var.azure_region
  connection_types = ["PRIVATELINK"]
  environment {
    id = confluent_environment.main.id
  }
  azure {
    private_link_service_aliases = {
      "az1" = azurerm_private_link_service.confluent.alias
    }
  }
}
```

**GCP (Private Service Connect)**:
```hcl
resource "confluent_private_service_connect_access" "gcp" {
  gcp {
    project = var.gcp_project_id
  }
  environment {
    id = confluent_environment.main.id
  }
  region = var.gcp_region
  cloud  = "GCP"
}

resource "confluent_kafka_cluster" "dedicated" {
  availability = "SINGLE_ZONE"
  cloud        = "GCP"
  region       = var.gcp_region
  dedicated {
    cku = 1
  }
  network {
    id = confluent_network.gcp.id
  }
  environment {
    id = confluent_environment.main.id
  }
}

resource "confluent_network" "gcp" {
  cloud           = "GCP"
  region          = var.gcp_region
  connection_types = ["PRIVATE_SERVICE_CONNECT"]
  environment {
    id = confluent_environment.main.id
  }
}
```

**Reference**: [Confluent Terraform Provider docs](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)

## Storage Integration

### URI Formats by Cloud

| Cloud | Storage URI Format | Example |
|-------|-------------------|---------|
| AWS | `s3://bucket/path` | `s3://my-tableflow-bucket/lkc-12345/orders/` |
| Azure | `abfss://container@account.dfs.core.windows.net/path` | `abfss://tableflow@mystorageacct.dfs.core.windows.net/lkc-12345/orders/` |
| GCP | `gs://bucket/path` | `gs://my-tableflow-bucket/lkc-12345/orders/` |

**How sync.py handles this**:
- Tableflow API returns the storage path in cloud-specific format
- `sync.py` receives it as a string, passes it to `CREATE TABLE ... LOCATION '<path>'`
- No parsing or conversion needed — the catalog (Unity Catalog, Snowflake, etc.) handles the URI

### Tableflow BYOB Configuration

Tableflow BYOB connectors are configured differently per cloud:

**AWS (S3)**:
```hcl
resource "confluent_connector" "byob_s3" {
  config_nonsensitive = {
    "connector.class"          = "TableflowSink"
    "topics"                   = "orders"
    "output.data.format"       = "DELTA"
    "table.format"             = "DELTA"
    "delta.storage.type"       = "S3"
    "s3.bucket.name"           = aws_s3_bucket.tableflow.bucket
    "s3.region"                = var.aws_region
    "provider.integration.id"  = confluent_provider_integration.s3.id
  }
}
```

**Azure (ADLS Gen2)**:
```hcl
resource "confluent_connector" "byob_adls" {
  config_nonsensitive = {
    "connector.class"          = "TableflowSink"
    "topics"                   = "orders"
    "output.data.format"       = "DELTA"
    "table.format"             = "DELTA"
    "delta.storage.type"       = "AZURE_STORAGE"
    "azure.storage.account"    = azurerm_storage_account.tableflow.name
    "azure.storage.container"  = azurerm_storage_container.tableflow.name
    "provider.integration.id"  = confluent_provider_integration.azure.id
  }
}
```

**GCP (GCS)**:
```hcl
resource "confluent_connector" "byob_gcs" {
  config_nonsensitive = {
    "connector.class"          = "TableflowSink"
    "topics"                   = "orders"
    "output.data.format"       = "DELTA"
    "table.format"             = "DELTA"
    "delta.storage.type"       = "GCS"
    "gcs.bucket.name"          = google_storage_bucket.tableflow.name
    "provider.integration.id"  = confluent_provider_integration.gcp.id
  }
}
```

**Provider integration** (cloud IAM for Tableflow to write):
- AWS: IAM role with trust relationship to Confluent
- Azure: Managed identity with Storage Blob Data Contributor role
- GCP: Service account with Storage Object Creator role

Reference: [Confluent Cloud BYOB docs](https://docs.confluent.io/cloud/current/connectors/bring-your-own-storage.html)

### Databricks External Locations

Databricks Unity Catalog needs a **storage credential** to access the BYOB bucket:

**AWS (IAM Role)**:
```hcl
# Create IAM role for Databricks
resource "aws_iam_role" "databricks_storage" {
  name = "databricks-storage-access"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      }
      Action = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "sts:ExternalId" = var.databricks_account_id
        }
      }
    }]
  })
}

# Grant S3 read access
resource "aws_iam_role_policy" "databricks_s3_read" {
  role = aws_iam_role.databricks_storage.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.tableflow.arn,
        "${aws_s3_bucket.tableflow.arn}/*"
      ]
    }]
  })
}

# Create Databricks storage credential
resource "databricks_storage_credential" "tableflow" {
  name = "tableflow_s3_credential"
  aws_iam_role {
    role_arn = aws_iam_role.databricks_storage.arn
  }
}

# Create external location
resource "databricks_external_location" "tableflow" {
  name            = "tableflow_bucket"
  url             = "s3://${aws_s3_bucket.tableflow.bucket}/"
  credential_name = databricks_storage_credential.tableflow.id
  read_only       = true
}
```

**Azure (Managed Identity)**:
```hcl
# Create managed identity
resource "azurerm_user_assigned_identity" "databricks" {
  name                = "databricks-storage-access"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
}

# Grant Storage Blob Data Contributor role
resource "azurerm_role_assignment" "databricks_storage" {
  scope                = azurerm_storage_account.tableflow.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.databricks.principal_id
}

# Create Databricks storage credential
resource "databricks_storage_credential" "tableflow" {
  name = "tableflow_adls_credential"
  azure_managed_identity {
    access_connector_id = var.databricks_access_connector_id
    managed_identity_id = azurerm_user_assigned_identity.databricks.id
  }
}

# Create external location
resource "databricks_external_location" "tableflow" {
  name            = "tableflow_adls"
  url             = "abfss://${azurerm_storage_container.tableflow.name}@${azurerm_storage_account.tableflow.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.tableflow.id
  read_only       = true
}
```

**GCP (Service Account)**:
```hcl
# Create service account
resource "google_service_account" "databricks" {
  account_id   = "databricks-storage-access"
  display_name = "Databricks Storage Access"
}

# Grant Storage Object Viewer role
resource "google_project_iam_member" "databricks_storage" {
  project = var.gcp_project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.databricks.email}"
}

# Grant bucket-level access
resource "google_storage_bucket_iam_member" "databricks" {
  bucket = google_storage_bucket.tableflow.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.databricks.email}"
}

# Create Databricks storage credential
resource "databricks_storage_credential" "tableflow" {
  name = "tableflow_gcs_credential"
  gcp_service_account {
    email = google_service_account.databricks.email
  }
}

# Create external location
resource "databricks_external_location" "tableflow" {
  name            = "tableflow_gcs"
  url             = "gs://${google_storage_bucket.tableflow.name}/"
  credential_name = databricks_storage_credential.tableflow.id
  read_only       = true
}
```

Reference: [Databricks Unity Catalog external locations](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html)

## Networking for Private Clusters

### Private Networking Concepts

| Cloud | Technology | Key Resources |
|-------|------------|---------------|
| AWS | **PNI** (Private Network Interface) for enterprise clusters | ENIs in your VPC subnets, no private DNS |
| AWS | **PrivateLink** for dedicated clusters | VPC endpoint + Route53 private hosted zone |
| Azure | **Azure Private Link** | Private endpoint + private DNS zone |
| GCP | **Private Service Connect** | Service attachment + forwarding rule |

**Common pattern across all clouds**:
1. Confluent creates a service endpoint in their network
2. You create a connection from your VPC/VNet to that endpoint
3. Traffic stays on the cloud provider's backbone (never crosses public internet)
4. DNS resolution points to private IPs

### AWS (Current Implementation)

**Architecture**:
- Enterprise cluster with PNI
- `confluent_gateway` + `confluent_access_point` create ENIs in your private subnets
- No private DNS provided — NGINX proxy on bastion handles routing
- Bastion in public subnet proxies Kafka traffic through PNI

**Key resources**:
```hcl
resource "confluent_access_point" "main" {
  aws {
    account = var.aws_account_id
  }
  environment {
    id = confluent_environment.main.id
  }
}

resource "confluent_gateway" "main" {
  aws_egress_private_link_gateway {
    region = var.aws_region
  }
  environment {
    id = confluent_environment.main.id
  }
}

# Bastion NGINX proxy configuration
resource "aws_instance" "bastion" {
  # ... basic config ...
  
  user_data = <<-EOF
    #!/bin/bash
    # Install NGINX
    dnf install -y nginx
    
    # Configure NGINX stream proxy for Kafka
    cat > /etc/nginx/nginx.conf <<'NGINX'
    stream {
      server {
        listen 9092;
        proxy_pass ${confluent_kafka_cluster.enterprise.bootstrap_endpoint};
      }
    }
    NGINX
    
    systemctl enable nginx
    systemctl start nginx
  EOF
}
```

**Endpoints**:
- Bootstrap: Extract from `confluent_kafka_cluster.enterprise.endpoints` filtered by `access_point_id`
- REST: Also in `endpoints` list

Reference: [Confluent PNI example](https://github.com/confluentinc/terraform-provider-confluent/tree/master/examples/configurations/enterprise-pni-aws-kafka-rbac)

### Azure (To Be Built)

**Architecture**:
- Dedicated cluster with Azure Private Link
- `confluent_private_link_access` resource
- Azure Private Endpoint in your VNet
- Private DNS zone for name resolution

**Key resources**:
```hcl
# Confluent side
resource "confluent_private_link_access" "azure" {
  azure {
    subscription = var.azure_subscription_id
  }
  environment {
    id = confluent_environment.main.id
  }
  region = var.azure_region
  cloud  = "AZURE"
}

resource "confluent_network" "azure" {
  cloud           = "AZURE"
  region          = var.azure_region
  connection_types = ["PRIVATELINK"]
  environment {
    id = confluent_environment.main.id
  }
  azure {
    private_link_service_aliases = {
      "az1" = azurerm_private_link_service.confluent.alias
    }
  }
}

# Azure side
resource "azurerm_private_endpoint" "confluent_kafka" {
  name                = "confluent-kafka-pe"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.private.id
  
  private_service_connection {
    name                           = "confluent-kafka-connection"
    private_connection_resource_id = confluent_private_link_access.azure.id
    is_manual_connection           = false
  }
}

# Private DNS zone for Kafka
resource "azurerm_private_dns_zone" "confluent" {
  name                = "privatelink.confluent.cloud"
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_private_dns_zone_virtual_network_link" "confluent" {
  name                  = "confluent-vnet-link"
  resource_group_name   = azurerm_resource_group.main.name
  private_dns_zone_name = azurerm_private_dns_zone.confluent.name
  virtual_network_id    = azurerm_virtual_network.main.id
}

# Bastion (optional for demos)
resource "azurerm_linux_virtual_machine" "bastion" {
  name                = "bastion"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  size                = "Standard_B2s"
  
  network_interface_ids = [azurerm_network_interface.bastion.id]
  
  # Similar NGINX proxy configuration as AWS
  custom_data = base64encode(<<-EOF
    #!/bin/bash
    # NGINX setup for Azure Private Endpoint
    # ...
  EOF
  )
}
```

Reference: [Confluent Azure Private Link docs](https://docs.confluent.io/cloud/current/networking/private-links/azure-privatelink.html)

### GCP (To Be Built)

**Architecture**:
- Dedicated cluster with Private Service Connect
- `confluent_private_service_connect_access` resource
- GCP forwarding rule and service attachment
- Cloud DNS private zone for name resolution

**Key resources**:
```hcl
# Confluent side
resource "confluent_private_service_connect_access" "gcp" {
  gcp {
    project = var.gcp_project_id
  }
  environment {
    id = confluent_environment.main.id
  }
  region = var.gcp_region
  cloud  = "GCP"
}

resource "confluent_network" "gcp" {
  cloud           = "GCP"
  region          = var.gcp_region
  connection_types = ["PRIVATE_SERVICE_CONNECT"]
  environment {
    id = confluent_environment.main.id
  }
}

# GCP side
resource "google_compute_forwarding_rule" "confluent_kafka" {
  name                  = "confluent-kafka-psc"
  region                = var.gcp_region
  load_balancing_scheme = ""
  target                = confluent_private_service_connect_access.gcp.service_attachment
  network               = google_compute_network.main.id
  ip_address            = google_compute_address.confluent_kafka.address
}

# Private DNS zone
resource "google_dns_managed_zone" "confluent" {
  name        = "confluent-private"
  dns_name    = "privatelink.confluent.cloud."
  visibility  = "private"
  
  private_visibility_config {
    networks {
      network_url = google_compute_network.main.id
    }
  }
}

# Bastion (optional for demos)
resource "google_compute_instance" "bastion" {
  name         = "bastion"
  machine_type = "e2-medium"
  zone         = "${var.gcp_region}-a"
  
  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
    }
  }
  
  network_interface {
    network    = google_compute_network.main.id
    subnetwork = google_compute_subnetwork.public.id
    
    access_config {
      # Ephemeral public IP for SSH
    }
  }
  
  metadata_startup_script = <<-EOF
    #!/bin/bash
    # NGINX setup for GCP Private Service Connect
    # ...
  EOF
}
```

Reference: [Confluent GCP Private Service Connect docs](https://docs.confluent.io/cloud/current/networking/private-service-connect/index.html)

### Bastion Considerations

**Why a bastion is needed**:
- Kafka protocol access for topic deletion (PNI/Private Link doesn't expose public REST endpoints)
- Debugging and manual operations
- Demo purposes (show private connectivity)

**NGINX proxy adaptation**:

**AWS (current)**:
```nginx
stream {
  server {
    listen 9092;
    proxy_pass bootstrap-endpoint.confluent.cloud:9092;
  }
}
```

**Azure/GCP (adapt this)**:
- Private endpoint creates a private IP in your VNet/VPC
- NGINX proxies to that private IP instead of public endpoint
- DNS resolution handled by private DNS zone

**Alternative**: SSH tunnel instead of NGINX:
```bash
ssh -L 9092:private-kafka-ip:9092 bastion-ip
```

### Confluent Cloud Control Plane

**Important**: The Confluent Cloud API (`api.confluent.cloud`) is **public-only on all clouds**. There is no private networking option for the control plane.

This means:
- Tableflow API calls (topic discovery) go over public internet (HTTPS)
- Stream Catalog GraphQL API calls (tag fetch) go over public internet (HTTPS)
- This is true for all customers, even those with fully private data planes

**Why this is acceptable**:
- Control plane only carries metadata (topic names, S3 paths, tags)
- No customer data or Kafka messages are transmitted
- Same API calls a human makes by logging into the Confluent Cloud console

## Configuration Variables

### Environment Variables That Stay the Same

These work across all clouds without changes:

```bash
# Confluent Cloud
CONFLUENT_API_KEY=<tableflow-api-key>
CONFLUENT_API_SECRET=<tableflow-api-secret>
CONFLUENT_CLUSTER_ID=lkc-xxxxx
CONFLUENT_ENVIRONMENT_ID=env-xxxxx

# Schema Registry (for tag sync)
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.provider.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<sr-api-key>
SCHEMA_REGISTRY_API_SECRET=<sr-api-secret>

# Databricks
DATABRICKS_HOST=https://dbc-xxxxx.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=<warehouse-id>
TARGET_CATALOG=tableflow_sync
TARGET_SCHEMA=default

# Tag sync (optional)
SYNC_TAGS=true
```

### What Changes by Cloud

**Databricks authentication**:
- **AWS**: Usually PAT (token), sometimes service principal
- **Azure**: Usually service principal with Azure AD OAuth
- **GCP**: Usually service principal with Google OAuth

**Storage paths** (automatically handled by Tableflow API):
- AWS: `s3://bucket/path`
- Azure: `abfss://container@account.dfs.core.windows.net/path`
- GCP: `gs://bucket/path`

**Terraform variables**:
- `aws_region` → `azure_region` → `gcp_region`
- `aws_account_id` → `azure_subscription_id` → `gcp_project_id`
- Cloud-specific storage names (bucket vs storage account vs bucket)

## Next Steps

- Review [best-practices.md](best-practices.md) for design principles on keeping sync.py cloud-agnostic
- Study the AWS Terraform in `terraform/confluent-cloud/` as a reference
- Consult Confluent docs for Azure Private Link or GCP Private Service Connect
- Consult Databricks docs for Unity Catalog on Azure or GCP
- Test with a pilot deployment before productionizing
