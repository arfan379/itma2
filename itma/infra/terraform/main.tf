# ITMA - Terraform Target Infrastructure Specification
# Deployment target: Google Cloud Platform (Region: asia-southeast2 Jakarta)

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type        = string
  description = "Cloud project ID"
  default     = "itma-analytics-prod"
}

variable "region" {
  type        = string
  description = "Deployment region (Jakarta)"
  default     = "asia-southeast2"
}

variable "db_tier" {
  type        = string
  description = "Cloud SQL database tier"
  default     = "db-custom-4-16384"
}

# 1. VPC Network for Analytics Platform
resource "google_compute_network" "itma_vpc" {
  name                    = "itma-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "itma_subnet" {
  name          = "itma-subnet-${var.region}"
  ip_cidr_range = "10.10.0.0/20"
  region        = var.region
  network       = google_compute_network.itma_vpc.id
}

# 2. Cloud Storage Bucket for GTFS Raw Archives
resource "google_storage_bucket" "gtfs_archive_bucket" {
  name                        = "${var.project_id}-gtfs-archives"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# 3. Managed Cloud SQL PostgreSQL (TimescaleDB Compatible)
resource "google_sql_database_instance" "itma_postgres" {
  name             = "itma-postgres-instance"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = var.db_tier
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.itma_vpc.id
    }
    database_flags {
      name  = "shared_preload_libraries"
      value = "timescaledb"
    }
    database_flags {
      name  = "max_connections"
      value = "200"
    }
    backup_configuration {
      enabled    = true
      start_time = "02:00" # 02:00 UTC / 09:00 WIB
    }
  }
}

resource "google_sql_database" "itma_db" {
  name     = "itma_db"
  instance = google_sql_database_instance.itma_postgres.name
}

# 4. Outputs
output "vpc_id" {
  description = "VPC Network ID"
  value       = google_compute_network.itma_vpc.id
}

output "storage_bucket_url" {
  description = "GCS GTFS Archive URL"
  value       = google_storage_bucket.gtfs_archive_bucket.url
}

output "database_connection_name" {
  description = "Cloud SQL Connection Name"
  value       = google_sql_database_instance.itma_postgres.connection_name
}
