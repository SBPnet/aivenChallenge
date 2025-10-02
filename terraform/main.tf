
# Terraform configuration
# See https://aiven.io/docs/tools/terraform for more information

terraform {
    required_version = ">=0.13"
    required_providers {
        aiven = {
            source  = "aiven/aiven"
            version = ">=4.0.0, <5.0.0"
        }
    }
}

# Aiven API token
variable "AIVEN_API_TOKEN" {
    type = string
    description = "Aiven API token"
}

# Aiven project name
variable "AIVEN_PROJECT" {
    type = string
    description = "Aiven project name"
}

# Aiven provider
provider "aiven" {
    api_token = var.AIVEN_API_TOKEN
}

# Aiven project
data "aiven_project" "challenge" {
  project = var.AIVEN_PROJECT
}

# Aiven Kafka resource
resource "aiven_kafka" "challenge-kafka" {
  project                 = var.AIVEN_PROJECT
  cloud_name              = "do-tor"
  plan                    = "startup-4"  # Adjust plan as needed
  service_name            = "challenge-kafka"
  maintenance_window_dow  = "monday"
  maintenance_window_time = "10:00:00"
}

# Aiven Kafka topic
resource "aiven_kafka_topic" "website-events" {
  project      = aiven_kafka.challenge-kafka.project
  service_name = aiven_kafka.challenge-kafka.service_name
  topic_name   = "website-events"
  partitions   = 3
  replication  = 3

  depends_on = [aiven_kafka.challenge-kafka]
}

# Aiven PostgreSQL resource
resource "aiven_pg" "challenge-postgres" {
  project                 = var.AIVEN_PROJECT
  cloud_name              = "do-tor"
  plan                    = "hobbyist"  # Adjust plan as needed
  service_name            = "challenge-postgres"
  maintenance_window_dow  = "monday"
  maintenance_window_time = "10:00:00"
}

# Aiven OpenSearch resource
resource "aiven_opensearch" "challenge-opensearch" {
  project                 = var.AIVEN_PROJECT
  cloud_name              = "do-tor"
  plan                    = "hobbyist"  # Adjust plan as needed
  service_name            = "challenge-opensearch"
  maintenance_window_dow  = "monday"
  maintenance_window_time = "10:00:00"
}

# Aiven Kafka bootstrap
output "challenge_kafka_bootstrap" {
  value = format(
    "%s:%d",
    aiven_kafka.challenge-kafka.service_host,
    aiven_kafka.challenge-kafka.service_port,
  )
}

# Aiven Kafka topic
output "website_events_topic" {
  value = aiven_kafka_topic.website-events.topic_name
}

# Aiven Kafka access cert
output "challenge_kafka_access_cert" {
  value     = aiven_kafka.challenge-kafka.kafka[0].access_cert
  sensitive = true
}

# Aiven Kafka access key
output "challenge_kafka_access_key" {
  value     = aiven_kafka.challenge-kafka.kafka[0].access_key
  sensitive = true
}

# Aiven Kafka CA cert
output "challenge_kafka_ca_cert" {
  value     = data.aiven_project.challenge.ca_cert
  sensitive = true
}

# Aiven PostgreSQL service URI
output "challenge_postgres_service_uri" {
  value     = aiven_pg.challenge-postgres.service_uri
  sensitive = true
}

# Aiven OpenSearch service URI
output "challenge_opensearch_service_uri" {
  value     = aiven_opensearch.challenge-opensearch.service_uri
  sensitive = true
}