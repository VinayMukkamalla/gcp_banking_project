# 1. Raw Dataset - Where the raw GCS data lands unchanged
resource "google_bigquery_dataset" "raw_banking" {
  dataset_id                  = "raw_banking"
  friendly_name               = "Raw Banking Ingestion Layer"
  description                 = "Contains unmodified daily transactions raw data"
  location                    = var.region
  default_table_expiration_ms = 2592000000 # 30 Days expiration to save free tier space
}

# 2. Staging Dataset - For intermediate transformations and deduplication
resource "google_bigquery_dataset" "staging_banking" {
  dataset_id    = "staging_banking"
  friendly_name = "Staging Transformation Layer"
  description   = "Contains intermediate cleaned and deduplicated views and tables"
  location      = var.region
}
