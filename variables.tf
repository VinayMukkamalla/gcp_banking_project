variable "project_id" {
  type        = string
  description = "The ID of your GCP Project"
  default     = "gcp-data-engineer-501607" 
}

variable "region" {
  type    = string
  default = "asia-south1" # This is the Mumbai region, closest to you!
}

bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE staging_banking.transactions_cleaned AS 
SELECT DISTINCT
  SAFE_CAST(transaction_id AS STRING) AS transaction_id,
  SAFE_CAST(account_id AS STRING) AS account_id,
  SAFE_CAST(amount AS FLOAT64) AS amount,
  UPPER(TRIM(transaction_type)) AS transaction_type,
  PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", timestamp) AS transaction_timestamp,
  UPPER(TRIM(location)) AS location,
  CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM 
  `gcp-data-engineer-501607.raw_banking.transactions`
WHERE 
  transaction_id IS NOT NULL;'
