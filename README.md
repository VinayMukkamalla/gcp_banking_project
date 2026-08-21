# Enterprise Banking Ledger & Analytics Platform (GCP)

An end-to-end, serverless cloud data platform built on Google Cloud Platform (GCP) designed to ingest, clean, optimize, and orchestrate high-volume banking transactional ledgers.

## 🏗️ System Architecture
1. **Infrastructure as Code:** Provisioned via **Terraform** (GCS Buckets, Multi-tier BigQuery Datasets).
2. **Ingestion Layer:** Raw pipeline automated via native `gcloud storage` and `bq load` utilities.
3. **Data Quality & Cleansing:** Handled within BigQuery Staging using resilient `SAFE_CAST` and defensive schema casting.
4. **Warehouse Optimization:** Production tables optimized utilizing **Time-Partitioning** and Multi-column **Clustering**.
5. **Orchestration:** Pipeline workflows fully automated via an **Apache Airflow (Cloud Composer)** DAG implementing incremental SQL `MERGE` loads.

## 📊 Performance Benchmarks (Proven Optimization)
A comparative dry-run query analysis filtering transactional ledger dates demonstrated a massive data-scan optimization benefit:
* **Full Table Scan (Unpartitioned Staging):** 190.43 KB scanned.
* **Partitioned Scan (Production Core):** 109.69 KB scanned (**~42% optimization savings**).
