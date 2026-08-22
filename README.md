# Enterprise Banking Ledger & Analytics Platform

An end-to-end **GCP data engineering project** that demonstrates the design and implementation of a scalable banking transaction data platform using **Google Cloud Platform, BigQuery, Cloud Storage, Cloud Composer/Airflow, Terraform, Python, and SQL**.

The project follows a production-style **Raw → Staging → Production** architecture and demonstrates data ingestion, validation, transformation, incremental loading, orchestration, data quality, and BigQuery performance optimization.

---

## 📌 Project Overview

This project simulates an enterprise banking transaction data platform where transaction data is:

1. Generated as structured transaction data.
2. Uploaded to **Google Cloud Storage (GCS)**.
3. Loaded into **BigQuery** through a multi-layer warehouse architecture.
4. Validated and transformed in the staging layer.
5. Incrementally loaded into production tables using **BigQuery MERGE**.
6. Orchestrated using **Cloud Composer / Apache Airflow**.
7. Optimized using **partitioning and clustering** for efficient analytical queries.

The project is designed to demonstrate practical data engineering concepts used in enterprise banking and financial-services environments.

---

## 🎯 Objectives

* Build an end-to-end GCP data pipeline.
* Implement a scalable BigQuery data warehouse.
* Separate raw, staging, and production data layers.
* Automate infrastructure provisioning using Terraform.
* Orchestrate data pipelines using Apache Airflow / Cloud Composer.
* Implement incremental data loading using SQL `MERGE`.
* Implement schema validation and data quality checks.
* Handle malformed data using `SAFE_CAST`.
* Improve BigQuery query performance using partitioning and clustering.
* Reduce unnecessary data processing through incremental processing.
* Implement production-style error handling and pipeline reliability practices.

---

## 🛠️ Tech Stack

| Category               | Technologies                                     |
| ---------------------- | ------------------------------------------------ |
| Cloud Platform         | Google Cloud Platform (GCP)                      |
| Data Warehouse         | BigQuery                                         |
| Cloud Storage          | Google Cloud Storage (GCS)                       |
| Orchestration          | Cloud Composer / Apache Airflow                  |
| Data Processing        | Python, SQL                                      |
| Infrastructure as Code | Terraform                                        |
| Data Loading           | BigQuery Load Jobs, SQL MERGE                    |
| Data Quality           | SAFE_CAST, Schema Validation, Reconciliation     |
| Optimization           | Partitioning, Clustering, Incremental Processing |
| Version Control        | Git, GitHub                                      |

---

## 🏗️ System Architecture

```text
                     Banking Transaction Data
                              │
                              ▼
                  ┌─────────────────────┐
                  │ Python Data         │
                  │ Generation          │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Google Cloud        │
                  │ Storage (GCS)       │
                  │ RAW DATA            │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ BigQuery            │
                  │ STAGING             │
                  │                     │
                  │ • Schema Validation │
                  │ • SAFE_CAST         │
                  │ • Data Quality      │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ BigQuery            │
                  │ PRODUCTION          │
                  │                     │
                  │ • MERGE             │
                  │ • Partitioning      │
                  │ • Clustering        │
                  └──────────┬──────────┘
                             │
                             ▼
                     Analytical Queries


             ┌─────────────────────────────┐
             │ Cloud Composer / Airflow    │
             │                             │
             │ • Scheduling                │
             │ • Dependencies              │
             │ • Retries                   │
             │ • Failure Handling          │
             │ • Pipeline Orchestration    │
             └─────────────────────────────┘


             ┌─────────────────────────────┐
             │ Terraform                   │
             │ Infrastructure as Code      │
             │                             │
             │ • GCS                       │
             │ • BigQuery                  │
             │ • GCP Configuration         │
             └─────────────────────────────┘
```

---

## 🔄 End-to-End Data Flow

```text
Transaction Data
       │
       ▼
Python Data Generation
       │
       ▼
Google Cloud Storage
       │
       ▼
BigQuery RAW Layer
       │
       ▼
BigQuery STAGING Layer
       │
       ├── Schema Validation
       ├── SAFE_CAST
       ├── Data Validation
       └── Data Quality Checks
       │
       ▼
Incremental MERGE
       │
       ▼
BigQuery PRODUCTION Layer
       │
       ├── Partitioning
       ├── Clustering
       └── Optimized Tables
       │
       ▼
Analytics / Reporting
```

---

## 🗄️ BigQuery Data Warehouse Design

The warehouse follows a three-layer architecture.

### 1. RAW Layer

The raw layer stores incoming transaction data with minimal transformation.

**Purpose:**

* Preserve source data.
* Maintain an ingestion layer.
* Provide traceability.
* Allow reprocessing when required.

---

### 2. STAGING Layer

The staging layer performs validation and transformation before data reaches production.

**Key operations:**

* Schema validation.
* Data type conversion.
* `SAFE_CAST` handling.
* Data quality validation.
* Invalid data isolation.
* Transformation and standardization.

Using `SAFE_CAST` prevents malformed values from causing the entire transformation query to fail.

Example:

```sql
SAFE_CAST(transaction_amount AS NUMERIC)
```

---

### 3. PRODUCTION Layer

The production layer contains cleaned and business-ready data.

**Key features:**

* Incremental loading.
* SQL `MERGE`.
* Time-based partitioning.
* Multi-column clustering.
* Optimized analytical queries.

---

## 🔄 Incremental Loading

The project uses BigQuery `MERGE` statements to implement incremental data processing.

Instead of processing the complete dataset during every pipeline execution, new and changed records are identified and applied to the production tables.

Example:

```sql
MERGE production.transactions AS target
USING staging.transactions AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
  UPDATE SET
    transaction_amount = source.transaction_amount,
    transaction_status = source.transaction_status

WHEN NOT MATCHED THEN
  INSERT (
    transaction_id,
    transaction_amount,
    transaction_status
  )
  VALUES (
    source.transaction_id,
    source.transaction_amount,
    source.transaction_status
  );
```

### Benefits

* Avoids unnecessary full-table processing.
* Reduces processing volume.
* Improves pipeline efficiency.
* Supports scalable incremental data ingestion.
* Helps reduce unnecessary BigQuery data scanning.

---

## 🔍 Data Quality & Validation

The staging layer implements defensive data validation before records are promoted to production.

### Techniques Used

* Schema validation.
* `SAFE_CAST` for safe data type conversion.
* Null handling.
* Data validation.
* Duplicate detection.
* Record-count validation.
* Load-status validation.
* Source-to-target reconciliation.

The objective is to prevent malformed or inconsistent records from affecting downstream production datasets.

---

## ⚙️ Cloud Composer / Apache Airflow

Apache Airflow is used to orchestrate the data pipeline.

The DAG manages:

* Data ingestion.
* Transformation.
* Task dependencies.
* Scheduling.
* Retries.
* Failure handling.
* Pipeline execution monitoring.

### Example Pipeline Flow

```text
Start
  │
  ▼
Check Source Data
  │
  ▼
Load Raw Data
  │
  ▼
Validate Schema
  │
  ▼
Transform Staging Data
  │
  ▼
Run Data Quality Checks
  │
  ▼
Execute Incremental MERGE
  │
  ▼
Validate Production Load
  │
  ▼
Complete
```

Airflow retry and failure-handling mechanisms help improve pipeline reliability and simplify production troubleshooting.

---

## 📈 BigQuery Performance Optimization

BigQuery performance was optimized using:

* Time-based partitioning.
* Multi-column clustering.
* Query optimization.
* Incremental processing.
* Date-based filtering.

### Before vs After Benchmark

The same analytical query was compared before and after implementing partitioning and clustering.

| Configuration       | Data Scanned |
| ------------------- | -----------: |
| Unpartitioned table |    190.43 KB |
| Optimized table     |    109.69 KB |
| Reduction           |       ~42.4% |

### Result

Data scanned was reduced from **190.43 KB to 109.69 KB**, representing an approximately **42.4% reduction**.

Calculation:

```text
(190.43 - 109.69) / 190.43 × 100
≈ 42.4%
```

The comparison was based on BigQuery query execution statistics.

### Why Partitioning?

Time-based partitioning allows BigQuery to process only relevant partitions when queries contain appropriate date filters.

### Why Clustering?

Clustering helps organize data within partitions based on frequently filtered or grouped columns, improving query efficiency for suitable workloads.

### Why Incremental Processing?

Incremental processing avoids repeatedly processing unchanged historical data and reduces unnecessary data processing.

---

## 🏗️ Infrastructure as Code

Terraform is used to provision and manage the GCP infrastructure.

### Terraform Components

```text
providers.tf
    │
    ├── GCP Provider Configuration
    │
variables.tf
    │
    └── Reusable Configuration Variables
    │
storage.tf
    │
    └── Google Cloud Storage Configuration
    │
bigquery.tf
    │
    └── BigQuery Dataset/Table Configuration
```

### Benefits of Terraform

* Repeatable infrastructure deployment.
* Infrastructure version control.
* Reduced manual configuration.
* Consistent environments.
* Infrastructure as Code practices.

---

## 📁 Repository Structure

```text
gcp_banking_project/
│
├── dags/
│   └── Airflow DAG files
│
├── bigquery.tf
├── storage.tf
├── providers.tf
├── variables.tf
│
├── generate_transactions.py
├── daily_transactions.csv
├── README.md
└── .gitignore
```

---

## 🚀 Setup & Deployment

### Prerequisites

* Google Cloud Platform account.
* GCP project with billing enabled.
* Google Cloud CLI.
* Terraform.
* Python.
* Appropriate GCP permissions.

### 1. Clone the Repository

```bash
git clone https://github.com/VinayMukkamalla/gcp_banking_project.git

cd gcp_banking_project
```

### 2. Authenticate with GCP

```bash
gcloud auth application-default login
```

### 3. Configure the GCP Project

```bash
gcloud config set project YOUR_PROJECT_ID
```

### 4. Initialize Terraform

```bash
terraform init
```

### 5. Review Infrastructure Changes

```bash
terraform plan
```

### 6. Provision Infrastructure

```bash
terraform apply
```

Review the Terraform plan carefully before applying changes to your GCP environment.

---

## 🧪 Testing & Validation

The pipeline can be validated using:

* Source record counts.
* Staging record counts.
* Production record counts.
* Schema validation.
* Data type validation.
* Duplicate checks.
* Source-to-target reconciliation.
* BigQuery query execution statistics.
* Airflow task execution status.

---

## 💡 Challenges & Solutions

| Challenge                   | Solution                                         |
| --------------------------- | ------------------------------------------------ |
| Invalid data types          | Used `SAFE_CAST` for defensive type conversion   |
| Full-table reprocessing     | Implemented incremental `MERGE` processing       |
| High data scanned           | Implemented time-based partitioning              |
| Frequent filtering/grouping | Implemented multi-column clustering              |
| Pipeline failures           | Implemented Airflow retries and failure handling |
| Duplicate records           | Implemented validation and deduplication logic   |
| Data inconsistencies        | Implemented reconciliation and validation checks |

---

## 📊 Key Results

* Built an end-to-end GCP banking data engineering platform.
* Implemented Raw → Staging → Production BigQuery architecture.
* Automated infrastructure provisioning using Terraform.
* Implemented Airflow/Cloud Composer pipeline orchestration.
* Implemented incremental loading using BigQuery `MERGE`.
* Implemented schema validation and data-quality controls.
* Implemented partitioning and clustering for BigQuery optimization.
* Reduced benchmark query data scanned by approximately **42.4%**.
* Implemented production-style pipeline failure handling and validation.

---

## 🔮 Future Enhancements

Potential enhancements to extend the platform include:

* Add Pub/Sub and Dataflow for real-time transaction ingestion.
* Add automated CI/CD using GitHub Actions.
* Add Cloud Monitoring and Cloud Logging dashboards.
* Add automated data-quality testing.
* Implement CDC-based ingestion.
* Add Looker Studio dashboards.
* Introduce Secret Manager for secure configuration management.

---

## 👨‍💻 Author

**Vinay Mukkamalla**

GCP Data Engineer | BigQuery | Python | SQL | Data Engineering

GitHub: https://github.com/VinayMukkamalla

LinkedIn: https://www.linkedin.com/in/vinay-mukkamalla/

