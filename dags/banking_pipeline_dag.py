import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# 1. Dynamically find and load the JSON configuration file at runtime
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(DAG_FOLDER, 'config', 'pipeline_config.json')

# 1/A. The Production Linux Path Standard
# Cloud Composer mounts your DAG folder directly to '/home/airflow/gcs/dags' on Linux
# DAGS_FOLDER = os.environ.get('AIRFLOW_HOME', '/home/airflow/gcs') + '/dags'
# CONFIG_PATH = os.path.join(DAGS_FOLDER, 'config', 'pipeline_config.json')

# No OS path calculations needed! Reads directly from the meta-database
# cfg = Variable.get("banking_pipeline_config", deserialize_json=True)

with open(CONFIG_PATH, 'r') as f:
    cfg = json.load(f)

# 2. Extract configuration values safely into variables
PROJECT_ID = cfg['project_id']
BUCKET = cfg['gcs_bucket_name']
RAW_TARGET = f"{PROJECT_ID}.{cfg['datasets']['raw']}.{cfg['tables']['raw_transactions']}"
STAGING_TARGET = f"{PROJECT_ID}.{cfg['datasets']['staging']}.{cfg['tables']['staging_cleaned']}"
PROD_TARGET = f"{PROJECT_ID}.{cfg['datasets']['production']}.{cfg['tables']['production_fact']}"

default_args = {
    'owner': 'vinay_gcp_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': cfg['dag_settings']['retries'],
    'retry_delay': timedelta(minutes=cfg['dag_settings']['retry_delay_minutes']),
}

# 3. Instantiate the DAG using modular variables
with DAG(
    'enterprise_banking_elt_pipeline_modular',
    default_args=default_args,
    description='Automated Dynamic Ingestion and Processing Pipeline for Banking Ledger',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    load_gcs_to_raw = GCSToBigQueryOperator(
        task_id='load_gcs_to_raw_table',
        bucket=BUCKET,
        source_objects=[cfg['source_file_path']],
        destination_project_dataset_table=RAW_TARGET,
        write_disposition='WRITE_APPEND',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=True,
    )

    transform_raw_to_staging = BigQueryExecuteQueryOperator(
        task_id='transform_raw_to_staging_cleaned',
        sql=f'''
            CREATE OR REPLACE TABLE `{STAGING_TARGET}` AS
            SELECT DISTINCT
              SAFE_CAST(transaction_id AS STRING) AS transaction_id,
              SAFE_CAST(account_id AS STRING) AS account_id,
              SAFE_CAST(amount AS FLOAT64) AS amount,
              UPPER(TRIM(transaction_type)) AS transaction_type,
              SAFE_CAST(timestamp AS TIMESTAMP) AS transaction_timestamp,
              UPPER(TRIM(location)) AS location,
              CURRENT_TIMESTAMP() AS ingestion_timestamp
            FROM 
              `{RAW_TARGET}`
            WHERE 
              transaction_id IS NOT NULL;
        ''',
        use_legacy_sql=False,
    )

    upsert_staging_to_production = BigQueryExecuteQueryOperator(
        task_id='upsert_staging_to_production_fact',
        sql=f'''
            MERGE `{PROD_TARGET}` T
            USING `{STAGING_TARGET}` S
            ON T.transaction_id = S.transaction_id
            WHEN NOT MATCHED THEN
              INSERT (transaction_id, account_id, amount, transaction_type, transaction_timestamp, location, ingestion_timestamp)
              VALUES (S.transaction_id, S.account_id, S.amount, S.transaction_type, S.transaction_timestamp, S.location, S.ingestion_timestamp);
        ''',
        use_legacy_sql=False,
    )

    load_gcs_to_raw >> transform_raw_to_staging >> upsert_staging_to_production







































# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# # 1. Define baseline configurations and retry policies
# default_args = {
#     'owner': 'vinay_gcp_data_engineer',
#     'depends_on_past': False,
#     'start_date': datetime(2026, 1, 1),
#     'email_on_failure': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
# }

# # 2. Instantiate the DAG workflow container
# with DAG(
#     'enterprise_banking_elt_pipeline',
#     default_args=default_args,
#     description='Automated Daily Ingestion and Processing Pipeline for Banking Ledger',
#     schedule_interval='@daily', # Runs every single night at midnight
#     catchup=False,
#     max_active_runs=1,
# ) as dag:

#     # TASK 1: Automatically extract the CSV from GCS and load it into the Raw Table
#     load_gcs_to_raw = GCSToBigQueryOperator(
#         task_id='load_gcs_to_raw_table',
#         bucket='gcp-data-engineer-501607-raw-banking-lake',
#         source_objects=['ingestion/daily_transactions.csv'],
#         destination_project_dataset_table='gcp-data-engineer-501607.raw_banking.transactions',
#         write_disposition='WRITE_APPEND', # Appends new data daily
#         source_format='CSV',
#         skip_leading_rows=1,
#         autodetect=True,
#     )

#     # TASK 2: Execute SQL processing logic to push cleaned data into the Staging Layer
#     transform_raw_to_staging = BigQueryExecuteQueryOperator(
#         task_id='transform_raw_to_staging_cleaned',
#         sql='''
#             CREATE OR REPLACE TABLE `gcp-data-engineer-501607.staging_banking.transactions_cleaned` AS
#             SELECT DISTINCT
#               SAFE_CAST(transaction_id AS STRING) AS transaction_id,
#               SAFE_CAST(account_id AS STRING) AS account_id,
#               SAFE_CAST(amount AS FLOAT64) AS amount,
#               UPPER(TRIM(transaction_type)) AS transaction_type,
#               SAFE_CAST(timestamp AS TIMESTAMP) AS transaction_timestamp,
#               UPPER(TRIM(location)) AS location,
#               CURRENT_TIMESTAMP() AS ingestion_timestamp
#             FROM 
#               `gcp-data-engineer-501607.raw_banking.transactions`
#             WHERE 
#               transaction_id IS NOT NULL;
#         ''',
#         use_legacy_sql=False,
#     )

#     # TASK 3: Load the cleaned staging data incrementally into the partitioned Production table
#     upsert_staging_to_production = BigQueryExecuteQueryOperator(
#         task_id='upsert_staging_to_production_fact',
#         sql='''
#             MERGE `gcp-data-engineer-501607.prod_banking.fact_transactions` T
#             USING `gcp-data-engineer-501607.staging_banking.transactions_cleaned` S
#             ON T.transaction_id = S.transaction_id
#             WHEN NOT MATCHED THEN
#               INSERT (transaction_id, account_id, amount, transaction_type, transaction_timestamp, location, ingestion_timestamp)
#               VALUES (S.transaction_id, S.account_id, S.amount, S.transaction_type, S.transaction_timestamp, S.location, S.ingestion_timestamp);
#         ''',
#         use_legacy_sql=False,
#     )

#     # 3. Set the definitive task dependencies execution order
#     load_gcs_to_raw >> transform_raw_to_staging >> upsert_staging_to_production
