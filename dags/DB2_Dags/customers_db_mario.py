import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

PROJECT_ID = "ready-de26"
BUCKET = "ready-labs-postgres-to-gcs"
BQ_DATASET = "project_landing"  # Both stage and landing in same dataset

# Specific date we want to process
TARGET_DATE = "2025-08-23"
TARGET_DATE_NODASH = "20250822"  # For file paths

TABLES = {"customers": "public.customers",
          "geolocation": "public.geolocation",
          "leads_closed": "public.leads_closed",
          "leads_qualified": "public.leads_qualified"
          }
SQL_FOLDER= os.path.join(os.path.dirname(__file__),"SQL","Merge")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': 60,  # 60 seconds
}
with DAG(
    "customers_db_mario",
    description="ETL pipeline for specific date ",
    schedule_interval=None,   # Manual trigger only
    start_date=datetime(2025, 8, 22),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['gcp', 'etl', 'postgres', 'bigquery'],
) as dag:

    start_pipeline = DummyOperator(task_id="start_pipeline")
    end_pipeline = DummyOperator(task_id="end_pipeline")


    for table_name, pg_table in TABLES.items():
        
        # 1. Extract from PostgreSQL to GCS (Archive)
        extract_to_gcs = PostgresToGCSOperator(
            task_id=f"extract_{table_name}_to_gcs",
            postgres_conn_id="customers_db_mario",
            sql=f"SELECT * FROM {pg_table} WHERE updated_at_timestamp::date = '{TARGET_DATE}'",
            bucket=BUCKET,
            filename=f"customers_db_snapshot_mario/{table_name}/dt={TARGET_DATE}/{table_name}.parquet",
            export_format="parquet",
            gzip=False,
        )
    

     # 2. Load from GCS to BigQuery Stage table
        load_to_stage = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_to_stage",
            bucket=BUCKET,
            source_objects=[f"customers_db_snapshot_mario/{table_name}/dt={TARGET_DATE}/{table_name}.parquet"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{table_name}_stage_mario",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
            autodetect=True,
            create_disposition="CREATE_IF_NEEDED",
        )

         # 3. Read merge SQL from file
        sql_file_path = os.path.join(SQL_FOLDER, f"{table_name}_merge.sql")
        
        # 4. Merge from Stage to Landing table
        merge_to_landing = BigQueryInsertJobOperator(
            task_id=f"merge_{table_name}_to_landing",
            configuration={
                "query": {
                    "query": "{% include 'SQL/Merge/" + table_name + "_merge.sql' %}",
                    "useLegacySql": False,
                }
            },
            location="US",
        )

        # Set task dependencies
        start_pipeline >> extract_to_gcs >> load_to_stage >> merge_to_landing >> end_pipeline