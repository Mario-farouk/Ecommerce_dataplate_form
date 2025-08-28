import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

PROJECT_ID = "ready-de26"
BUCKET = "ready-labs-postgres-to-gcs"
BQ_STAGE_DATASET = "project_landing"  
BQ_LANDING_DATASET = "project_landing"

TABLES = {
    "order_items": "public.order_items",
    "orders": "public.orders",
    "products": "public.products",
    "order_reviews": "public.order_reviews",
    "product_category_name_translation": "public.product_category_name_translation"
}

SQL_FOLDER = os.path.join(os.path.dirname(__file__), "SQL", "Merge")

with DAG(
    "orders_products_etl_pipeline_mario_20250822",
    description="ETL pipeline for specific date 2025-08-22",
    schedule_interval=None,   # run manually only
    start_date=datetime(2025, 8, 22),
    catchup=False,
    max_active_runs=1,
) as dag:

    for table_name, pg_table in TABLES.items():
        extract_to_gcs = PostgresToGCSOperator(
            task_id=f"extract_{table_name}_to_gcs",
            postgres_conn_id="orders_products_db_mario",
            sql=f"SELECT * FROM {pg_table} WHERE updated_at_timestamp::date = '2025-08-22'",
            bucket=BUCKET,
            filename=f"snapshot/orders_products/{table_name}/dt=2025-08-22/{table_name}.parquet",
            export_format="parquet",
            gzip=False,
        )

        load_to_stage = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_to_stage",
            bucket=BUCKET,
            source_objects=[f"snapshot/orders_products/{table_name}/dt=2025-08-22/{table_name}.parquet"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_STAGE_DATASET}.{table_name}_stage_mario",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        sql_file_path = os.path.join(SQL_FOLDER, f"{table_name}_merge.sql")
        with open(sql_file_path, "r") as f:
            merge_sql = f.read()

        merge_to_landing = BigQueryInsertJobOperator(
            task_id=f"merge_{table_name}_to_landing",
            configuration={ 
                "query": {
                    "query": merge_sql,
                    "useLegacySql": False,
                }
            },
            location="US",
        )

        extract_to_gcs >> load_to_stage >> merge_to_landing
