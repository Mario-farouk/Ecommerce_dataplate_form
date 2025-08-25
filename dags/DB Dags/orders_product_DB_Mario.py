from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

PROJECT_ID = "ready-de26"
BUCKET = "ready-labs-postgres-to-gcs"


BQ_STAGE_DATASET = "project_stage"
BQ_LANDING_DATASET = "project_landing"

TABLES = {
    "order_items": "public.order_items",
    "orders": "public.orders",
    "products": "public.products",
    "order_reviews": "public.order_reviews",
    "product_category_name_translation": "public.product_category_name_translation"
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "orders_products_etl_pipeline",
    default_args=default_args,
    description="ETL from Postgres -> GCS Snapshot -> BQ Stage -> BQ Landing",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=1,
) as dag:

    for table_name, pg_table in TABLES.items():

        # 1. Extract: Postgres -> GCS Snapshot
        extract_to_gcs = PostgresToGCSOperator(
            task_id=f"extract_{table_name}_to_gcs",
            postgres_conn_id="orders_products_db_mario",
            sql=f"SELECT * FROM {pg_table} WHERE updated_at_timestamp::date = '{{{{ ds }}}}'",
            bucket=BUCKET,
            filename=f"snapshot/orders_products/{table_name}/dt={{{{ ds }}}}/{table_name}.parquet",
            export_format="parquet",
            gzip=False,
        )

        # 2. Load: GCS Snapshot -> BQ Stage (overwrite daily)
        load_to_stage = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_to_stage",
            bucket=BUCKET,
            source_objects=[f"snapshot/orders_products/{table_name}/dt={{{{ ds }}}}/{table_name}.parquet"],
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_STAGE_DATASET}.{table_name}_stage",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        # 3. Merge: Stage -> Landing
        merge_to_landing = BigQueryInsertJobOperator(
            task_id=f"merge_{table_name}_to_landing",
            configuration={
                "query": {
                    "query": f"""
                    MERGE `{PROJECT_ID}.{BQ_LANDING_DATASET}.{table_name}_mario` T
                    USING `{PROJECT_ID}.{BQ_STAGE_DATASET}.{table_name}_stage` S
                    ON T.order_id = S.order_id  
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT ROW
                    """,
                    "useLegacySql": False,
                }
            },
            location="US",
        )

        extract_to_gcs >> load_to_stage >> merge_to_landing
