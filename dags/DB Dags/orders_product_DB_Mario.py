from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from datetime import datetime

PROJECT_ID = "ready-de26"
BUCKET = "ready-labs-postgres-to-gcs"
DATASET_LANDING = "project_landing"

SNAPSHOT_FOLDER = "orders_products_db_snapshot_mario"
STAGE_FOLDER = "orders_products_db_stage_mario"

TABLES = {
    "order_items": "order_item_id",
    "order_reviews": "review_id",
    "orders": "order_id",
    "products": "product_id",
    "product_category_name_translation": "product_category_name",
}

default_args = {"start_date": datetime(2025, 1, 1)}

with DAG(
    dag_id="orders_products_db_pipeline_mario",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["postgres", "gcs", "bigquery"],
) as dag:

    for table, pk in TABLES.items():

        #  Extract snapshot from Postgres → Snapshot folder
        snapshot = PostgresToGCSOperator(
            task_id=f"snapshot_{table}",
            postgres_conn_id="orders_products_db_mario",
            sql=f"""
                SELECT * 
                FROM public.{table} 
                WHERE DATE(updated_at_timestamp) = '{{{{ ds }}}}'
            """,
            bucket=BUCKET,
            filename=f"{SNAPSHOT_FOLDER}/{table}/dt={{{{ ds }}}}/{table}.json",
            export_format="json",
            gcp_conn_id="google_cloud_default",
        )

        # 2️ Load snapshot → Stage folder (overwrite)
        load_stage = PostgresToGCSOperator(
            task_id=f"load_stage_{table}",
            postgres_conn_id="orders_products_db_mario",
            sql=f"""
                SELECT * 
                FROM public.{table} 
                WHERE DATE(updated_at_timestamp) = '{{{{ ds }}}}'
            """,
            bucket=BUCKET,
            filename=f"{STAGE_FOLDER}/{table}/dt={{{{ ds }}}}/{table}.json",
            export_format="json",
            gcp_conn_id="google_cloud_default",
        )

        # 3️ Merge stage → Landing table in BigQuery
        merge_sql = f"""
        MERGE `{PROJECT_ID}.{DATASET_LANDING}.{table}` T
        USING `{PROJECT_ID}.{STAGE_FOLDER}.{table}` S
        ON T.{pk} = S.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT ROW
        """

        merge = BigQueryInsertJobOperator(
            task_id=f"merge_{table}",
            configuration={"query": {"query": merge_sql, "useLegacySql": False}},
            gcp_conn_id="google_cloud_default",
        )

        # 4️⃣ Cleanup stage folder after merge
        cleanup_stage = GCSDeleteObjectsOperator(
            task_id=f"cleanup_stage_{table}",
            bucket_name=BUCKET,
            objects=[f"{STAGE_FOLDER}/{table}/dt={{ ds }}/"],
            gcp_conn_id="google_cloud_default",
        )

        snapshot >> load_stage >> merge >> cleanup_stage
