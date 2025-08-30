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
TARGET_DATE = "2025-08-22"
TARGET_DATE_NODASH = "20250822"  # For file paths

Tables = {"customers": "public.customers",
          "geolocation": "public.geolocation",
          "leads_closed": "public.leads_closed",
          "leads_qualified": "public.leads_qualified"
          }
