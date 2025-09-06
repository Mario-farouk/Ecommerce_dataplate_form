from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import requests
import pandas as pd
from io import StringIO

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def extract_api_to_gcs():
    """Extract CSV data from API and upload to GCS"""
    api_url = 'https://payments-table-834721874829.europe-west1.run.app'
    gcs_bucket = 'ready-labs-postgres-to-gcs'
    gcs_object = f'PaymentMethod_API_Mario/data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    try:
        # Fetch CSV data from API
        print(f"Fetching CSV data from API: {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        # Read CSV data with pandas
        csv_data = response.text
        print(f"Successfully fetched CSV data: {len(csv_data)} characters")
        
        # Optional: Validate CSV structure
        df = pd.read_csv(StringIO(csv_data))
        print(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Upload CSV to GCS
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=gcs_object,
            data=csv_data,
            mime_type='text/csv'
        )
        
        print(f"Successfully uploaded CSV to GCS: gs://{gcs_bucket}/{gcs_object}")
        return gcs_object
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"API request failed: {str(e)}")
    except Exception as e:
        raise AirflowException(f"CSV processing or GCS upload failed: {str(e)}")

with DAG(
    'payment_method_api_to_bq',
    default_args=default_args,
    description='Extract payment method CSV data from API to GCS to BigQuery',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['payment_method', 'api', 'gcs', 'bigquery', 'csv']
) as dag:

    extract_to_gcs = PythonOperator(
        task_id='extract_api_to_gcs',
        python_callable=extract_api_to_gcs
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket='ready-labs-postgres-to-gcs',
        source_objects=['PaymentMethod_API_Mario/*.csv'],  # Changed to CSV
        destination_project_dataset_table='project_landing.payment_method_api_data',
        source_format='CSV',  # Changed from JSON to CSV
        autodetect=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,  # Skip header row if exists
        field_delimiter=',',  # CSV delimiter
        gcp_conn_id='google_cloud_default'
    )

    extract_to_gcs >> load_to_bq