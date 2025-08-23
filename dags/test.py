from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 8, 1),  # past date
    schedule_interval=None,           # manual trigger
    catchup=False,
    tags=["test"],
) as dag:

    # Simple test task
    hello_task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow!'"
    )
