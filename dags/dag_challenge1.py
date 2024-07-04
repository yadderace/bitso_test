# dags/csv_processing_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.download_data import download_multiple_files

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_challenge1',
    default_args=default_args,
    description='DAG to build a user activity datawarehouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)


download_data_task = PythonOperator(
    task_id='download_data_task',
    python_callable=download_multiple_files,
    dag=dag,
)

download_data_task
