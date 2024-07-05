from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the path to your project directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import your functions from the challenge1.py script
from src.challenge1 import download_data, create_fct_active_users, create_fct_system_activity, create_dim_users

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'daily_data_processing',
    default_args=default_args,
    description='A simple data processing DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Task to download data
    task_download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    # Task to create fct_active_users
    task_create_fct_active_users = PythonOperator(
        task_id='create_fct_active_users',
        python_callable=create_fct_active_users,
        op_args=["2024-07-01", 30, "data/input/deposit_sample_data.csv", "data/input/withdrawals_sample_data.csv", "data/output/fct_active_users.csv"],
    )

    # Task to create fct_system_activity
    task_create_fct_system_activity = PythonOperator(
        task_id='create_fct_system_activity',
        python_callable=create_fct_system_activity,
        op_args=["2024-07-01", 30, "data/input/event_sample_data.csv", "data/input/deposit_sample_data.csv", "data/input/withdrawals_sample_data.csv", "data/output/fct_system_activity.csv"],
    )

    # Task to create dim_users
    task_create_dim_users = PythonOperator(
        task_id='create_dim_users',
        python_callable=create_dim_users,
        op_args=["2024-07-01", 30, "data/output/fct_system_activity.csv", "data/input/user_id_sample_data.csv", "data/output/dim_users.csv"],
    )

    # Set task dependencies
    task_download_data >> [task_create_fct_active_users, task_create_fct_system_activity] >> task_create_dim_users
