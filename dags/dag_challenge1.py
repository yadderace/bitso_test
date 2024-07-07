from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import asyncio
import json
import os
import pandas as pd
import config

from src.challenge1 import process_books, json_to_partitioned_file

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with params
dag = DAG(
    'book_processing_dag',
    default_args=default_args,
    description='A DAG to process books every ten minutes',
    schedule_interval='*/10 * * * *',
    catchup=False,
    params={
        'sleep_time': config.ARG_SLEEP_TIME,  
        'execution_count': config.ARG_EXECUTION_COUNT, 
        'book_list': config.ARG_BOOK_LIST 
    }
)

# Function to run process_books with parameters
async def run_process_books(books, sleep_time, execution_count):
    for _ in range(execution_count):
        await process_books(books)
        await asyncio.sleep(sleep_time)

# Validate and run process_books
def process_books_task(**kwargs):
    params = kwargs['params']
    
    # Validate parameters
    try:
        sleep_time = int(params['sleep_time'])
        execution_count = int(params['execution_count'])
        books = json.loads(params['book_list'])
        
        if not isinstance(books, list):
            raise ValueError("book_list must be a JSON string representing a list.")
        
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid parameter format: {e}")

    # Execute the async function
    asyncio.run(run_process_books(books, sleep_time, execution_count))

process_books_operator = PythonOperator(
    task_id='process_books_task',
    python_callable=process_books_task,
    provide_context=True,
    dag=dag,
)

# Validate and run json_to_partitioned_file
def partition_files_task(**kwargs):
    params = kwargs['params']
    
    # Validate parameters
    try:
        books = json.loads(params['book_list'])
        
        if not isinstance(books, list):
            raise ValueError("book_list must be a JSON string representing a list.")
        
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid parameter format: {e}")

    # Execute the function
    json_to_partitioned_file(books)

partition_files_operator = PythonOperator(
    task_id='partition_files_task',
    python_callable=partition_files_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
process_books_operator >> partition_files_operator
