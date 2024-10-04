import sys
import os

# Add src directory to Python path
sys.path.append('/opt/airflow/src')


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src import ingest_data, process_data, load_data



def run_pipeline():
    raw_data = ingest_data()
    processed_tables = process_data(raw_data)
    load_data(processed_tables)


# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 3),
    'retries': 1,
}

# Create the DAG
with DAG('uber_taxi_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline,
    )

    run_pipeline_task
