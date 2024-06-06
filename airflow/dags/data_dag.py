from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'goodreads_fake_data',
    default_args=default_args,
    description='A simple DAG to generate fake Goodreads data',
    schedule_interval=timedelta(days=1),
)

def generate_goodreads_data(num_records):
    subprocess.run(['python', 'C:/Users/tuank/OneDrive/Documents/goodreads_etl_pipeline-1/goodreadsfaker/generate_fake_data.py', '--num_records', str(num_records)], check=True)

generate_data_task = PythonOperator(
    task_id='generate_goodreads_data',
    python_callable=generate_goodreads_data,
    op_kwargs={'num_records': 100},  # Set the number of records you want to generate
    dag=dag,
)

generate_data_task