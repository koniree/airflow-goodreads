from airflow import DAG
import argparse
import os,sys
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from goodreadsfaker.generate_fake_data import GoodreadsFake

default_args = {
    'owner': 'goodreads',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'goodreads_fake_data',
    default_args=default_args,
    description='A simple DAG to generate fake Goodreads data',
    schedule_interval=timedelta(seconds=20),
)
start_operator = EmptyOperator(task_id='Begin_execution',  dag=dag)

# def generate_goodreads_data(num_records):
#     subprocess.run(['python', './goodreadsfaker/generate_fake_data.py', '--num_records', str(num_records)], check=True)

# generate_data_task = PythonOperator(
#     task_id='generate_goodreads_data',
#     python_callable=generate_goodreads_data,
#     op_kwargs={'num_records': 100},  # Set the number of records you want to generate
#     dag=dag,
# )

def generate_goodreads_data():
    fk = GoodreadsFake()
    for i in range(100):
        print(f"Running iteration: {i}")
        fk.generate(i)



def load_data_to_postgres(**kwargs):
    # Create a PostgresHook to connect to your PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    
    # Read the generated data
    file_path = './goodreadsfaker/generated_data.csv'
    df = pd.read_csv(file_path)

    # Establish connection
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Define the table name and the columns
    table_name = 'goodreads_fake_data'
    columns = ', '.join(df.columns)

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f'{col} TEXT' for col in df.columns])}
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    for index, row in df.iterrows():
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(insert_query, tuple(row))
    
    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()

generate_data_task = PythonOperator(
    task_id='generate_goodreads_data',
    python_callable=generate_goodreads_data,
    #op_kwargs={'num_records': 100},
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)


end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> generate_data_task >> load_data_task >> end_operator