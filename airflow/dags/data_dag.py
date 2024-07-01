# # from airflow import DAG
# # import argparse
# # import os,sys
# # from airflow.operators.python import PythonOperator
# # from datetime import datetime, timedelta
# # from airflow.operators.empty import EmptyOperator
# # import pandas as pd
# # from airflow.providers.postgres.hooks.postgres import PostgresHook
# # import logging

# # sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
# # from goodreadsfaker.generate_fake_data import GoodreadsFake

# # default_args = {
# #     'owner': 'goodreads',
# #     'depends_on_past': False,
# #     'start_date': datetime(2024, 6, 1),
# #     'email': ['your_email@example.com'],
# #     'email_on_failure': False,
# #     'email_on_retry': False,
# #     'retries': 1,
# #     'retry_delay': timedelta(minutes=2),
# # }

# # dag = DAG(
# #     'goodreads_fake_data',
# #     default_args=default_args,
# #     description='A simple DAG to generate fake Goodreads data',
# #     schedule_interval=timedelta(hours=5),
# # )
# # start_operator = EmptyOperator(task_id='Begin_execution',  dag=dag)

# # def generate_goodreads_data(num_records):
# #     subprocess.run(['python', './goodreadsfaker/generate_fake_data.py', '--num_records', str(num_records)], check=True)

# # generate_data_task = PythonOperator(
# #     task_id='generate_goodreads_data',
# #     python_callable=generate_goodreads_data,
# #     op_kwargs={'num_records': 100},  # Set the number of records you want to generate
# #     dag=dag,
# # )

# #def generate_goodreads_data(num_records = 50):

# from airflow import DAG
# import os
# import sys
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from datetime import datetime, timedelta
# from airflow.operators.empty import EmptyOperator
# import pandas as pd
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# import logging

# sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
# from goodreadsfaker.generate_fake_data import GoodreadsFake

# pg_hook = PostgresHook(postgres_conn_id='postgres')
# conn = pg_hook.get_conn()
# cursor = conn.cursor()

# default_args = {
#     'owner': 'goodreads',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 6, 1),
#     'email': ['your_email@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2),
# }



# dag = DAG(
#     'goodreads_fake_data',
#     default_args=default_args,
#     description='A simple DAG to generate fake Goodreads data',
#     schedule_interval=timedelta(hours=5),
# )

# start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

# def generate_goodreads_data(num_records=50):
#     fk = GoodreadsFake()
#     for i in range(100):
#         print(f"Running iteration: {i}")
#         fk.generate(num_records)

# def load_data_to_postgres(**kwargs):
#     try:
#         directory = "/opt/airflow/GoodReadsDatafake"
#         csv_files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.csv')]
        
#         if not csv_files:
#             logging.info(f"No CSV files found in directory {directory}.")
#             return

#         for file_path in csv_files:
#             try:
#                 logging.info(f"Processing file: {file_path}")
#                 for chunk in pd.read_csv(file_path, chunksize=1000):  # Adjust chunksize as needed
#                     table_name = os.path.splitext(os.path.basename(file_path))[0]
#                     columns = ', '.join(chunk.columns)

#                     create_table_query = f"""
#                     CREATE TABLE IF NOT EXISTS "{table_name}" (
#                         {', '.join([f'{col} TEXT' for col in chunk.columns])}
#                     );
#                     """
#                     cursor.execute(create_table_query)
#                     conn.commit()

#                     for index, row in chunk.iterrows():
#                         insert_query = f"""INSERT INTO "{table_name}" ({columns}) VALUES ({', '.join(['%s'] * len(row))})"""
#                         cursor.execute(insert_query, tuple(row))

#                     conn.commit()
#             except Exception as e:
#                 logging.error(f"Error processing file {file_path}: {e}")
#                 continue

#         cursor.close()
#         conn.close()
#         logging.info("Data loading completed successfully.")
#     except Exception as e:
#         logging.error(f"Error in load_data_to_postgres: {e}")
#         raise


# def create_db():
#     cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'goodreads'")
#     exists = cursor.fetchone()
#     if not exists:
#         cursor.execute("CREATE DATABASE goodreads")
#         conn.commit()

# # Task to create the new database
# create_db = PythonOperator(
#     task_id='create_db',
#     python_callable = create_db,
#     dag=dag,
# )


# generate_data_task = PythonOperator(
#     task_id='generate_goodreads_data',
#     python_callable=generate_goodreads_data,
#     op_kwargs={'num_records': 50},
#     dag=dag,
# )

# load_data_task = PythonOperator(
#     task_id='load_data_to_postgres',
#     python_callable=load_data_to_postgres,
#     dag=dag,
# )

# end_operator = EmptyOperator(task_id='Stop_execution', dag=dag)

# start_operator >> create_db >> generate_data_task >> load_data_task >> end_operator

    
  



# generate_data_task = PythonOperator(
#     task_id='generate_goodreads_data',
#     python_callable=generate_goodreads_data,
#     op_kwargs={'num_records': 50},
#     dag=dag,
# )

# load_data_task = PythonOperator(
#     task_id='load_data_to_postgres',
#     python_callable=load_data_to_postgres,
#     #provide_context=True,
#     dag=dag,
# )


# end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)

# start_operator >> generate_data_task >> load_data_task >> end_operator

from airflow import DAG
import os
import sys
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

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
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'goodreads_fake_data',
    default_args=default_args,
    description='A simple DAG to generate fake Goodreads data',
    schedule_interval=timedelta(hours=5),
)

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

def create_db():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'goodreads'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute("CREATE DATABASE goodreads")
        conn.commit()
    cursor.close()
    conn.close()

def generate_goodreads_data(num_records=50):
    fk = GoodreadsFake()
    for i in range(100):
        logging.info(f"Running iteration: {i}")
        fk.generate(num_records)

def load_data_to_postgres(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        directory = "/opt/airflow/GoodReadsDatafake"
        csv_files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.csv')]
        
        if not csv_files:
            logging.info(f"No CSV files found in directory {directory}.")
            return

        for file_path in csv_files:
            try:
                logging.info(f"Processing file: {file_path}")
                for chunk in pd.read_csv(file_path, chunksize=1000):  # Adjust chunksize as needed
                    table_name = os.path.splitext(os.path.basename(file_path))[0]
                    columns = ', '.join(chunk.columns)

                    create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS "{table_name}" (
                        {', '.join([f'{col} TEXT' for col in chunk.columns])}
                    );
                    """
                    cursor.execute(create_table_query)
                    conn.commit()

                    for index, row in chunk.iterrows():
                        insert_query = f"""INSERT INTO "{table_name}" ({columns}) VALUES ({', '.join(['%s'] * len(row))})"""
                        cursor.execute(insert_query, tuple(row))

                    conn.commit()
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")
                continue

        logging.info("Data loading completed successfully.")
    except Exception as e:
        logging.error(f"Error in load_data_to_postgres: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

create_db_task = PythonOperator(
    task_id='create_db',
    python_callable=create_db,
    dag=dag,
)

generate_data_task = PythonOperator(
    task_id='generate_goodreads_data',
    python_callable=generate_goodreads_data,
    op_kwargs={'num_records': 50},
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

end_operator = EmptyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_db_task >> generate_data_task >> load_data_task >> end_operator
