import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2 import sql
from streaming.kafka_producer import DataStreamer

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from config.db_manager import PostgresTableManager

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['younes.test@gmail.com'],  # Notify on failure
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,  # Number of retries before failing
    'retry_delay': timedelta(minutes=5),  # Time between retries
    'depends_on_past': False,  # Run each task independently by default
    'catchup': False,  # Prevents backfilling when the DAG is triggered manually
}




def create_user_table():
    manager = PostgresTableManager()
    manager.create_user_table_if_not_exists()
    # manager.close_connection()


def stream_data_to_kafka_potgres():   
        streamer = DataStreamer(bootstrap_servers=["broker:29092"], topic_name="users_created")
        streamer.stream_data()
     

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
        #  max_active_tasks=2  #this allows both tasks to run at the same time
         ) as dag:
    first_task_postgres = PythonOperator(
        task_id='creating_postgres_table',
        python_callable= create_user_table,
    )
    streaming_task = PythonOperator(
        task_id='stream_data_to_kafka_and_postgres',
        python_callable= stream_data_to_kafka_potgres,
    )
    

first_task_postgres >> streaming_task
