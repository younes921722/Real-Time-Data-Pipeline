import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2 import sql
from data import DataExtractor, DataTransformer
from controllers.user_controller import UserController
from streaming.kafka_producer import DataStreamer

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

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
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 8, 30, 10, 00),
# }

extracted_data = DataExtractor().extract_data()
transformed_data = DataTransformer().transform_data(extracted_data)

# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import logging
#     import time
    

#     curr_time = time.time()

#     producer = KafkaProducer(bootstrap_servers=["broker:29092"],
#                              max_block_ms=5000)
#     while True:
#         if time.time() > curr_time + 60:
#             break
#         try:
#             data = transformed_data
#             print(json.dumps(data, indent=4))

#             producer.send("users_created", json.dumps(data).encode("utf-8"))
#             # Insert data into PostgreSQL
#             UserController().insert_user(data)

#             time.sleep(2)
#         except Exception as e:
#             logging.error(f"An error occurred: {e}")
#             continue
    
def stream_data_to_kafka():   
        streamer = DataStreamer(bootstrap_servers=["broker:29092"], topic_name="users_created")
        # ... get and transform your data (e.g., using DataExtractor and DataTransformer) ...
        if transformed_data:
            streamer.stream_data(transformed_data)

def stream_data_to_postgres():
    UserController().insert_user(transformed_data)
     

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         max_active_tasks=2  #this allows both tasks to run at the same time
         ) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_to_kafka',
        python_callable= stream_data_to_kafka,
    )
    # Second task run in parallel if it does not depend on the first one
    second_task_operator = PythonOperator(
        task_id='stream_data_to_postgres',
        python_callable= stream_data_to_postgres,
    )
