import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2 import sql
from streaming.kafka_producer import DataStreamer

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from config.db_manager import PostgresTableManager
from test import SQLServerConnection

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
    manager.create_load_table_if_not_exists()
    manager.close_connection()


def stream_data_to_kafka_potgres():
         # Using SQL Server Authentication
        db = SQLServerConnection(
            server="host.docker.internal",  
            database="AdventureWorksLT2022",
            username="younes",  
            password="Younes921722",  
            trusted_connection=False 
        )
        
        try:
            with db as connection:
                cursor = connection.cursor()
                
                # Create a new table for job candidates
                create_table_query = """
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'JobCandidates')
                CREATE TABLE JobCandidates (
                    CandidateID INT PRIMARY KEY IDENTITY(1,1),
                    FirstName NVARCHAR(50),
                    LastName NVARCHAR(50),
                    Position NVARCHAR(100),
                    YearsExperience INT,
                    ExpectedSalary DECIMAL(10,2)
                )
                """
                cursor.execute(create_table_query)
                connection.commit()
                print("Table created successfully")
                
                # Insert multiple candidates
                insert_query = """
                INSERT INTO JobCandidates (FirstName, LastName, Position, YearsExperience, ExpectedSalary)
                VALUES (?, ?, ?, ?, ?)
                """
                candidates = [
                    ('John', 'Smith', 'Software Engineer', 5, 85000.00),
                    ('Sarah', 'Johnson', 'Data Scientist', 3, 78000.00),
                    ('Michael', 'Brown', 'Project Manager', 7, 95000.00),
                    ('Emily', 'Davis', 'UX Designer', 4, 72000.00)
                ]
                
                for candidate in candidates:
                    cursor.execute(insert_query, candidate)
                connection.commit()
                print("Sample data inserted successfully")
                
                # Query and display the inserted data
                cursor.execute("SELECT * FROM JobCandidates")
                rows = cursor.fetchall()
                print("\nCurrent Job Candidates:")
                print("-----------------------")
                for row in rows:
                    print(f"ID: {row.CandidateID}, Name: {row.FirstName} {row.LastName}")
                    print(f"Position: {row.Position}")
                    print(f"Experience: {row.YearsExperience} years")
                    print(f"Expected Salary: ${row.ExpectedSalary:,.2f}")
                    print("-----------------------")
                
        except Exception as e:
            print(f"Error: {str(e)}")

        streamer = DataStreamer(bootstrap_servers=["ed-kafka:29092"], topic_name="users_created")
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
