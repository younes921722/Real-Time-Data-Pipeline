import psycopg2
import logging

def get_db_connection():
    try:
        connection = psycopg2.connect(
            host="postgres",     # DB host
            database="airflow",
            user="airflow",
            password="airflow",
            port="5432"           # Default PostgreSQL port
        )
        return connection
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return None
