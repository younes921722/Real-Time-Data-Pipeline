import psycopg2
import logging
from config.db_config import get_db_connection

class PostgresTableManager:
    def __init__(self):
        self.connection = get_db_connection()
        if self.connection:
            self.cursor = self.connection.cursor()

    def create_user_table_if_not_exists(self):
        if self.connection:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS users_data (
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    gender VARCHAR(10),
                    address TEXT,
                    postcode INTEGER,
                    email VARCHAR(100) UNIQUE,
                    username VARCHAR(50) UNIQUE,
                    dob TIMESTAMP,
                    registered TIMESTAMP,
                    phone VARCHAR(20),
                    picture TEXT
                );
                """
            self.cursor.execute(create_table_query)
            self.connection.commit()
            logging.info("Table 'user_data' created or already exists.")

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            logging.info("Database connection closed.")
