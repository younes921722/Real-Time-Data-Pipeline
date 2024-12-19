import psycopg2
import pyodbc
import pandas as pd
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Logging configuration
def log_error(message):
    print(f"ERROR: {message}", file=sys.stderr)

def log_info(message):
    print(f"INFO: {message}")

# PostgreSQL Connection Configuration
POSTGRES_CONFIG = {
    "host": os.getenv('POSTGRESQL_HOST'),
    "port": os.getenv('POSTGRESQL_PORT'),
    "database":os.getenv('POSTGRESQL_DATABASE'),
    "user": os.getenv('POSTGRESQL_USER'),
    "password": os.getenv('POSTGRESQL_PASSWORD')
}


# SQL Server Connection Configuration
SQL_SERVER_CONFIG = {
    "driver": os.getenv('ODBC Driver 17 for SQL Server'),  # Exact match to system driver
    "server": "(local)",
    "database": os.getenv('SQL_SERVER_DATABASE'),
    "username": os.getenv('SQL_SERVER_USERNAME'),
    "password": os.getenv('SQL_SERVER_PASSWORD')
}


# Table names
POSTGRES_TABLE = "loaded_users_data"
SQL_SERVER_TABLE = "loaded_users_data"

def detailed_odbc_driver_check():
    """Perform a comprehensive ODBC driver check."""
    try:
        # Print system environment
        log_info("System Environment:")
        log_info(f"Python Version: {sys.version}")
        log_info(f"pyodbc Version: {pyodbc.version}")

        # List all ODBC drivers with detailed information
        log_info("\nDetailed ODBC Driver List:")
        all_drivers = pyodbc.drivers()
        for driver in all_drivers:
            log_info(f"- {driver}")

        # Check for specific drivers with multiple possible names
        target_drivers = [
            "ODBC Driver 17 for SQL Server",
            "{ODBC Driver 17 for SQL Server}",
            "SQL Server"
        ]

        found_drivers = [d for d in all_drivers if any(target in d for target in target_drivers)]
        
        log_info("\nMatched Drivers:")
        for driver in found_drivers:
            log_info(f"✓ Found: {driver}")

        return found_drivers

    except Exception as e:
        log_error(f"ODBC Driver Check Error: {e}")
        return []

def fetch_postgres_data():
    """Fetch data from PostgreSQL and return it as a pandas DataFrame."""
    connection = None
    try:
        log_info("Connecting to PostgreSQL...")
        connection = psycopg2.connect(**POSTGRES_CONFIG)
        query = f"SELECT * FROM {POSTGRES_TABLE};"
        
        df = pd.read_sql(query, connection)
        log_info(f"Data fetched from PostgreSQL: {len(df)} rows")
        return df
    
    except psycopg2.Error as e:
        log_error(f"PostgreSQL Connection Error: {e}")
        return None
    
    finally:
        if connection:
            connection.close()
            log_info("PostgreSQL connection closed.")

def load_to_sql_server(df):
    """Load data into SQL Server from a pandas DataFrame."""
    connection = None
    cursor = None
    
    try:
        # Detailed driver check
        matched_drivers = detailed_odbc_driver_check()
        
        if not matched_drivers:
            log_error("No suitable ODBC drivers found!")
            return False

        # Use the first matched driver
        driver_to_use = matched_drivers[0]
        log_info(f"Using driver: {driver_to_use}")

        # Construct connection string
        connection_string = (
            f"DRIVER={{{driver_to_use}}};"
            f"SERVER=DESKTOP-6H1MIV8\\MSSQLSERVER101;"
            f"DATABASE={SQL_SERVER_CONFIG['database']};"
            f"UID={SQL_SERVER_CONFIG['username']};"
            f"PWD={SQL_SERVER_CONFIG['password']}"
        )
        print("driver: ",driver_to_use)
        log_info(f"Connection String: {connection_string}")

        # Attempt connection
        connection = pyodbc.connect(connection_string, timeout=10)
        cursor = connection.cursor()

        # Prepare insert query to match the new columns
        insert_query = f"""
            INSERT INTO {SQL_SERVER_TABLE} (first_name, last_name, gender, address, postcode, email, username, dob, registered, phone, picture)
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {SQL_SERVER_TABLE}
                WHERE email = ? OR username = ?
            )
            """
        
        # Prepare data for bulk insert, including the new columns
        data_to_insert = df[['first_name', 'last_name', 'gender', 'address', 'postcode', 'email', 'username', 'dob', 'registered', 'phone', 'picture']].values.tolist()
        print("data to insert",data_to_insert)

        # Modify the data to include the email and username for the WHERE condition
        data_with_condition = [
            (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[5], row[6])  # email and username for check
            for row in data_to_insert
        ]

        # Bulk insert with condition
        cursor.executemany(insert_query, data_with_condition)
        connection.commit()

        log_info(f"Successfully loaded {len(data_to_insert)} rows into SQL Server")
        return True

    except Exception as e:
        log_error(f"Error loading data into SQL Server: {e}")
        return False
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            log_info("SQL Server connection closed.")

def main():
    # Fetch data from PostgreSQL
    df = fetch_postgres_data()

    if df is not None and not df.empty:
        # Load data into SQL Server
        success = load_to_sql_server(df)
        
        if success:
            log_info("Data transfer completed successfully!")
        else:
            log_error("Data transfer failed!")

if __name__ == "__main__":
    main()