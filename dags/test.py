import pyodbc
from typing import Optional

class SQLServerConnection:
    def __init__(self, server: str, database: str, username: Optional[str] = None, password: Optional[str] = None, trusted_connection: bool = True):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.conn = None
        
    def get_connection_string(self) -> str:
        """Generate the connection string based on authentication method"""
        if self.trusted_connection:
            # Windows Authentication
            return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=yes;'
        else:
            # SQL Server Authentication
            return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};TrustServerCertificate=yes;'
    
    def connect(self):
        """Establish connection to SQL Server"""
        try:
            self.conn = pyodbc.connect(self.get_connection_string())
            print("Successfully connected to SQL Server")
            return self.conn
        except pyodbc.Error as e:
            print(f"Error connecting to SQL Server: {str(e)}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            print("Connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

if __name__ == "__main__":

    # Using SQL Server Authentication
    db = SQLServerConnection(
        server="host.docker.internal\\MSSQLSERVER101",  # Full instance name
        username="younes",
        password="Younes921722",
        database="AdventureWorksLT2022",
        trusted_connection=False)
    
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

import socket
import platform

print(f"OS: {platform.system()}")
print(f"Hostname: {socket.gethostname()}")
