from config.db_config import get_db_connection

class UserModel:
    def __init__(self):
        self.connection = get_db_connection()

    def create_table(self):
        """Create the 'users' table if it doesn't exist."""
        connection = self.connection
        if connection:
            try:
                cursor = connection.cursor()

                # Check if the table exists
                check_table_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'users'
                );
                """
                cursor.execute(check_table_query)
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    print("Table 'users' already exists.")
                else:
                    # SQL statement to create the table
                    create_table_query = """
                    CREATE TABLE users (
                        id SERIAL PRIMARY KEY,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        gender VARCHAR(10),
                        address TEXT,
                        postcode VARCHAR(20),
                        email VARCHAR(150),
                        username VARCHAR(100),
                        dob TIMESTAMP,
                        registered TIMESTAMP,
                        phone VARCHAR(20),
                        picture VARCHAR(255)
                    );
                    """
                    # Execute the SQL query
                    cursor.execute(create_table_query)
                    connection.commit()
                    print("Table 'users' created successfully!")

            except Exception as e:
                print(f"Error while creating table: {e}")
            finally:
                cursor.close()

    def insert_user(self, data):
        
        connection = self.connection
        if connection:
            try:
                cursor = connection.cursor()
                
                insert_query = """
                INSERT INTO users_data (first_name, last_name, gender, address, postcode, email, username, dob, registered, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                
                cursor.execute(insert_query, (
                        data["first_name"], data["last_name"], data["gender"], data["address"],
                        data["postcode"], data["email"], data["username"], data["dob"],
                        data["registered"], data["phone"], data["picture"]
                        ))
                
                # Commit the transaction to insert data
                connection.commit()
                print("User inserted successfully!")
            except Exception as e:
                print(f"Error inserting user: {e}")
                connection.rollback()  # Rollback in case of error
            finally:
                cursor.close()
