import psycopg2
from psycopg2 import sql

# Define your database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'ferneytupapa',
    'host': 'proyectobigdata.cbfryhd4vdmu.us-east-1.rds.amazonaws.com',
    'port': '5432'
}

try:
    # Establish the connection
    conn = psycopg2.connect(**db_params)
    print("Connection successful")

    # Create a cursor object
    cursor = conn.cursor()

    # Example query execution
    cursor.execute("SELECT * FROM juegos;")
    db_version = cursor.fetchone()
    print(f"PostgreSQL version: {db_version}")

    # Commit any changes if necessary
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
    print("Connection closed")

except Exception as e:
    print(f"An error occurred: {e}")
