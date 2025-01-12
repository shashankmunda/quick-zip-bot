from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

# Initialize the connection pool
db_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1, 
    maxconn=10,  # adjust based on your needs
    host=os.environ['DATABASE_HOST'],
    database=os.environ['DATABASE_NAME'],
    user=os.environ['DATABASE_USER'],
    password=os.environ['DATABASE_PASSWORD']
)

# Get a connection from the pool
def get_connection():
    return db_pool.getconn()

# Release the connection back to the pool
def release_connection(conn):
    db_pool.putconn(conn)