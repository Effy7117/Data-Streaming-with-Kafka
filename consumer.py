import json
from kafka import KafkaConsumer
import psycopg2
from datetime import datetime
import logging

# Configure logging to write into a file
logging.basicConfig(level=logging.INFO, filename='consumer.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to postgres to disconnect all active sessions and drop the database if exists
conn = psycopg2.connect(user = "postgres", 
                        host= '127.0.0.1',
                        password = "Myupsd7*",
                        port = 5432)
conn.autocommit = True

# Disconnect active sessions
with conn.cursor() as cursor:
    cursor.execute("""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'assignment6_db'
          AND pid <> pg_backend_pid();
    """)

# Drop the database
with conn.cursor() as cursor:
    cursor.execute("""
        DROP DATABASE IF EXISTS assignment6_db;
    """)
conn.close()

# Connect to postgres to create the database
conn = psycopg2.connect(user = "postgres", 
                        host= '127.0.0.1',
                        password = "Myupsd7*",
                        port = 5432)
conn.autocommit = True

# Create the database
cur = conn.cursor()
cur.execute("""
                CREATE DATABASE assignment6_db
            """)
conn.commit()
cur.close()
conn.close()

# Connect to the database assignment4_db
db_params = {
    'user': 'postgres',
    'password': 'Myupsd7*',
    'host': '127.0.0.1',
    'database': 'assignment6_db',
    'port': 5432
}
conn = psycopg2.connect(**db_params)
cur = conn.cursor()


# Function to create table in PostgreSQL database
def create_table():
    cur.execute('''CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(50) NOT NULL,
                    temperature FLOAT NOT NULL,
                    wind_speed FLOAT NOT NULL,
                    humidity FLOAT NOT NULL,
                    received_at TIMESTAMP NOT NULL)''')
    conn.commit()
    

# Define Kafka broker address
bootstrap_servers = ['localhost:9092']

# Create Kafka consumer
consumer = KafkaConsumer('weather-data',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         group_id='weather-group')
                         
# Function to insert data into weather_data table
def insert_to_database(data, received_at):
    city = data["city"]
    temperature = data["temperature"]
    wind_speed = data["wind_speed"]
    humidity = data["humidity"]
    cur.execute(f"INSERT INTO weather_data (city, temperature, wind_speed, humidity, received_at) VALUES ('{city}', {temperature}, {wind_speed}, {humidity}, '{received_at}')")
    conn.commit()

if __name__ == "__main__":
    create_table()  
    for message in consumer:
        weather_data = json.loads(message.value.decode('utf-8'))
        received_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Consumed: {weather_data}, Received at: {received_at}")
        insert_to_database(weather_data, received_at)
        logger.info(f"Inserted to Database: {weather_data}, Received at: {received_at}")

