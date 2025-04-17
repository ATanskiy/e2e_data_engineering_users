import json
import requests
import time
import logging
import psycopg2

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


default_args = {
    'owner': 'alextanskii',
    'start_date': datetime(2025, 4, 15, 10, 00)
}

# get and return data from API
def get_data():
    response = requests.get('https://randomuser.me/api/')
    data = response.json().get('results')[0]

    return data

def connect_to_postgress_users_raw():
    # Connect to Postgres
    try:
        conn = psycopg2.connect(
            dbname='users_raw',
            user='airflow',  # Replace with your user
            password='airflow',  # Replace with your password
            host='postgres',  # Use the container service name
            port='5432'
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS public.users_raw (
                        user_id SERIAL PRIMARY KEY,
                        inserted_at TIMESTAMPTZ DEFAULT NOW(),
                        raw_api_data JSONB
                    );
                """)
        return conn, cursor
    except Exception as db_conn_error:
        logging.error(f"Postgres connection error: {db_conn_error}")
        return None, None

def insert_into_users_raw(cursor, data):
    try:
        # Insert into Postgres
        cursor.execute("""
            INSERT INTO public.users_raw (raw_api_data)
            VALUES (%s);
        """, (json.dumps(data),))
    except Exception as e:
        logging.error(f'An error occured: {e}')

def send_to_kafka(producer, data):
    try:
        producer.send('users_created', json.dumps(data).encode('utf-8'))
    except Exception as e:
        logging.error(f"Failed to send to Kafka: {e}")

def stream_data():
    current_time = time.time()
    conn, cursor = connect_to_postgress_users_raw()
    if not conn or not cursor:
        return

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=500)

    while time.time() < current_time + 60: #1 minute

        try:
            response = get_data()
            send_to_kafka(producer, response)
            insert_into_users_raw(cursor, response)
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )