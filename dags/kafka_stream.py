from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

import json
import requests
import uuid
import time
import logging
import psycopg2


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 4, 15, 10, 00)
}

def get_data():
    response = requests.get('https://randomuser.me/api/')
    data = response.json().get('results')

    # return data
    return data

def format_data(response):
    data = {}

    #to process
    data['data'] = response

    #return data
    return data

def stream_data():
    current_time = time.time()

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
                    user_id UUID PRIMARY KEY,
                    raw_api_data JSONB
                );
            """)
    except Exception as db_conn_error:
        logging.error(f"Postgres connection error: {db_conn_error}")

    while True:
        if time.time() > current_time + 60: #1 minute
            break
        try:
            response = format_data(get_data())
            producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=500)
            producer.send('users_created', json.dumps(response).encode('utf-8'))

            # Insert into Postgres
            insert_query = """
                            INSERT INTO public.users_raw (user_id, raw_api_data)
                            VALUES (%s, %s);
                        """
            cursor.execute(insert_query, (str(uuid.uuid4()), json.dumps(response)))

        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

    # cursor.close()
    # conn.close()

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )