from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'alextanskii',
    'start_date': datetime(2025, 4, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'run_dbt_transformations',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # every 5 min
    catchup=False
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='./run_dbt.sh',
        dag=dag
    )