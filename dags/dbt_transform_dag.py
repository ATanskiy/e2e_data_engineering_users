from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'alextanskii',
    'start_date': datetime(2025, 4, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'run_dbt_transformations',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # every 5 min
    catchup=False,
    max_active_runs=2
) as dag:
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command="""
                mkdir -p /tmp/dbt_logs /tmp/dbt_target &&
                export DBT_LOG_PATH=/tmp/dbt_logs/dbt.log &&
                dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt --target-path /tmp/dbt_target --select users
            """,
        dag=dag
    )