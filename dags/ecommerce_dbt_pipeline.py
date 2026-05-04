"""
eCommerce dbt Pipeline DAG
Runs dbt transformations on a schedule
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'beena',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'ecommerce_dbt_pipeline',
    default_args=default_args,
    description='Run dbt models for eCommerce analytics',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'dbt', 'analytics'],
) as dag:

    # Task 1: Run dbt staging models
    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='echo "dbt run --select staging"',
    )

    # Task 2: Run dbt marts models
    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='echo "dbt run --select marts"',
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='echo "dbt test"',
    )

    # Set task dependencies
    dbt_staging >> dbt_marts >> dbt_test