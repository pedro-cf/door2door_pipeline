from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks.calculate_operating_periods import calculate_operating_periods
from tasks.calculate_operating_periods_metrics import calculate_operating_periods_metrics

from tasks.fetch_and_validate_bucket import fetch_and_validate_bucket
from tasks.ensure_table_creation import ensure_table_creation
from tasks.fetch_and_import_to_psql import fetch_and_import_to_psql

## Directed Acyclic Graph

default_args = {
    'owner': 'Pedro',
    'retry': 5,
    'retry_delay': timedelta(minutes=5),
    'target_date': datetime.now()
}

with DAG(
    default_args=default_args,
    dag_id='door2door',
    start_date=datetime(2023,3,10),
    schedule_interval='@daily'
) as dag:
    
    # Fetch and validate bucket data
    task_fetch_and_validate_bucket_data = PythonOperator(
        task_id='fetch_and_validate_bucket',
        python_callable=fetch_and_validate_bucket,
        op_kwargs={'target_date': '{{ dag_run.conf.get("target_date", None) or dag.default_args.target_date }}'},
        provide_context=True
    )
    
    # Ensure table creation
    task_ensure_table_creation = PythonOperator(
        task_id='ensure_table_creation',
        python_callable=ensure_table_creation,
        provide_context=True
    )
    
    # Fetch and import data to PSQL
    task_fetch_and_import_to_psql = PythonOperator(
        task_id='fetch_and_import_to_psql',
        python_callable=fetch_and_import_to_psql,
        provide_context=True
    )
    
    # Calculate operating periods
    task_calculate_operating_periods = PythonOperator(
        task_id='calculate_operating_periods',
        python_callable=calculate_operating_periods,
        provide_context=True
    )
    
    # Calculate operating periods metrics
    task_calculate_operating_periods_metrics = PythonOperator(
        task_id='calculate_operating_periods_metrics',
        python_callable=calculate_operating_periods_metrics,
        provide_context=True
    )
    
    # Set task dependencies
    task_fetch_and_validate_bucket_data >> task_ensure_table_creation >> task_fetch_and_import_to_psql >> task_calculate_operating_periods >> task_calculate_operating_periods_metrics
    