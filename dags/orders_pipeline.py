from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'kelompok27',
    'start_date' : datetime(2024, 1, 1),
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1)
}

with DAG(
    "orders_pipeline",                       
    default_args=default_args,
    schedule_interval='@daily',              
    catchup=False,
    max_active_runs=1,
    description="Orders API -> Spark -> ClickHouse"
) as dag:

    fetch_orders = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/fetch_data.py"
    )

    process_orders = BashOperator(
        task_id="process_orders_spark",
        bash_command="python /opt/airflow/dags/process_orders.py"
    )

    fetch_orders >> process_orders
