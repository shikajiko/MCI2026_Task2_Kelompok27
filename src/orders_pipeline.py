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
        bash_command="python /opt/airflow/src/dags/fetch_data.py"
    )

    process_orders = BashOperator(
        task_id="process_orders_spark",
        bash_command="python /opt/airflow/src/dags/process_orders_spark.py"
    )

    load_orders = BashOperator(
        task_id="load_orders_clickhouse",
        bash_command="python /opt/airflow/src/dags/load_orders_clickhouse.py"
    )

    fetch_orders >> process_orders >> load_orders
