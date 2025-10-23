from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_airflow_dag",
    description="A simple DAG that prints Hello Airflow every 5 minutes",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # 매 5분마다
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello Airflow'",
    )

    hello_task

