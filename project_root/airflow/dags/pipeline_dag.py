from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="pipeline_dag",
    default_args=default_args,
    description="Trigger multiple DAGs in sequence",
    schedule_interval=None,  # 수동 또는 외부 트리거
    catchup=False,
    tags=["pipeline", "trigger"],
) as dag:

    # 1️⃣ 첫 번째 DAG 트리거
    trigger_hello = TriggerDagRunOperator(
        task_id="trigger_hello_airflow_dag",
        trigger_dag_id="hello_airflow_dag",
        wait_for_completion=True,   # ✅ 다음 단계로 넘어가기 전에 완료될 때까지 대기
        poke_interval=30,           # 상태 체크 간격 (초)
    )

    # 2️⃣ 두 번째 DAG 트리거
    trigger_s3 = TriggerDagRunOperator(
        task_id="trigger_s3_download_dag",
        trigger_dag_id="s3_download_dag",
        wait_for_completion=True,
        poke_interval=30,
    )

    # 3️⃣ 세 번째 DAG 트리거
    trigger_db = TriggerDagRunOperator(
        task_id="trigger_db_query_dag",
        trigger_dag_id="db_query_dag",
        wait_for_completion=True,
        poke_interval=30,
    )

    # 순차 실행 정의
    trigger_hello >> trigger_s3 >> trigger_db

