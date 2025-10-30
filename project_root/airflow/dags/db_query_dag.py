from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2


# Python callable: PostgreSQL 연결 후 SELECT 실행
def query_postgres(**context):
    # 1️⃣ Airflow Connection 불러오기
    conn = BaseHook.get_connection("my_postgres")

    # 2️⃣ Variable 에서 테이블명 가져오기
    table_name = Variable.get("target_table", default_var="public.sample_table")

    # 3️⃣ DB 접속
    connection = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )

    cursor = connection.cursor()
    sql = f"SELECT * FROM {table_name} LIMIT 10;"
    cursor.execute(sql)
    rows = cursor.fetchall()

    # 4️⃣ 결과 로그 출력
    for r in rows:
        print(r)

    cursor.close()
    connection.close()


# DAG 정의
with DAG(
    dag_id="db_query_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "postgres", "variable"],
    description="Use Airflow Variable & Connection to query PostgreSQL"
) as dag:

    run_query = PythonOperator(
        task_id="run_postgres_query",
        python_callable=query_postgres,
        # provide_context=True,
    )

    run_query
