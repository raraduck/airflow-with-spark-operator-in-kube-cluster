from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("👋 Hello from Airflow DAG using SparkKubernetesOperator!")
    return "DAG executed successfully."

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 11),
}

with DAG(
    dag_id='spark_kubernetes_example',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    # (1) 단순 파이썬 Task
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )

    # (2) Spark Application 생성 (SparkKubernetesOperator)
    spark_submit = SparkKubernetesOperator(
        task_id='submit_spark_application',
        namespace='default',   # SparkApplication이 실행될 namespace (spark-operator와 동일)
        application_file='/opt/airflow/dags/spark-consume.yaml',  # ✅ SparkApplication YAML 파일 경로
        kubernetes_conn_id='kubernetes_default',  # Airflow가 기본 제공하는 Kubernetes 연결
        do_xcom_push=True,  # SparkApplication 상태를 XCom으로 반환
    )

    # (3) Spark Application 상태 모니터링 (SparkKubernetesSensor)
    monitor_spark = SparkKubernetesSensor(
        task_id='monitor_spark_application',
        namespace='default',
        application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_application')['metadata']['name'] }}",
        kubernetes_conn_id='kubernetes_default',
    )

    hello_task >> spark_submit >> monitor_spark