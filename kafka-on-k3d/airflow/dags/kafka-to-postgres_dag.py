from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from kubernetes.client import models as k8s

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
        task_id="submit_spark_application" ,
        # config_file="/opt/airflow/.kube/config",
        # in_cluster=False,
        in_cluster=True,              # ✅ 클러스터 내부 ServiceAccount로 인증
        namespace="airflow", # "spark-operator",
        application_file="spark-consume.yaml", # /opt/airflow/dags/spark-consume.yaml
        # application_file=r"/opt/airflow/dags/spark-consume.yaml",  # raw string
        # do_xcom_push=False,
        # volumes=[
        #     k8s.V1Volume(
        #         name='airflow-dags',
        #         persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        #             claim_name='airflow-dags'   # ✅ PVC 이름 확인 필요
        #         )
        #     )
        # ],
        # volume_mounts=[
        #     k8s.V1VolumeMount(
        #         name='airflow-dags',
        #         mount_path='/opt/airflow/dags',
        #         read_only=False
        #     )
        # ]
    )

    hello_task >> spark_submit