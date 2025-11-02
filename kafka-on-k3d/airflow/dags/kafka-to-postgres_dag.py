from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime
from airflow.operators.python import PythonOperator

def say_hello():
    print("👋 Hello from Airflow DAG!")
    return "DAG executed successfully."

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 11),
}

with DAG(
    dag_id='spark_submit_example',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )



    submit_spark_job = KubernetesPodOperator(
        task_id='submit_spark_operator_job',
        name='spark-submit-job',
        namespace='airflow',
        service_account_name='airflow-kubectl',  # ✅ RBAC 권한 있는 계정 지정
        image='bitnami/kubectl:latest',  # kubectl CLI가 들어있는 lightweight 이미지
        cmds=['/bin/sh', '-c'],
        arguments=['kubectl apply -f /opt/airflow/dags/spark-consume.yaml'],
        env_vars={
            'KUBECONFIG': '/root/.kube/config'
        },
        get_logs=True,
    )


    # 실행 순서 (단일 Task이므로 그냥 등록)
    hello_task >> submit_spark_job

    # spark_submit_task = SparkSubmitOperator(
    #     task_id='submit_spark_job',
    #     application='/opt/bitnami/spark/jobs/consume_kafka_to_postgres_batch.py',
    #     conn_id='spark-default',
    #     conf={
    #         'spark.master': 'spark://spark-master:7077',
    #     },
    #     jars=','.join([
    #         '/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar',
    #         '/opt/bitnami/spark/jars/kafka-clients-3.6.1.jar',
    #         '/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar',
    #         '/opt/bitnami/spark/jars/commons-pool2-2.11.1.jar',
    #         '/opt/bitnami/spark/jars/postgresql-42.7.1.jar',
    #     ]),
    #     verbose=True,
    # )