from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import os
import json


def fetch_and_store_to_s3(templates_dict, **kwargs):
    api_key = ""      
    aws_access_key = "" 
    aws_secret_key = "" 
    aws_region = "ap-northeast-2"

   #코드 추가


def analyze_and_store_to_s3(templates_dict, **kwargs):
    # 🎯 문제 4: S3에서 원본 데이터 다운로드 (7일 전 기준)
    import boto3
    import json
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import explode

    yyyymmdd = templates_dict['yyyymmdd']
    yymmdd = templates_dict['yymmdd']

    aws_access_key = "" 
    aws_secret_key = "" 
    aws_region = "ap-northeast-2"
    s3_bucket = ""
    s3_input_key = f"subway/source_data/{yyyymmdd}/{yymmdd}.json"
    s3_output_key = f"subway/analysis/{yyyymmdd}.json"
    local_input_path = f"/tmp/{yymmdd}.json"

   #코드 추가

   
# -------------------------
# DAG 정의 및 Task 구성
# -------------------------

with DAG(
    dag_id='fetch_subway_data_with_yymmdd_filename',
    default_args=default_args,
    schedule='@daily',
    catchup=True,
    tags=['subway', 's3', 'api'],
    description='서울시 지하철 데이터를 yymmdd 형식 파일로 S3에 저장',
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_store_to_s3',
        python_callable=fetch_and_store_to_s3,
        templates_dict={
            'api_date': "{{ macros.ds_format(macros.ds_add(ds, -7), '%Y-%m-%d', '%Y%m%d') }}",
            'yyyymmdd': "{{ macros.ds_format(macros.ds_add(ds, -7), '%Y-%m-%d', '%Y%m%d') }}",
            'yymmdd': "{{ macros.ds_format(macros.ds_add(ds, -7), '%Y-%m-%d', '%y%m%d') }}"
        }
    )

    analyze_task = PythonOperator(
        task_id='analyze_and_store_to_s3',
        python_callable=analyze_and_store_to_s3,
        templates_dict={
            'yyyymmdd': "{{ macros.ds_format(macros.ds_add(ds, -7), '%Y-%m-%d', '%Y%m%d') }}",
            'yymmdd': "{{ macros.ds_format(macros.ds_add(ds, -7), '%Y-%m-%d', '%y%m%d') }}"
        }
    )

    # Task 연결
    fetch_task >> analyze_task
