from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.hooks.base import BaseHook
import boto3
import os

aws_conn = BaseHook.get_connection("aws_default")
# --- 환경 변수로 AWS 접근 키를 설정 (Airflow Connection 또는 Env에서 가져와야 함)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", aws_conn.login)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", aws_conn.password)
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
# --- 다운로드 대상 설정
S3_BUCKET = "databricks-workspace-stack-60801-bucket" # "samples/" # "your-bucket-name"
S3_KEY = "samples/data.csv" # "path/to/your/file.csv"       # 예: data/sample.csv
# LOCAL_SAVE_PATH = "/opt/airflow/dags/file.csv" # "/opt/airflow/data/file.csv"
LOCAL_SAVE_PATH = "/opt/airflow/dags/file.csv" # "/opt/airflow/data/file.csv"

# DAG 정의
with DAG(
    dag_id="s3_download_dag",
    description="Download CSV file from S3 using boto3 (Airflow 3.x)",
    start_date=datetime(2025, 1, 1),
    schedule=None,           # Airflow 3.x에서는 schedule_interval 대신 schedule 사용
    catchup=False,
    tags=["test", "boto3"],
) as dag:

    @task()
    def say_hello():
        print("✅ Hello, World from Airflow 3.x running on k3d!")

    @task()
    def download_csv_from_s3():
        """S3에서 CSV 파일을 다운로드합니다."""
        print("🚀 Connecting to S3...")

        # boto3 클라이언트 생성
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        # 로컬 저장 디렉토리 생성
        os.makedirs(os.path.dirname(LOCAL_SAVE_PATH), exist_ok=True)

        # 파일 다운로드
        print(f"Downloading s3://{S3_BUCKET}/{S3_KEY} → {LOCAL_SAVE_PATH}")
        s3.download_file(S3_BUCKET, S3_KEY, LOCAL_SAVE_PATH)

        print("✅ Download completed successfully!")

        # 간단한 파일 검증
        if os.path.exists(LOCAL_SAVE_PATH):
            size = os.path.getsize(LOCAL_SAVE_PATH)
            print(f"📄 File saved: {LOCAL_SAVE_PATH} ({size} bytes)")
        else:
            raise FileNotFoundError("❌ Download failed, file not found locally!")

    # Task 실행
    say_hello() >> download_csv_from_s3()


