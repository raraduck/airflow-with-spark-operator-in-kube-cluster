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
    aws_region = "us-east-2"
    #코드 추가
    yyyymmdd = templates_dict['api_date']
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayStatsNew/1/1000/{yyyymmdd}"
    print(url)

    try:
        response = requests.get(url)
        res = response.json()['CardSubwayStatsNew'] # ['CardSubwayStatsNew']
    except Exception as e:
        print(e)
        raise Exception(e)
        return ""

    if "ERROR" in res['RESULT']['CODE']:
        print(res['RESULT']['MESSAGE'])
        return ""

    print(res['RESULT']['MESSAGE'])
    print(res['row'])

    contents = res['row']
    # file_name = f"{yyyymmdd}/{yyyymmdd}.json"
    yymmdd = yyyymmdd[2:]
    file_name = f"{yymmdd}.json"
    os.makedirs(os.path.join("/tmp", f"{yyyymmdd}"), exist_ok=True)
    full_path = os.path.join("/tmp", f"{yyyymmdd}", file_name)

    try:
        with open(full_path, "w", encoding="utf-8") as f:
            json_str = json.dumps(contents, ensure_ascii=False) #, separators=(",", ":"))
            json_str = json_str.replace("[", "").replace("]", "").replace("},","}").replace("}","}\n") #.replace("{", "\t{")
            f.write(json_str)
    except Exception as e:
        print(f"ERROR: {file_name} 파일 저장 실패")
        raise Exception(e)
        return ""
        
    print(f"{file_name} 파일 저장 완료")
    
    s3_client = boto3.client("s3",
                             aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key,
                             region_name=aws_region
                             )
    try:
        bucket_name = f"subway-assignment-daewoon"
        object_name = f"subway/{yyyymmdd}/{file_name}"
        s3_client.upload_file(full_path, bucket_name, object_name)
    except Exception as e:
        print("ERROR: AWS 자격 증명을 찾을 수 없습니다.", e)
        raise Exception(e)

    print(f"{file_name} 파일 s3 업로드 완료")

def analyze_and_store_to_s3(templates_dict, **kwargs):
    # 🎯 문제 4: S3에서 원본 데이터 다운로드 (7일 전 기준)
    import boto3
    import json
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import explode
    from pyspark.sql.functions import col

    # print(f"hello, analyze_and_store_to_s3")

    yyyymmdd = templates_dict['yyyymmdd']
    yymmdd = templates_dict['yymmdd']

    aws_access_key = ""
    aws_secret_key = ""
    aws_region = "us-east-2"
    s3_bucket = "subway-assignment-daewoon"
    s3_input_key = f"subway/{yyyymmdd}/{yymmdd}.json"
    s3_output_key = f"subway/analysis/{yyyymmdd}.json"
    local_input_path = f"/tmp/{yymmdd}.json"

    try:
        s3_client = boto3.client("s3",
                                aws_access_key_id=aws_access_key,
                                aws_secret_access_key=aws_secret_key,
                                region_name=aws_region
                                )
        s3_client.download_file(s3_bucket, s3_input_key, local_input_path)
    except Exception as e:
        print(f"ERROR: ", e)
        raise(e)
        return ""
    
    print(f"[INFO] S3 JSON downloaded → {local_input_path}")

    try:
        spark = (
            SparkSession.builder
            .appName("subway-analysis")
            # # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.664")
            # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
            # .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            # .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .getOrCreate()
        )
    except Exception as e:
        print(f"ERROR: Spark Session 초기화 실패")
        raise Exception(e)
        return ""

    print(f"Spark Version: {spark.version}")
    print(f"Spark Session 초기화 성공")

    try:
        df = spark.read.json(local_input_path)
        print("[INFO] 로컬 JSON → DataFrame 로드 성공")
        df.show(truncate=False)
    except Exception as e:
        print(f"[ERROR] JSON 로드 실패: {e}")
        spark.stop()
        raise
    
    try:
        # df.createOrReplaceTempView("subway_data")

        df_casted = df.withColumn("GTON_TNOPE", col("GTON_TNOPE").cast("long")) \
                    .withColumn("GTOFF_TNOPE", col("GTOFF_TNOPE").cast("long"))
        df_casted.createOrReplaceTempView("subway_data")

        sql_result = spark.sql("""
            SELECT * FROM subway_data
        """)
        print("=== SQL 결과 ===")
        sql_result.show(truncate=False)
    except Exception as e:
        print(f"[ERROR] SQL 처리 실패: {e}")
        spark.stop()
        raise

    try:
        df = df_casted

        print("\n=== [데이터 스키마] ===")
        df.printSchema()

        # ✅ 3. 노선별 총 이용자 수 (승차+하차)
        total_by_line = spark.sql("""
            SELECT 
                SBWY_ROUT_LN_NM AS line_name,
                SUM(GTON_TNOPE + GTOFF_TNOPE) AS total_passengers
            FROM subway_data
            GROUP BY SBWY_ROUT_LN_NM
            ORDER BY total_passengers DESC
        """)
        total_by_line.show()

        # ✅ 4. 역별 승차/하차 수
        station_stats = spark.sql("""
            SELECT 
                SBWY_STNS_NM AS station_name,
                SUM(GTON_TNOPE) AS total_boarding,
                SUM(GTOFF_TNOPE) AS total_alighting
            FROM subway_data
            GROUP BY SBWY_STNS_NM
            ORDER BY total_boarding DESC
        """)
        station_stats.show(10)

        # ✅ 5. 승하차 비율 상위/하위
        ratio_df = spark.sql("""
            SELECT 
                SBWY_STNS_NM AS station_name,
                ROUND(SUM(GTON_TNOPE) / NULLIF(SUM(GTOFF_TNOPE), 0), 2) AS board_to_alight_ratio
            FROM subway_data
            GROUP BY SBWY_STNS_NM
            ORDER BY board_to_alight_ratio DESC
        """)
        ratio_df.show(10)

        # ✅ 6. TOP 5 이용역
        top5_stations = spark.sql("""
            SELECT 
                SBWY_STNS_NM AS station_name,
                SUM(GTON_TNOPE + GTOFF_TNOPE) AS total_usage
            FROM subway_data
            GROUP BY SBWY_STNS_NM
            ORDER BY total_usage DESC
            LIMIT 5
        """)
        top5_stations.show()

        # ✅ 7. 결과를 Python dict로 변환
        result = {
            "total_by_line": [r.asDict() for r in total_by_line.collect()],
            "station_stats": [r.asDict() for r in station_stats.collect()],
            "ratio_top10": [r.asDict() for r in ratio_df.limit(10).collect()],
            "top5_stations": [r.asDict() for r in top5_stations.collect()],
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

            # "total_by_line": [row.asDict() for row in total_by_line.collect()],
            # "station_stats": [row.asDict() for row in station_stats.collect()],
            # "ratio_top10": [row.asDict() for row in ratio_df.limit(10).collect()],
            # "top5_stations": [row.asDict() for row in top5_stations.collect()],
        }

        print("\n✅ 분석이 성공적으로 완료되었습니다.")
        print(result)

    except Exception as e:
        print("\n❌ 분석 중 오류 발생:")
        # print(traceback.format_exc())
        spark.stop()
        raise e

    try:
        # ✅ 5️⃣ dict → JSON 문자열 (UTF-8, 한글 깨짐 방지)
        json_str = json.dumps(result, ensure_ascii=False, indent=2)

        # ✅ 8️⃣ 메모리 내 문자열 바로 업`로드
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_output_key,
            Body=json_str.encode("utf-8"),
            ContentType="application/json",
        )

        print(f"\n✅ 분석 결과가 S3에 성공적으로 저장되었습니다.")
        print(f"→ s3://{s3_bucket}/{s3_output_key}")
    except Exception as e:
        print("\n 업로드 중 오류 발생:")
        spark.stop()
        raise(e)

    spark.stop()
    # s3_full_path = f"s3a://{s3_bucket}/{s3_input_key}"
    # try:

    #     df = spark.read.json(s3_full_path)
    #     df.show(10, truncate=False)
    # except Exception as e:
    #     print(f"ERROR: {e}")
    #     raise(e)
    #     return ""
    # print(f"s3 read 성공")


    # try:
    #     with open(local_input_path, "w", encoding="utf-8") as f:
    #         f.write(df)
    # except Exception as e:
    #     print(f"ERROR: {file_name} 파일 저장 실패")
    #     raise Exception(e)
    #     return ""
        
    # print(f"{file_name} 파일 저장 완료")


   #코드 추가

   
# -------------------------
# DAG 정의 및 Task 구성
# -------------------------

with DAG(
    dag_id='fetch_subway_data_with_yymmdd_filename',
    # default_args=default_args,
    schedule='@daily',
    catchup=False, # True,
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
