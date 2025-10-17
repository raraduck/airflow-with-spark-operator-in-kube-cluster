# Airflow 3.1.0 공식 이미지 기반
FROM apache/airflow:3.1.0

USER root

# -------------------------------------------------------
# 1️⃣ OS 패키지 업데이트 및 OpenJDK 17 설치
# -------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------
# 2️⃣ PySpark 및 Airflow Spark Provider 설치
# -------------------------------------------------------
# Airflow와 PySpark의 버전 호환:
#   Airflow 3.1.0 → Spark provider 4.11.3 이상 / PySpark 3.5.x 호환
RUN pip install --no-cache-dir \
      pyspark==3.5.0 \
      apache-airflow-providers-apache-spark==4.11.3

# -------------------------------------------------------
# 3️⃣ JAVA_HOME 환경변수 설정
# -------------------------------------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# -------------------------------------------------------
# 4️⃣ 권한 복귀
# -------------------------------------------------------
USER airflow
