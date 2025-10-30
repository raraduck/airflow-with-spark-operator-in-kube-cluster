FROM apache/airflow:2.8.1-python3.10

USER root

# Java 설치 (Spark에 필요)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_HOME=/opt/spark

# Spark 설치 (Spark client만 필요)
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

ENV PATH="${SPARK_HOME}/bin:$PATH"

USER airflow

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
