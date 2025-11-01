FROM docker.io/library/spark:3.5.4
WORKDIR /opt/spark-data

# (1) job 복사
COPY jobs /opt/spark-data/jobs/

# (2) jar는 spark 공식 이미지 경로에 복사
COPY jars /opt/spark/jars/

CMD ["spark-submit", "/opt/spark-data/jobs/consume_kafka_batch.py"]




# FROM bitnamilegacy/spark:3.5.4

# COPY ./jars /opt/bitnami/spark/jars/
# COPY ./jobs /opt/bitnami/spark/jobs/

# # # Dockerfile.spark
# # FROM bitnamilegacy/spark:3.4.1
# # COPY ./spark/jars /opt/bitnami/spark/jars/
# # USER root
# RUN pip install python-dotenv pyspark pandas numpy matplotlib scikit-learn kafka-python
# # USER 1001
