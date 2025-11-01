FROM docker.io/library/spark:4.0.0
WORKDIR /opt/spark-data
COPY jobs/consume_kafka_batch.py .
CMD ["spark-submit", "/opt/spark-data/analyze_csv.py"]




# FROM bitnamilegacy/spark:3.5.4

# COPY ./jars /opt/bitnami/spark/jars/
# COPY ./jobs /opt/bitnami/spark/jobs/

# # # Dockerfile.spark
# # FROM bitnamilegacy/spark:3.4.1
# # COPY ./spark/jars /opt/bitnami/spark/jars/
# # USER root
# RUN pip install python-dotenv pyspark pandas numpy matplotlib scikit-learn kafka-python
# # USER 1001
