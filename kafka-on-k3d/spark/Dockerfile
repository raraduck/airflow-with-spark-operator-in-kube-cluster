FROM docker.io/library/spark:3.5.4
WORKDIR /opt/spark-data

RUN pip install python-dotenv pyspark pandas numpy matplotlib scikit-learn kafka-python

COPY jars /opt/spark/jars/

# CMD ["spark-submit", "/opt/spark-data/jobs/consume_kafka_batch.py"]




# FROM bitnamilegacy/spark:3.5.4

# COPY ./jars /opt/bitnami/spark/jars/
# COPY ./jobs /opt/bitnami/spark/jobs/

# # # Dockerfile.spark
# # FROM bitnamilegacy/spark:3.4.1
# # COPY ./spark/jars /opt/bitnami/spark/jars/
# # USER root
# RUN pip install python-dotenv pyspark pandas numpy matplotlib scikit-learn kafka-python
# # USER 1001
