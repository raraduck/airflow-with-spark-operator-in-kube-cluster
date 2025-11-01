FROM docker.io/library/spark:3.5.4
WORKDIR /opt/spark-data
COPY jobs /opt/spark-data/jobs/
COPY jars /opt/bitnami/spark/jars/
CMD ["spark-submit", "/opt/spark-data/jobs/analyze_csv.py"]




# FROM bitnamilegacy/spark:3.5.4

# COPY ./jars /opt/bitnami/spark/jars/
# COPY ./jobs /opt/bitnami/spark/jobs/

# # # Dockerfile.spark
# # FROM bitnamilegacy/spark:3.4.1
# # COPY ./spark/jars /opt/bitnami/spark/jars/
# # USER root
# RUN pip install python-dotenv pyspark pandas numpy matplotlib scikit-learn kafka-python
# # USER 1001
