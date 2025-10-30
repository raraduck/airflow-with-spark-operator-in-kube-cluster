from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1️⃣ Kafka 및 Postgres 환경설정
KAFKA_BROKERS = "kafka.kafka.svc.cluster.local:9092" 
TOPIC_NAME = "user_events"

POSTGRES_URL = "jdbc:postgresql://airflow-postgresql:5432/airflow"
POSTGRES_USER = "airflow"
POSTGRES_PW = "airflow"
POSTGRES_TABLE = "user_events_stream"

# 2️⃣ SparkSession 생성
spark = SparkSession.builder \
    .appName("KafkaToPostgresBatch") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
        "org.postgresql:postgresql:42.6.0"
    ) \
    .getOrCreate()
    # .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \

spark.sparkContext.setLogLevel("WARN")

# 3️⃣ Kafka에서 메시지 읽기 (배치모드)
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

df_raw = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# 4️⃣ JSON 스키마 정의
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 5️⃣ JSON 파싱
df_parsed = df_raw \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("timestamp", "event_time")

print("About to show parsed df")
df_parsed.show(truncate=False)
print("After show")


# 6️⃣ PostgreSQL에 저장 (테이블 매번 덮어쓰기)
df_parsed.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", POSTGRES_TABLE) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PW) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print(f"✅ PostgreSQL({POSTGRES_TABLE})에 Kafka 메시지 {df_parsed.count()}건 저장 완료.")
spark.stop()
