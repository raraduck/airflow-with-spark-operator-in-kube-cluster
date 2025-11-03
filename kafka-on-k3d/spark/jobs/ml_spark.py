from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Spark 세션 생성
spark = SparkSession.builder.appName("SparkTestJob").getOrCreate()

filePath = """/opt/spark-data/sf-airbnb-clean.parquet/"""
airbnbDF = spark.read.parquet(filePath)
airbnbDF.select("neighborhood_cleansed", "room_type", "bedroombs", "bathrooms", "number_of_reviews", "price").show(5)
