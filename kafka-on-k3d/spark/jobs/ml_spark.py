from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Spark 세션 생성
spark = SparkSession.builder.appName("SparkTestJob").getOrCreate()

filePath = """/opt/spark-data/sf-airbnb-clean.parquet/"""
airbnbDF = spark.read.parquet(filePath)
airbnbDF.select(
    "neighbourhood_cleansed", 
    "room_type", 
    "bedrooms", 
    "bathrooms", 
    "number_of_reviews", 
    "price"
    ).show(5)

trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
print(f"""There are {trainDF.count()} rows in the training set, and {testDF.count()} in the test set""")


