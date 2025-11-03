from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Spark 세션 생성
spark = SparkSession.builder.appName("SparkTestJob").getOrCreate()

# 샘플 데이터
data = [("Alice", 30), ("Bob", 25), ("Charlie", 28)]
df = spark.createDataFrame(data, ["name", "age"])

print("\n[1] Original Data:")
df.show()