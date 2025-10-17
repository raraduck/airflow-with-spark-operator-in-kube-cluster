from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, split

spark = SparkSession.builder \
    .appName("WordCountTest") \
    .getOrCreate()

data = [
    "hello world",
    "hello docker",
    "hello spark",
    "docker is fun"
]

rdd = spark.sparkContext.parallelize(data).map(lambda x: Row(line=x))
df = rdd.toDF()

words = df.select(explode(split(df.line, " ")).alias("word"))
word_count = words.groupBy("word").count()

word_count.show()

spark.stop()
