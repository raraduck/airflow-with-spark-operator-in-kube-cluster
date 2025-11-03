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

from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
vecTrainDF = vecAssembler.transform(trainDF)
vecTrainDF.select("bedrooms", "features", "price").show(10)

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="price")
lrModel = lr.fit(vecTrainDF)

m = round(lrModel.coefficients[0], 2)
b = round(lrModel.intercept, 2)
print(f"""The formula for the linear regression line is price = {m}*bedrooms + {b}""")

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vecAssembler, lr])
pipelineModel = pipeline.fit(trainDF)

predDF = pipelineModel.transform(testDF)
predDF.select("bedrooms", "features", "price", "prediction").show(10)

from pyspark.ml.feature import OneHotEncoder, StringIndexer

categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
indexOutputCols = [x + "Index" for x in categoricalCols]
oheOutputCols = [x + "OHE" for x in categoricalCols]

stringIndexer = StringIndexer(
    inputCols=categoricalCols, 
    outputCols=indexOutputCols,
    handleInvalid="skip"
    )
oheEncoder = OneHotEncoder(
    inputCols=indexOutputCols,
    outputCols=oheOutputCols
    )
numericCols = [field for (field, dataType) in trainDF.dtypes 
               if ((dataType == "double") & (field != "price"))]
assemblerInputs = oheOutputCols + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs,
                               outputCol="features")

from pyspark.ml.feature import RFormula

rFormula = RFormula(formula="price ~ .",
                    featuresCol="features",
                    labelCol="price",
                    handleInvalid="skip")

lr = LinearRegression(labelCol="price", featuresCol="features")
pipeline = Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])
# pipeline = Pipeline(stages = [rFormula, lr])

pipelineModel = pipeline.fit(trainDF)
predDF = pipelineModel.transform(testDF)
predDF.select("features", "price", "prediction").show(5)