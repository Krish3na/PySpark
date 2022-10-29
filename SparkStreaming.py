# Databricks notebook source
# Databricks notebook source
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Streaming")
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Streaming using RDD

# COMMAND ----------

rdd = ssc.textFileStream("/FileStore/tables/")

# COMMAND ----------

rdd = rdd.map(lambda x: (x,1))
rdd = rdd.reduceByKey(lambda x,y : x+y)
rdd.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(1000000)

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Streaming using DF

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Sparking Streaming DF").getOrCreate()
word = spark.readStream.text("/FileStore/tables")
word = word.groupBy("value").count()
word.writeStream.format("console").outputMode("complete").start()

# COMMAND ----------

display(word)