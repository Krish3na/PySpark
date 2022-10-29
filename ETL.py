# Databricks notebook source
# Databricks notebook source
#dbutils.fs.rm("/FileStore/tables", True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, explode
import pyspark.sql.functions as f

# COMMAND ----------

# Extract
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
df = spark.read.text("/FileStore/tables/WordData1.txt")
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

# Transformation
df2 = df.withColumn("splitedData", f.split("value"," "))
df3 = df2.withColumn("words", explode("splitedData"))
wordsDF = df3.select("words")
wordCount = wordsDF.groupBy("words").count()
wordCount.show()

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df3)

# COMMAND ----------

# Load

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://database-1.ccfv0t9zoob8.us-east-1.rds.amazonaws.com/"
table = "pyspark_rds.WordCount"
user = "postgres"
password = "abcde12345"

wordCount.write.format("jdbc").option("driver", driver).option("url",url).option("dbtable", table).option("mode", "append").option("user",user).option("password", password).save()


# COMMAND ----------

