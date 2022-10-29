# Databricks notebook source
# MAGIC %md
# MAGIC #Full load Capture

# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql import SparkSession

spark= SparkSession.builder.appName("CDC").getOrCreate()

# COMMAND ----------

fldf = spark.read.csv("/FileStore/tables/LOAD00000001.csv") #full load dataframe
display(fldf)

# COMMAND ----------

fldf = fldf.withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "FullName").withColumnRenamed("_c2","City")
display(fldf)
fldf.write.mode("overwrite").csv("/FileStore/tables/finalOutput/finalFile.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #Change Data Capture

# COMMAND ----------

udf = spark.read.csv("/FileStore/tables/20220706_192146951.csv")
display(udf)

# COMMAND ----------

udf = udf.withColumnRenamed("_c0", "action").withColumnRenamed("_c1", "id").withColumnRenamed("_c2", "FullName").withColumnRenamed("_c3","City")
display(udf)
ffdf = spark.read.csv("/FileStore/tables/finalOutput/finalFile.csv")
ffdf = ffdf.withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "FullName").withColumnRenamed("_c2", "City")
display(ffdf)

# COMMAND ----------

for row in udf.collect():
    print(row)
    if row["action"] == "U":
        print("Update")
        ffdf = ffdf.withColumn("FullName", when(ffdf["id"] == row["id"], row["FullName"]).otherwise(ffdf["FullName"]))
        ffdf = ffdf.withColumn("City", when(ffdf["id"] == row["id"], row["City"]).otherwise(ffdf["City"]))
        
    if row["action"] == "I":
        insertedRow = [list(row)[1:]]
        print(insertedRow)
        print("Insert")
        columns= ["id", "FullName", "City"]
        newdf = spark.createDataFrame(insertedRow, columns)
        ffdf = ffdf.union(newdf)
        
        
    if row["action"] == "D":
        print("Delete")
        ffdf = ffdf.filter(ffdf["id"] != row["id"])
        

# COMMAND ----------

display(ffdf)
ffdf.write.mode("overwrite").csv("/FileStore/tables/finalOutput/finalFile_ffdf.csv")


# COMMAND ----------

