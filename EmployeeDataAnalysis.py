# Databricks notebook source
# MAGIC %md
# MAGIC # For the project we’ll be using OfficeDataProject.csv
# MAGIC * Read data from the file in the DF and perform following analytics on it.
# MAGIC * Print the total number of employees in the company
# MAGIC * Print the total number of departments in the company
# MAGIC * Print the department names of the company
# MAGIC * Print the total number of employees in each department
# MAGIC * Print the total number of employees in each state
# MAGIC * Print the total number of employees in each state in each department
# MAGIC * Print the minimum and maximum salaries in each department and sort salaries in ascending order
# MAGIC * Print the names of employees working in NY state under Finance department whose bonuses are greater than the average bonuses of employees in NY state
# MAGIC * Raise the salaries $500 of all employees whose age is greater than 45
# MAGIC * Create DF of all those employees whose age is greater than 45 and save them in a file

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.functions import sum,avg,max,min,mean,count
spark = SparkSession.builder.appName("Mini Project").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeDataProject.csv')
df.show()

# COMMAND ----------

# 1 
df.count()

# COMMAND ----------

# 2
#df.groupBy("department").count().show()
#df.groupBy("department").count().count()
df.select("department").dropDuplicates(["department"]).count()

# COMMAND ----------

# 3
df.select("department").dropDuplicates(["department"]).show()

# COMMAND ----------

# 4
df.groupBy('department').count().show()

# COMMAND ----------

# 5
df.groupBy('state').count().show()

# COMMAND ----------

# 6
df.groupBy('state','department').count().show()

# COMMAND ----------

df.show()

# COMMAND ----------

# 7
df.groupBy("department").agg(min("salary").alias("min"), max("salary").alias("max")).orderBy(col("max").asc(), col("min").asc()).show()

# COMMAND ----------

df.show()

# COMMAND ----------

# 8
avgBonus = df.filter(df.state == "NY").groupBy("state").agg(avg("bonus").alias("avg_bonus")).select("avg_bonus").collect()[0]['avg_bonus']
print(avgBonus)
df.filter((df.state == "NY") & (df.department == "Finance") & (df.bonus > avgBonus)).show()

# COMMAND ----------

# 9
def incr_salary(age, currentSalary):
  if age > 45:
    return currentSalary + 500
  return currentSalary

incrSalaryUDF = udf(lambda x,y : incr_salary(x,y), IntegerType())
df.withColumn("salary", incrSalaryUDF(col("age"), col("salary"))).show()

# COMMAND ----------

#10
df.filter(df.age > 45).write.csv("/FileStore/tables/output_45")

# COMMAND ----------

