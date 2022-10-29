# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()


# COMMAND ----------

df = spark.read.option("header",True).csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #inferSchema parameter

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True', delimiter=',').csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # providing Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("course", StringType(), True),
                    StructField("roll", StringType(), True),
                    StructField("marks", IntegerType(), True),
                    StructField("email", StringType(), True)
])

# COMMAND ----------

df = spark.read.options(header='True').schema(schema).csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #creating DF from RDD

# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("RDD")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x != headers).map(lambda x: x.split(','))
rdd = rdd.map(lambda x: [int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]])
rdd.collect()

# COMMAND ----------

columns = headers.split(',')
dfRdd = rdd.toDF(columns)
dfRdd.show()
dfRdd.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("course", StringType(), True),
                    StructField("roll", StringType(), True),
                    StructField("marks", IntegerType(), True),
                    StructField("email", StringType(), True)
])

# COMMAND ----------

dfRdd = spark.createDataFrame(rdd, schema=schema)
dfRdd.show()
dfRdd.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC #select method

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.select("name","gender").show()

# COMMAND ----------

df.select(df.name, df.email).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("roll"), col("name")).show()

# COMMAND ----------

df.select('*').show()

# COMMAND ----------

df.select(df.columns[2:6]).show()

# COMMAND ----------

df2 = df.select(col("roll"), col("name"))
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #withColumn method

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df = df.withColumn("roll", col("roll").cast("String"))
df.printSchema()

# COMMAND ----------

df = df.withColumn("marks", col('marks') + 10)
df.show()

# COMMAND ----------

df = df.withColumn("aggregated marks", col('marks') - 10)
df.show()

# COMMAND ----------

df = df.withColumn("name", lit("USA"))
df.show()

# COMMAND ----------

df = df.withColumn("marks", col("marks") - 10).withColumn("updated marks", col("marks") + 20).withColumn("Country", lit("USA"))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #withColumnRenamed method

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df = df.withColumnRenamed("gender","sex").withColumnRenamed("roll", "roll number")
df.show()

# COMMAND ----------

df.select(col("name").alias("Full Name")).show()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #filter method

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.filter(df.course == "DB").show()

# COMMAND ----------

df.filter(col("course") == "DB").show()

# COMMAND ----------

df.filter( (df.course == "DB") & (df.marks > 50) ).show()

# COMMAND ----------

courses = ["DB", "Cloud", "OOP", "DSA"]
df.filter(df.course.isin(courses)).show()

# COMMAND ----------

df.filter(df.course.startswith("DS")).show()

# COMMAND ----------

df.filter(df.name.endswith("se")).show()

# COMMAND ----------

df.filter(df.name.contains("se")).show()

# COMMAND ----------

df.filter(df.name.like('%s%e%')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Quiz (select, withColumn, filter)

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

#create a new column with 'total_marks' having value 120
df = df.withColumn("total_marks", lit(120))
df.show()

# COMMAND ----------

#create a column 'average' find average marks of the students
df = df.withColumn("average", (col("marks") / col ("total_marks"))*100 )
df.show()

# COMMAND ----------

#find out students who have achieved more than 80% marks in OOP course
df_OOP = df.filter((df.course == "OOP") & (df.average > 80))
df_OOP.show()

# COMMAND ----------

#find out students who have achieved more than 60% marks in Cloud course
df_Cloud = df.filter((df.course == "Cloud") & (df.average > 60))
df_Cloud.show()

# COMMAND ----------

df_OOP.select("name","marks").show()

# COMMAND ----------

df_Cloud.select("name","marks").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #count, distinct, dropDuplicates methods

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.filter(df.course == "DB").count()

# COMMAND ----------

df.select("gender").distinct().show()

# COMMAND ----------

df.dropDuplicates(["gender","course"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #sort, orderBy methods

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.sort(df.marks, df.age).show()

# COMMAND ----------

df.orderBy(df.marks, df.age).show()

# COMMAND ----------

df.sort(df.marks.asc(), df.age.desc()).show()

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

df2 = df.sort(df.bonus.asc())
df2.show()

# COMMAND ----------

df3 = df.sort(df.age.desc(), df.salary.asc())
df3.show()

# COMMAND ----------

df4 = df.sort(df.age.desc(), df.bonus.desc(), df.salary.asc())
df4.show()

# COMMAND ----------

df4 = df.orderBy(df.age.desc(), df.bonus.desc(), df.salary.asc())
df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #groupBy method

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.groupBy("gender").sum("marks").show()

# COMMAND ----------

df.groupBy("gender").count().show()
df.groupBy("course").count().show()
df.groupBy("course").sum("marks").show()

# COMMAND ----------

df.groupBy("gender").max("marks").show()
df.groupBy("gender").min("marks").show()

# COMMAND ----------

df.groupBy("age").avg("marks").show()

# COMMAND ----------

df.groupBy("gender").mean("marks").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #groupBy method (multiple rows & aggregations)

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.groupBy("course").count().show()
df.groupBy("course","gender").count().show()


# COMMAND ----------

from pyspark.sql.functions import sum,avg,max,min,mean,count

df.groupBy("course","gender").agg(count("*").alias("total_enrollments"), sum("marks").alias("total_marks"), min("marks").alias("min_makrs"), max("marks"), avg("marks")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #groupBy method (filtering)

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max,min,mean,count
df.filter(df.gender == "Male").groupBy("course","gender").agg(count('*').alias("total_enrollments")).filter(col("total_enrollments") > 85).show()

# COMMAND ----------

# Alternate way
df2 = df.filter(df.gender == "Male").groupBy("course","gender").agg(count('*').alias("total_enrollments"))
df2.filter(col("total_enrollments") > 85).show()

# COMMAND ----------

#throws an error if used df.total_enrollments in filter coz modified changes are not saved to df ( if we assign to df it will work)
df.filter(df.gender == "Male").groupBy("course","gender").agg(count('*').alias("total_enrollments")).filter(df.total_enrollments > 85).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Quiz( groupBy)
# MAGIC For the quiz you’ll be using StudentData.csv
# MAGIC * Read this file in the DF
# MAGIC * Display the total numbers of students enrolled in each course
# MAGIC * Display the total number of male and female students enrolled in each course
# MAGIC * Display the total marks achieved by each gender in each course
# MAGIC * Display the minimum, maximum and average marks achieved in each course by each age group.

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum,avg,max,min,mean,count
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# 1
df.groupBy("course").count().show()
df.groupBy("course").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# 2
df.groupBy("course", "gender").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# 3
df.groupBy("course", "gender").agg(sum("marks").alias("total_marks")).show()

# COMMAND ----------

# 4
df.groupBy("course", "age").agg(min("marks"), max("marks"), avg("marks")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #For the quiz you’ll be using WordData.txt
# MAGIC * Read this file in the DF
# MAGIC * Calculate and show the count of each word present in the file

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options().text('/FileStore/tables/WordData.txt')
df.show()

# COMMAND ----------

df.groupBy('value').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #User Defined funtions - udf

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

def get_total_salary(salary):
  return salary + 100

totalSalaryUDF = udf(lambda x: get_total_salary(x), IntegerType())
df.withColumn("total_salary", totalSalaryUDF(df.salary)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Quiz(udf)
# MAGIC #For the quiz you’ll be using OfficeData.csv
# MAGIC * Read this file in the DF
# MAGIC * Create a new column increment and provide the increment to the employees on the following criteria
# MAGIC * If the employee is in NY state, his increment would be 10% of salary plus 5% of bonus
# MAGIC * If the employee is in CA state, his increment would be 12% of salary plus 3% of bonus

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

def get_incr(state, salary, bonus):
  sum = 0
  if state == "NY":
    sum = salary * 0.10
    sum += bonus * 0.05
  elif state == "CA":
    sum = salary * 0.12
    sum += bonus * 0.03
  return sum

incrUDF = udf(lambda x,y,z: get_incr(x,y,z), DoubleType())

df.withColumn("increment", incrUDF(df.state, df.salary, df.bonus)).show()

# COMMAND ----------

(90000  * 0.10) + (10000 * 0.05)


# COMMAND ----------

# MAGIC %md
# MAGIC #DF to RDD

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

type(df)

# COMMAND ----------

rdd = df.rdd
type(rdd)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd.filter(lambda x: x[0] == 28 ).collect()

# COMMAND ----------

rdd.filter(lambda x: x["gender"] == "Male" ).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #Spark SQL

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.createOrReplaceTempView("Student")

# COMMAND ----------

spark.sql("select course, gender, count(*) from Student group by course, gender").show()
df.groupBy("course", "gender").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #write function

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
# df.show()
df.count()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df = df.groupBy("course","gender").count()

# COMMAND ----------

# overwrite
# append
# ignore
# error

# COMMAND ----------

df.write.mode("overwrite").options(header='True').csv('/FileStore/tables/StudentData/output')

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData/output')
df.show()

# COMMAND ----------

