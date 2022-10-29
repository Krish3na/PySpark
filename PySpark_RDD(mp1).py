# Databricks notebook source
print("Hello World !")

# COMMAND ----------

from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("Read File") #sets basic spark config
sc = SparkContext.getOrCreate(conf = conf) #responsible for creating transformation
rdd = sc.textFile('/FileStore/tables/sample.txt') #giving path of sample file to read

# COMMAND ----------

rdd #since no action is given, when just try to retrieve data we can't get (it gives metadata that's it)

# COMMAND ----------

rdd.collect() #here action is given, now all the processing takes place and retrieves the data

# COMMAND ----------

# MAGIC %md
# MAGIC #map funtion

# COMMAND ----------

rdd2 = rdd.map(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd3 = rdd.map(lambda x: x + "ooo")
rdd3.collect()

# COMMAND ----------

def foo(x):
    l=x.split(' ')
    l2=[]
    for s in l:
        if s:
            l2.append(int(s) + 10)
    return l2

rdd4= rdd.map(foo)
rdd4.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # flatMap funtion

# COMMAND ----------

flatmapped_rdd = rdd.flatMap(lambda x: x.split(' '))
flatmapped_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # filter funtion

# COMMAND ----------

fltr_rdd = rdd.filter(lambda x: x!= '5 6 7  8')
fltr_rdd.collect()

# COMMAND ----------

def foo(x):
    if x ==  '5 6 7  8':
        return False
    else:
        return True

fltr_rdd2 = rdd.filter(foo)
fltr_rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Quiz

# COMMAND ----------

confq = SparkConf().setAppName("Quiz")
scq = SparkContext.getOrCreate(conf = confq)
rddq = scq.textFile('/FileStore/tables/text.txt')

def filterAandC(x):
    if x.startswith('a') or x.startswith('c'):
        return False
    else:
        return True

q = rddq.flatMap(lambda x: x.split(' '))
qf = q.filter(filterAandC)
qf.collect()

# COMMAND ----------

qf = q.filter(lambda x : not (x.startswith('a') or x.startswith('c')) )
qf.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # distinct funtion

# COMMAND ----------

flatMap_rdd = rdd.flatMap(lambda x: x.split(' '))
distinct_flatmap_rdd = flatMap_rdd.distinct()
distinct_flatmap_rdd.collect()

# COMMAND ----------

rdd.flatMap(lambda x: x.split(' ')).distinct().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # groupByKey function

# COMMAND ----------

confq = SparkConf().setAppName("groupBy")
scq = SparkContext.getOrCreate(conf = confq)
rddq = scq.textFile('/FileStore/tables/text.txt')

q = rddq.flatMap(lambda x: x.split(' '))
qf = q.map(lambda x : (x,len(x)))
qf.collect()

# COMMAND ----------

qf.groupByKey().collect()

# COMMAND ----------

qf.groupByKey().mapValues(list).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #reduceByKey function

# COMMAND ----------

confq = SparkConf().setAppName("reduceBy")
scq = SparkContext.getOrCreate(conf = confq)
rddq = scq.textFile('/FileStore/tables/sample.txt')

q = rddq.flatMap(lambda x: x.split(' '))
qf = q.map(lambda x : (x,1))
qf.collect()

# COMMAND ----------

qf.reduceByKey(lambda x, y : x+y).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #Quiz word count

# COMMAND ----------

confq = SparkConf().setAppName("reduceBy")
scq = SparkContext.getOrCreate(conf = confq)
rddq = scq.textFile('/FileStore/tables/text.txt')

q = rddq.flatMap(lambda x: x.split(' '))
qff = q.filter(lambda x: len(x)!=0)
qf = qff.map(lambda x : (x,1))
qfq = qf.reduceByKey(lambda x, y : x+y)
qfq.collect()

# COMMAND ----------

rddq.flatMap(lambda x: x.split(' ')).filter(lambda x: len(x)!=0).map(lambda x : (x,1)).reduceByKey(lambda x, y : x+y).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #count, countByValue function

# COMMAND ----------

confq = SparkConf().setAppName("groupBy")
scq = SparkContext.getOrCreate(conf = confq)
rddq = scq.textFile('/FileStore/tables/text.txt')

q = rddq.flatMap(lambda x: x.split(' '))
q.count()

# COMMAND ----------

rddq.flatMap(lambda x: x.split(' ')).count()

# COMMAND ----------

rddq.flatMap(lambda x: x.split(' ')).countByValue()

# COMMAND ----------

# MAGIC %md
# MAGIC # saveAsTextFile function

# COMMAND ----------

confq = SparkConf().setAppName("save")
scq = SparkContext.getOrCreate(conf = confq)
rddq = scq.textFile('/FileStore/tables/text.txt')

q = rddq.flatMap(lambda x: x.split(' '))
qf = q.map(lambda x : (x,len(x)))
qf.collect()

# COMMAND ----------

qf.getNumPartitions()

# COMMAND ----------

qf.saveAsTextFile('/FileStore/tables/output/sample_output1.txt')

# COMMAND ----------

# MAGIC %md
# MAGIC #repartition and coalesce functions

# COMMAND ----------

conf = SparkConf().setAppName("save")
sc = SparkContext.getOrCreate(conf = conf)
rdd = sc.textFile('/FileStore/tables/text.txt')
rdd1 = rdd.flatMap(lambda x: x.split(' '))
rdd2 = rdd1.map(lambda x : (x,len(x)))
rdd2.getNumPartitions()

# COMMAND ----------

rdd2 = rdd2.repartition(5) #to increase from small no. of partitions to bigger one
rdd2.getNumPartitions()

# COMMAND ----------

rdd2.saveAsTextFile('/FileStore/tables/output/5_partitions_output')
rdd2.getNumPartitions()

# COMMAND ----------

rdd3 = rdd2.coalesce(3) #to reduce from bigger no. of partitions to smaller one
rdd3.getNumPartitions()

# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/output/3__partitions_output')
rdd3.getNumPartitions()

# COMMAND ----------

rddq = sc.textFile('/FileStore/tables/output/3__partitions_output') #to read from partitioned outputs
rddq.getNumPartitions()

# COMMAND ----------

rddq.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # calculating average

# COMMAND ----------

conf = SparkConf().setAppName("average")
sc = SparkContext.getOrCreate(conf = conf)
rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0],(float(x.split(',')[2]),1)))
rdd3 = rdd2.reduceByKey(lambda x,y: (x[0] + y[0], x[1]+y[1]))
rdd4 = rdd3.map(lambda x: (x[0],x[1][0]/x[1][1]))
rdd4.collect()

# COMMAND ----------

conf = SparkConf().setAppName("Average")
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0],   (int(x.split(',')[1]),1)  ))
rdd3 = rdd2.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
rdd4 = rdd3.map(lambda x: (x[0],x[1][0]/x[1][1]))

# COMMAND ----------

rdd4.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #finding min and max

# COMMAND ----------

conf = SparkConf().setAppName("Min and Max")
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0],int(x.split(',')[1])))
rdd2.collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x , y : x if x < y else y).collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x , y : x if x > y else y).collect()

# COMMAND ----------

conf = SparkConf().setAppName("Quiz")
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

rdd = rdd.map(lambda x: x.split(','))
rdd = rdd.map(lambda x: (x[1], float(x[2])))

# COMMAND ----------

rdd.reduceByKey(lambda x,y: x if x > y else y).collect()

# COMMAND ----------

rdd.reduceByKey(lambda x,y: x if x < y else y).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Project - Student Data Analysis

# COMMAND ----------

conf = SparkConf().setAppName("Mini Project")
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('/FileStore/tables/StudentData.csv')
rdd.collect()

# COMMAND ----------

#Total Students
rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x!=headers)
rdd = rdd.map(lambda x: x.split(','))
rdd.count()

# COMMAND ----------

#Total Marks by Male and Female Student
rdd2 = rdd
rdd2 = rdd2.map(lambda x: (x[1], int(x[5])))
rdd2 = rdd2.reduceByKey(lambda x,y : x+y)
rdd2.collect()

# COMMAND ----------

#Total Passed and Failed Students
rdd3 = rdd
passed = rdd3.filter(lambda x: int(x[5]) > 50).count()
failed = rdd3.filter(lambda x: int(x[5]) <= 50).count()
print(passed,failed)

passed2 = rdd3.filter(lambda x: int(x[5]) > 50).count()
failed2 = rdd.count() - passed2
print(passed2,failed2)

# COMMAND ----------

#Total Enrollments per Course
rdd4 = rdd
rdd4 = rdd4.map(lambda x: (x[3],1))
rdd4.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

#Total Marks per Course
rdd5 = rdd
rdd5 = rdd5.map(lambda x: (x[3], int(x[5])))
rdd5.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

#Average marks per Course
rdd6 = rdd
rdd6 = rdd6.map(lambda x: (x[3], (int(x[5]), 1) ))
rdd6 = rdd6.reduceByKey( lambda x,y : (x[0] + y[0], x[1] + y[1]))

rdd6.map(lambda x: (x[0], (x[1][0] / x[1][1]))).collect()
rdd6.mapValues(lambda x: (x[0] / x[1])).collect()

# COMMAND ----------

#Finding Minimum and Maximum marks
rdd7 = rdd
rdd7 = rdd7.map(lambda x: (x[3], int(x[5])))
print(rdd7.reduceByKey(lambda x,y: x if x > y else y).collect())
print(rdd7.reduceByKey(lambda x,y: x if x < y else y).collect())

# COMMAND ----------

#Average Age of Male and Female Students
rdd8 = rdd
rdd8 = rdd8.map( lambda x: ( x[1] , ( int(x[0]), 1 ) ) )
rdd8 = rdd8.reduceByKey(lambda x,y: ( x[0] + y[0], x[1] + y[1] )  )
rdd8.mapValues(lambda x: x[0]/x[1]).collect()