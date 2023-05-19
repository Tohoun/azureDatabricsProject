# Databricks notebook source
# MAGIC %md
# MAGIC #Movie recommendation service

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load data and explore from storage attached to dabricks

# COMMAND ----------

#load data
df = spark.read.format('delta').load("dbfs:/databricks-datasets/tpch/delta-001/customer")

# COMMAND ----------

# print dataframe schema
df.printSchema()

# COMMAND ----------

# print the head of data
df.show(10)

# COMMAND ----------

# print the dataframe using display method
display(df)

# COMMAND ----------

# list of segment and the size of each
dataSegment = (df
               .select('c_mktsegment', 'c_acctbal')
               .distinct()
               )

display(dataSegment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load data and explore from azure data lake storage gen2

# COMMAND ----------

dbutils.fs.ls("abfss://row@storageadlgen2555684820.dfs.core.windows.net/MovieLens")

# COMMAND ----------

#Load data from azure blob storage gen2
df = spark.read.csv('abfss://row@storageadlgen2555684820.dfs.core.windows.net/MovieLens/movies.csv', inferSchema=True, header=True)

# COMMAND ----------

# Display the dataframe
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the col genres

# COMMAND ----------

from pyspark.sql.functions import *

df1 = (df
       .withColumn("genres", split(col("genres"), "[|]"))
       )

display(df1)

# COMMAND ----------

df2 = (df1
       .withColumn("genres", explode(col("genres")))
)

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### replace the () in title by ;

# COMMAND ----------

df3 = (df2
       .withColumn("title", regexp_replace("title", "[(]", ";"))
       .withColumn("title", regexp_replace("title", "[)]", ""))
       )

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the title column in aray contains the name and year

# COMMAND ----------

df4 = (df3
       .withColumn("title", split(col("title"), ";"))
       )

display(df4)

# COMMAND ----------

df5 = (df4
       .withColumn("year", col("title").getItem(1).cast("int"))
       .withColumn("title", col("title").getItem(0))
       )

display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Some desciptive statistics on dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1. Number of movies per year from 2000

# COMMAND ----------

df6 = (df5
       .filter("year>=2000")
       .groupBy("year")
       .count()
       .sort("year")
       )

display(df6)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2. Number of movies per genres

# COMMAND ----------

df7 = (df5
       .filter(col("genres").isNotNull())
       .groupBy("genres")
       .count()
       .sort(col("count").desc())
       )

display(df7)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3. Number of movies per year and genres the five years ago

# COMMAND ----------

df8 = (df5
       .filter(col("genres").isNotNull())
       .filter("year>=2014")
       .groupBy("genres","year")
       .count()
       .orderBy("genres","year")
       )

display(df8)
