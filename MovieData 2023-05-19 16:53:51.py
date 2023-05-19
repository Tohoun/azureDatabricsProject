# Databricks notebook source
# MAGIC %md
# MAGIC #Movie recommendation service

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load data and explore

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
