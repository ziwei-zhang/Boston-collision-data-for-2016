# Databricks notebook source
df = table("visionzero_boston_entry")

# COMMAND ----------

display(df)

# COMMAND ----------

import pyspark.sql.functions


# COMMAND ----------

sqlContext.registerDataFrameAsTable(df, "T")

# COMMAND ----------

result1 = sqlContext.sql("SELECT OBJECTID as INCIDENT, REQUESTDATE as DATE, USERTYPE as MODE, STATUS, CONCAT(x, ',',  y) AS location, COMMENTS FROM T WHERE REQUESTDATE LIKE '2016%'")

# COMMAND ---------- to create a dataframe containing desired columns

display(result1)

# COMMAND ----------


