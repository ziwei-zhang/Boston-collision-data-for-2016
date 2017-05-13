# Databricks notebook source
df = table("visionzero_boston_entry")

# COMMAND ----------

import pyspark.sql.functions


# COMMAND ----------

 from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------to add new columns in the dataframe

df = df.withColumn("USERID", lit('')).withColumn("REPORTTYPEID", lit('')).withColumn("ADDEDBY", lit('')).withColumn("HOSTID", lit('')).withColumn("RADIUSIMPACT", lit('')).withColumn("PRICE", lit('')).withColumn('REPORTSTATUSID', lit('')).withColumn("CREATEDTIME", lit('')).withColumn("LASTMODIFIEDTIME", lit('')).withColumn("ADDRESS", lit('')).withColumn("TOTALNUMBERINJURED", lit('')).withColumn("TOTALNUMBERKILLED", lit(''))

# COMMAND ----------to check the result of the previos commmand

display(df)

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

df = df.withColumn('DATE_TIME', date_format('REQUESTDATE', 'yyyy-MM-dd, H:mm:ss'))

# COMMAND ----------to split the date/time columns into two columns containing date and time respectively

split_col = pyspark.sql.functions.split(df['DATE_TIME'], ',')
df = df.withColumn('DATE', split_col.getItem(0))
df = df.withColumn('TIME', split_col.getItem(1))

# COMMAND ----------

display(df)

# COMMAND ----------

sqlContext.registerDataFrameAsTable(df, "T")

# COMMAND ----------to create a new dataframe containing desired columns

result = sqlContext.sql("SELECT OBJECTID as REPORTID, USERID, REQUESTID, REQUESTTYPE, REPORTTYPEID, ADDEDBY, HOSTID, x as LATITUDE, y as LONGITUDE, RADIUSIMPACT, COMMENTS as DESCRIPTION, PRICE, STATUS, REPORTSTATUSID, DATE, TIME, CREATEDTIME, LASTMODIFIEDTIME, ADDRESS, TOTALNUMBERINJURED, TOTALNUMBERKILLED, USERTYPE as VEHICLETYPECODE FROM T WHERE REQUESTDATE LIKE '2016%'")

# COMMAND ----------

display(result)

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------to insert value in the column REPORTSTATUSID depending on the values in the column STATUS

result = result.withColumn('REPORTSTATUSID', when(result.STATUS == 'Unassigned', lit(0)).otherwise(lit(1)))

# COMMAND ----------

display(result)

# COMMAND ----------to cast datatypes 

 df1 = result.select(result.REPORTID.cast("integer"), 
                     result.USERID.cast("string"),
                     result.REQUESTTYPE.cast("string"),
                     result.REPORTTYPEID.cast("integer"), 
                     result.ADDEDBY.cast("string"),
                     result.HOSTID.cast("integer"), 
                     result.LATITUDE.cast("float"), 
                     result.LONGITUDE.cast("float"),
                     result.RADIUSIMPACT.cast("float"), 
                     result.DESCRIPTION.cast("string"), 
                     result.PRICE.cast("float"),
                     result.REPORTSTATUSID.cast("integer"),                     
                     result.DATE.cast("date"), 
                     result.TIME.cast("string"), 
                     result.CREATEDTIME.cast("timestamp"),
                     result.LASTMODIFIEDTIME.cast("timestamp"), 
                     result.ADDRESS.cast("string"), 
                     result.TOTALNUMBERINJURED.cast("integer"),
                     result.TOTALNUMBERKILLED.cast("integer"), 
                     result.VEHICLETYPECODE.cast("string")                                     
                    )

# COMMAND ---------- to show the schema of the dataframe

df1.printSchema()

# COMMAND ----------to show the finl results

display(df1)

# COMMAND ----------


