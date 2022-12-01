# Databricks notebook source
#raw_bronze
raw_path = 'dbfs:/FileStore/tables'
bronze_path = 'dbfs:/tables'
silver_path = 'dbfs:/tables'

# COMMAND ----------

from pyspark.sql.functions import explode, col, to_json,lit
raw = spark.read.json(path = raw_path, multiLine = True)

# COMMAND ----------

raw = raw.select("movie", explode("movie"))


# COMMAND ----------

raw = raw.drop(col("movie"))

# COMMAND ----------

display(raw)

# COMMAND ----------

  from pyspark.sql.functions import current_timestamp, lit  

# COMMAND ----------

df = raw.select('movie',
        current_timestamp().cast("date").alias("ingestdate"),
        current_timestamp().alias("ingesttime"),
        lit("antra_sep_movie").alias("resource"),
        lit("new").alias("status")
)

# COMMAND ----------

df.select("resource", 
          "ingesttime", 
          "movie", 
          "status", 
          col("ingestdate").alias("p_ingestdate")).
write.format("delta").
mode("append").
partitionBy("p_ingestdate").
save(bronze_path)

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

df.write.mode("overwrite").
    option("path", "bronze_path").
    saveAsTable("bronze")

# COMMAND ----------

#bronze_silver
b_s = spark.read.load(bronze_path).
    filter("status = 'new'")

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

movies_bronze = spark.read.load(path = bronze_path)
display(movies_bronze)

# COMMAND ----------

#quarantine runtime<0 data
q_runtime = movies_bronze.filter("RunTime < 0")
q_budget = movies_bronze.filter("Budget < 1000000")


# COMMAND ----------

b_s = movies_bronze.filter("RunTime >= 0") & ("Budget >= 1000000")
b_s.write.format("delta").
    mode("append").
    option(silver_path).
    saveAsTable("silver")

# COMMAND ----------

cleaned_runtime = q_runtime.withColumn("RunTime", abs(quarantine_df.RunTime))

# COMMAND ----------

q_budget.loc[:,"Budget"] = 1000000


# COMMAND ----------


