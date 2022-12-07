# Databricks notebook source
from pyspark.sql.functions import explode, explode_outer, current_timestamp, lit, abs
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# COMMAND ----------

#metadata
def meta_raw():
    return raw.select(
        "Movies",
        current_timestamp().cast("date").alias("Ingestdate"),
        current_timestamp().alias("Ingesttime"),
        lit("New").alias("Status"),
        "source", lit("sep_movie"）
    )

# COMMAND ----------

#json explpode
def raw_bronze():
    jsonschema = StructType([
        StructField('movie', 
            ArrayType(
                StructType([
                    StructField('Id', IntegerType(), True),
                    StructField('Title', StringType(), True),
                    StructField('Overview', StringType(), True), 
                    StructField('Tagline', StringType(), True),
                    StructField('Budget', DoubleType(), True),
                    StructField('Revenue', DoubleType(), True), 
                    StructField('ImdbUrl', StringType(), True), 
                    StructField('TmdbUrl', StringType(), True),
                    StructField('PosterUrl', StringType(), True),
                    StructField('BackdropUrl', StringType(), True), 
                    StructField('OriginalLanguage', StringType(), True), 
                    StructField('ReleaseDate', TimestampType(), True),
                    StructField('RunTime', IntegerType(), True),
                    StructField('Price', DoubleType(), True),
                    StructField('CreatedDate', TimestampType(), True),
                    StructField('UpdatedDate', TimestampType(), True),
                    StructField('UpdatedBy', StringType(), True),
                    StructField('CreatedBy', StringType(), True), 
                    StructField('genres', 
                        ArrayType(
                            StructType([
                                StructField('id', IntegerType(), True),
                                StructField('name', StringType(), True)
                            ]),True
                        ), True
                    )
                ]), True
            ), True
        )
    ])
    

# COMMAND ----------

#runrime update
def update_runtime(dataframe: DataFrame):
    dataframe.withColumn("RunTime", abs(col("RunTime")))

# COMMAND ----------

#update the budget<100000
def update_budget():
    df = spark.read.load(silverPath)
    budget_1=df.filter("Budget>=1000000")
    budget_quarantined=df.filter("Budget<1000000")
    budget_updated = udget_quarantined.withColumn("Budget", lit(1000000))
    return(budget_1,budget_updated)

# COMMAND ----------

#genre look up table
def sub_genre():
    genre_sub=(dataframe.select(explode(column)).alias(alias)).distinct()
    genre_sub=genre_sub.select('col', 'col.*')
    genre_sub = (genre_sub.filter(col("name") != ''))
    return genre_sub

# COMMAND ----------


