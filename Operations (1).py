# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from delta.tables import DeltaTable

# COMMAND ----------

#json_schema
movieSchema = StructType([
                   StructField("movie", ArrayType(
                                         StructType([
                                                 StructField("BackdropUrl", StringType()),
                                                 StructField("Budget", DoubleType()),
                                                 StructField("CreatedBy",StringType()),
                                                 StructField("CreatedDate", TimestampType()),
                                                 StructField("Id", LongType()),
                                                 StructField("ImdbUrl", StringType()),
                                                 StructField("OriginalLanguage", StringType()),
                                                 StructField("Overview", StringType()),
                                                 StructField("PosterUrl", StringType()),
                                                 StructField("Price", DoubleType()),
                                                 StructField("ReleaseDate", TimestampType()),
                                                 StructField("Revenue", DoubleType()),
                                                 StructField("RunTime", LongType()),
                                                 StructField("Tagline", StringType()),
                                                 StructField("Title", StringType()),
                                                 StructField("TmdbUrl", StringType()),
                                                 StructField("UpdatedBy", StringType()),
                                                 StructField("UpdatedDate", TimestampType()),
                                                 StructField("genres", ArrayType(
                                                                        StructType([
                                                                                  StructField("id", LongType()),
                                                                                  StructField("name", StringType())
                                                                                  ])))
                     ]))
                     )])
    
def read_raw(path: str, schema: StructType) -> DataFrame:
    return (spark.read
                .option("multiLine", "true")
                .schema(movieSchema)
                .json(path))

# COMMAND ----------

#explode_json
def explode_function(Dataframe: DataFrame, explode_column: str, alias: str) -> DataFrame:
    return Dataframe.select(explode(col(explode_column)).alias(alias))

# COMMAND ----------

#add_surrogateKEY
def add_surrogateKEY(Dataframe: DataFrame, column:str,alias: str)-> DataFrame:
    return Dataframe.select(row_number().over(Window.orderBy(col(column))).alias(alias),
                                      col(column)
                             )

# COMMAND ----------

#metadata
def ingest_meta(Dataframe: DataFrame)-> DataFrame:
    return Dataframe.select(
            "surrogateKEY",
            "movie",
            current_timestamp().alias("ingesttime"),
            lit("new").alias("status"),
            current_timestamp().cast("date").alias("p_ingestdate")
             )

# COMMAND ----------

#write_data
def batch_writer_append(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append"
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns) 
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )

# COMMAND ----------

def batch_writer_overwrite(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "overwrite"
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )

# COMMAND ----------

def write_delta(
    dataframe: DataFrame,
    mode: str,
    exclude_columns: List = []
) -> DataFrameWriter:
    return (
        dataframe.drop(
            *exclude_columns)
        .write.format("delta")
        .mode(mode)
    )

# COMMAND ----------

#register_table
def register_table(table: str, path: str) -> None:
  spark.sql(f"""
  DROP TABLE IF EXISTS {table}
  """)

  spark.sql(f"""
  CREATE TABLE {table}
  USING DELTA
  LOCATION "{path}"
  """)
  
  return True

# COMMAND ----------

# extract_nested_json
def extract_nested():
    bronzeDF = spark.read.load(bronzePath)
    extractedDF = bronzeDF.select("surrogateKEY","movie", "p_ingestdate", "movie.*")
    return extractedDF

# COMMAND ----------

def extract_fetch(bronze: DataFrame, column: str) -> DataFrame:
  return bronze.select(col(column), 
                      col(column + ".*"))

# COMMAND ----------

#genre_lookup
def genre_lookup(dataframe: DataFrame, column: str, alias:str ) -> DataFrame:
    genreDF=(dataframe.select(explode(column)).alias(alias)).distinct()
    genreDF=genreDF.select('col', 'col.*')
    genreDF = (genreDF.filter(col("name") != ''))
    return genreDF

# COMMAND ----------

#movie_Genre_Junction_table
def movieGenreJunction(dataframe: DataFrame, explode_column:str, alias:str, column: List = []):
    movieGenreDF=dataframe.select(*column, explode(col(explode_column)).alias(alias))
    movieGenreDF = movieGenreDF.select(row_number().over(Window.orderBy(col("id"))).alias("moviegenre_id"),
                                      col("id").alias("movie_id"),
                                      col("genres.id").alias("genre_id"))
    return movieGenreDF

# COMMAND ----------

#language_lookup
def language_lookup(dataframe: DataFrame, explode_column:str, alias:str, column: List = []):
    languageDF = (dataframe.select(col(explode_column).alias(alias)).distinct())
    languageDF=languageDF.select(
                       row_number().over(Window.orderBy(col(explode_column))).alias("OriginalLanguage_id"),
                       col(explode_column).alias("name")
                      )
    return languageDF

# COMMAND ----------

#runtime_filter
def runtime_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("RunTime >= 0"),
        dataframe.filter("RunTime < 0"),
    )

# COMMAND ----------

#Update_Clean_records
def Update_Clean_records(dataframe:DataFrame):
    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    silverAugmented = dataframe.withColumn("status", lit("loaded"))

    update_match = "bronze.surrogateKEY = clean.surrogateKEY"
    update = {"status": "clean.status"}

    (
        bronzeTable.alias("bronze")
        .merge(silverAugmented.alias("clean"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )


# COMMAND ----------

#Update_Quarantined_records(runtime,budget...)
def Update_Quarantined_records(dataframe:DataFrame):
    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    silverAugmented = dataframe.withColumn(
    "status", lit("quarantined")
    )

    update_match = "bronze.surrogateKEY = quarantine.surrogateKEY"
    update = {"status": "quarantine.status"}

    (
        bronzeTable.alias("bronze")
        .merge(silverAugmented.alias("quarantine"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

# COMMAND ----------

#update_runtime_with_abs
def update_runtime(dataframe: DataFrame):
    dataframe.withColumn("RunTime", abs(col("RunTime")))

# COMMAND ----------

#update_budget_1000000
def update_budget():
    df = spark.read.load(silverPath)
    df_budget_clean=df.filter("Budget>=1000000")
    df_budget_quarantined=df.filter("Budget<1000000")
    df_budget_quarantined_repaired = df_budget_quarantined.withColumn("Budget", lit(1000000).cast("Double"))
    return(df_budget_clean,df_budget_quarantined_repaired)

# COMMAND ----------

#duplicated_filter_with_count
def duplicated_non_duplicated_seperated():

    bronzeDF_duplicates = extractedDF.groupBy("movie").count().filter("count > 1")
    bronzeDF_non_duplicates = extractedDF.groupBy("movie").count().filter("count = 1")

# COMMAND ----------

#mark_duplicates
def mark_duplicates():
    
    bronzeDF_duplicates = extractedDF.groupBy("movie").count().filter("count > 1")
    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    silverAugmented = bronzeDF_duplicates.withColumn(
    "status", lit("duplicated")
    )

    update_match = "bronze.movie = quarantine.movie"
    update = {"status": "quarantine.status"}

    (
        bronzeTable.alias("bronze")
        .merge(silverAugmented.alias("quarantine"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

# COMMAND ----------

#mark_non_duplicates
def mark_non_duplicates():
    bronzeDF = spark.read.load(bronzePath)
    bronzeDF_non_duplicates = extractedDF.groupBy("movie").count().filter("count = 1")

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    silverAugmented = bronzeDF_non_duplicates.withColumn(
    "status", lit("non_duplicated")
    )

    update_match = "bronze.movie = quarantine.movie"
    update = {"status": "quarantine.status"}

    (
        bronzeTable.alias("bronze")
        .merge(silverAugmented.alias("quarantine"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )
