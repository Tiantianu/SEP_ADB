# Databricks notebook source
#set_up

# COMMAND ----------

username = 'tian'

# COMMAND ----------

uploadPath = f"/FileStore/{username}/ADB"

moviePath = f"/mnt/{username}/movie"
rawPath = moviePath + '/0_raw'
bronzePath = moviePath + '/1_bronze'
silverPath = moviePath + '/2_silver'

genrePath = moviePath + '/silver_genre'
movieGenrePath = moviePath + '/silver_movieGenre'
languagePath = moviePath + '/silver_language'

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS movie_{username}")
spark.sql(f"USE movie_{username}")

# COMMAND ----------

#raw

# COMMAND ----------

display(dbutils.fs.ls(uploadPath))

# COMMAND ----------

file_list = [[file.path, file.name] for file in dbutils.fs.ls(uploadPath)]

# COMMAND ----------

display(file_list)

# COMMAND ----------

for file in file_list:
    dbutils.fs.cp(file[0], rawPath + "/" +file[1])

# COMMAND ----------

dbutils.fs.ls(rawPath)

# COMMAND ----------

rawDF = (spark.read
                .option("multiLine", "true")
                .json(rawPath))

# COMMAND ----------

display(rawDF)

# COMMAND ----------

#Raw_to_Bronze

# COMMAND ----------

rawDF.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

moiveSchema = StructType([
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

# COMMAND ----------

rawDF=(spark.read
                .option("multiLine", "true")
                .schema(moiveSchema)
                .json(rawPath)
)

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

rawDF=rawDF.withColumn("movie", explode("movie"))

# COMMAND ----------

display(rawDF)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

DF = rawDF.select(row_number().over(Window.orderBy(col("movie"))).alias("surrogateKEY"), col("movie") )

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

meta_DF = DF.select(
    "surrogateKEY",
    "movie",
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"))

# COMMAND ----------

display(meta_DF)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

(
    meta_DF.select(
        "surrogateKEY",
        "movie",
        "ingesttime",
        "status",
        col("ingestdate").alias("p_ingestdate"),
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)                                                                     

spark.sql(
f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

#Bronze_to_Silver

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")

# COMMAND ----------

bronzeDF = spark.read.load(bronzePath)

# COMMAND ----------

bronzeDF.printSchema()

# COMMAND ----------

extractedDF = bronzeDF.select("surrogateKEY","movie", "p_ingestdate", "movie.*")

# COMMAND ----------

display(extractedDF)

# COMMAND ----------

silver_runtime_clean = extractedDF.filter("RunTime >= 0")
silver_runtime_quarantine = extractedDF.filter("RunTime < 0")

# COMMAND ----------

print(silver_runtime_clean.count())


# COMMAND ----------

# 15
print(silver_runtime_quarantine.count())

# COMMAND ----------

(   silver_runtime_clean.drop('movie'
     )
    .write
    .format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silver_runtime_clean_table
"""
)

spark.sql(
    f"""
CREATE TABLE silver_runtime_clean_table
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_runtime_clean.withColumn("status", lit("loaded"))

update_match = "bronze.surrogateKEY = clean.surrogateKEY" 
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

silverAugmented = silver_runtime_quarantine.withColumn(
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

#15
display(silver_runtime_quarantine)

# COMMAND ----------

# abs runtime
silver_runtime_quarantine_cleaned = silver_runtime_quarantine.withColumn("RunTime", abs(col("RunTime")))

# COMMAND ----------

display(silver_runtime_quarantine_cleaned)

# COMMAND ----------

( silver_runtime_quarantine.drop('movie'
     )
    .write
    .format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silver_runtime_clean_table
"""
)

spark.sql(
f"""
CREATE TABLE silver_runtime_clean_table
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

#geners explode

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

genreDF=(extractedDF.select(explode("genres")).alias("genres")).distinct()

# COMMAND ----------

display(genreDF)
# name = ' '

# COMMAND ----------

genreDF=genreDF.select(
    'col',
    'col.*'
)

# COMMAND ----------

display(genreDF)

# COMMAND ----------

genreDF = (genreDF.filter(col("name") != ''))

# COMMAND ----------

#cleaned_genre df
display(genreDF)

# COMMAND ----------

(
    genreDF
    .write
    .format("delta")
    .mode("overwrite")
    .save(genrePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS genre
"""
)

spark.sql(
f"""
CREATE TABLE genre
USING DELTA
LOCATION "{genrePath}"
"""
)

# COMMAND ----------

#Genre_junction
movieGenreDF=extractedDF.select(*["Id"], explode(col("genres")).alias("genres"))

# COMMAND ----------

movieGenreDF.display()

# COMMAND ----------

movieGenreDF = movieGenreDF.select(row_number().over(Window.orderBy(col("id"))).alias("moviegenre_id"),
                                      col("id").alias("movie_id"),
                                      col("genres.id").alias("genre_id"))

# COMMAND ----------

movieGenreDF.display()

# COMMAND ----------

(
    movieGenreDF
    .write
    .format("delta")
    .mode("overwrite")
    .save(movieGenrePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movieGenre
"""
)

spark.sql(
f"""
CREATE TABLE movieGenre
USING DELTA
LOCATION "{movieGenrePath}"
"""
)

# COMMAND ----------

#language_lookup_table

# COMMAND ----------

languageDF = (extractedDF.select(col("OriginalLanguage").alias("language")).distinct())

# COMMAND ----------

 languageDF=languageDF.select(
                       row_number().over(Window.orderBy(col("language"))).alias("language_id"),
                       col("language").alias("name")
                      )

# COMMAND ----------

display(languageDF)

# COMMAND ----------

(
    languageDF
    .write
    .format("delta")
    .mode("overwrite")
    .save(languagePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS languageDF
"""
)

spark.sql(
    f"""
CREATE TABLE languageDF
USING DELTA
LOCATION "{languagePath}"
"""
)

# COMMAND ----------

#update_budget_in_silver

# COMMAND ----------

df = spark.read.load(silverPath)

# COMMAND ----------

#cleaned_budget
df_budget_clean=df.filter("Budget>=1000000")

# COMMAND ----------

#unclean_budget
df_budget_quarantined=df.filter("Budget<1000000")

# COMMAND ----------

df_budget_quarantined.display()

# COMMAND ----------

#4480
df_budget_quarantined.count()

# COMMAND ----------

#update_budget
df_budget_quarantined_updated = df_budget_quarantined.withColumn("Budget", lit(1000000).cast("Double"))

# COMMAND ----------

df_budget_quarantined_updated.display()

# COMMAND ----------

df_budget_quarantined_updated.count()

# COMMAND ----------

df_budget_clean.printSchema()

# COMMAND ----------

#save_cleaned
(   df_budget_clean
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

#save_quarantined_updated
(   df_budget_quarantined_updated
    .write
    .format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

#duplicate

# COMMAND ----------

extractedDF.display()

# COMMAND ----------

#duplicates
DF_duplicates = extractedDF.groupBy("movie").count().filter("count > 1")

# COMMAND ----------

#non_duplicates
DF_non_duplicates = extractedDF.groupBy("movie").count().filter("count = 1")

# COMMAND ----------

DF_duplicates.display()

# COMMAND ----------

DF_duplicates.count()

# COMMAND ----------

#update_duplicated_status
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = DF_duplicates.withColumn(
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

#update_non_duplicated_status
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = DF_non_duplicates.withColumn(
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

