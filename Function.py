# Databricks notebook source
# MAGIC %run /Users/youtiantian1215@gmail.com/Operations

# COMMAND ----------

# MAGIC %run /Users/youtiantian1215@gmail.com/Configuration_main

# COMMAND ----------

dbutils.fs.rm(moviePath, True)

# COMMAND ----------

rawDF=read_raw(rawPath, movieSchema)

# COMMAND ----------

rawDF=explode_function(rawDF,'movie','movie')

# COMMAND ----------

DF=add_surrogateKEY(rawDF,"movie","surrogateKEY")

# COMMAND ----------

meta_DF=ingest_meta(DF)

# COMMAND ----------

batch_writer_append(meta_DF,"p_ingestdate").save(bronzePath)

# COMMAND ----------

register_table("movie_bronze",bronzePath)

# COMMAND ----------

extractedDF=extract_nested()

# COMMAND ----------

silver_runtime_clean=runtime_clean_and_quarantine_dataframes(extractedDF)[0]
silver_runtime_quarantine=runtime_clean_and_quarantine_dataframes(extractedDF)[1]

# COMMAND ----------

batch_writer_append(silver_runtime_clean,"p_ingestdate",['movie']).save(silverPath)

# COMMAND ----------

register_table('silver_runtime_clean_table',silverPath)

# COMMAND ----------

Update_Clean_records(silver_runtime_clean)

# COMMAND ----------

Update_Quarantined_records(silver_runtime_quarantine)

# COMMAND ----------

silver_runtime_quarantine_cleaned = update_runtime(silver_runtime_quarantine)

# COMMAND ----------

batch_writer_append(silver_runtime_clean,"p_ingestdate",['movie']).save(silverPath)

# COMMAND ----------

register_table('silver_runtime_clean_table',silverPath)

# COMMAND ----------

df_budget_clean=update_budget()[0]
df_budget_quarantined_repaired= update_budget()[1]

# COMMAND ----------

batch_writer_overwrite(df_budget_clean,"p_ingestdate").save(silverPath)
batch_writer_append(df_budget_quarantined_repaired,"p_ingestdate").save(silverPath)

# COMMAND ----------

register_table('silver_runtime_clean_table',silverPath)

# COMMAND ----------

duplicated_non_duplicated_seperated()

# COMMAND ----------

mark_duplicates()

# COMMAND ----------

mark_non_duplicates()

# COMMAND ----------

bronzeDF = spark.read.load(bronzePath).filter("status = 'non-duplicated'")

# COMMAND ----------

genreDF=genre_lookup(extractedDF,'genres','genres')

# COMMAND ----------

genreDF=genre_lookup(extractedDF,'genres','genres')

# COMMAND ----------

write_delta(genreDF,"overwrite").save(genrePath)

# COMMAND ----------

register_table("genre",genrePath)

# COMMAND ----------

movieGenreDF=movieGenreJunction(extractedDF,"genres","genres",["id"])

# COMMAND ----------

write_delta(movieGenreDF,"overwrite").save(movieGenrePath)
register_table("movieGenre",movieGenrePath)

# COMMAND ----------

languageDF=language_lookup(extractedDF,'OriginalLanguage','OriginalLanguage')

# COMMAND ----------

write_delta(languageDF,"overwrite").save(languagePath)
register_table("languageLookup",languagePath)
