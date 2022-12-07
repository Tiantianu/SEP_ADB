# Databricks notebook source
username = 'tian'

# COMMAND ----------

moviePipelinePath=f"/dbfs/FileStore/tian/sep_movie/"
rawPath = moviePipelinePath + "raw/"
bronzePath = moviePipelinePath + "bronze/"
silverPath = moviePipelinePath + "silver/"
