# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion
# MAGIC Configure `source_path` and `bronze_path` for your ADLS Gen2 container.
# COMMAND ----------
from pyspark.sql import functions as F

source_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/incoming/employee_compensation/"
bronze_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/bronze/employee_compensation/"

raw_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name()))

(raw_df.write.format("delta")
    .mode("append")
    .save(bronze_path))

display(raw_df.limit(20))
