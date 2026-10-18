# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Delta Lake incremental MERGE pattern
# MAGIC This example assumes `source_df` is already incrementally ingested and validated.
# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F

silver_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/silver/employee_compensation/"

# Replace this with your current validated incremental batch.
source_df = spark.read.format("delta").load(
    "abfss://<container>@<storage-account>.dfs.core.windows.net/staging/validated_increment/"
)

if DeltaTable.isDeltaTable(spark, silver_path):
    target = DeltaTable.forPath(spark, silver_path)
    (target.alias("t")
        .merge(source_df.alias("s"),
               "t.employee_id = s.employee_id AND t.effective_date = s.effective_date")
        .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())
else:
    source_df.write.format("delta").mode("overwrite").save(silver_path)

# For production, add schema-evolution policy, audit counts, retry handling,
# and a reliable watermark strategy to the ingestion pipeline.
