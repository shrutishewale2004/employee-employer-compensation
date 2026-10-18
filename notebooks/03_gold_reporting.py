# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold reporting aggregate
# COMMAND ----------
from pyspark.sql import functions as F

silver_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/silver/employee_compensation/"
gold_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/gold/compensation_by_employer_department/"

silver = spark.read.format("delta").load(silver_path)
enriched = silver.withColumn(
    "total_compensation_cost",
    F.col("annual_salary") + F.col("employer_contribution")
)

gold = (enriched.groupBy("employer_id", "employer_name", "department", "currency")
    .agg(F.countDistinct("employee_id").alias("employee_count"),
         F.sum("annual_salary").alias("total_annual_salary"),
         F.sum("employer_contribution").alias("total_employer_contribution"),
         F.sum("total_compensation_cost").alias("total_compensation_cost"))
    .withColumn("processed_at", F.current_timestamp()))

(gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path))
display(gold.orderBy(F.desc("total_compensation_cost")))
