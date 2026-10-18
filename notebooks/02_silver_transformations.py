# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver cleansing and validation
# COMMAND ----------
from pyspark.sql import functions as F, Window

bronze_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/bronze/employee_compensation/"
silver_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/silver/employee_compensation/"
reject_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/rejected/employee_compensation/"

df = spark.read.format("delta").load(bronze_path)
df = (df.withColumn("employee_id", F.trim("employee_id"))
        .withColumn("employee_name", F.trim("employee_name"))
        .withColumn("employer_id", F.trim("employer_id"))
        .withColumn("employer_name", F.trim("employer_name"))
        .withColumn("department", F.trim("department"))
        .withColumn("annual_salary", F.col("annual_salary").cast("decimal(18,2)"))
        .withColumn("employer_contribution", F.col("employer_contribution").cast("decimal(18,2)"))
        .withColumn("effective_date", F.to_date("effective_date", "yyyy-MM-dd"))
        .withColumn("updated_at", F.to_timestamp("updated_at")))

bad = (F.col("employee_id").isNull() | (F.col("employee_id") == "") |
       F.col("employee_name").isNull() | (F.col("employee_name") == "") |
       F.col("employer_id").isNull() | (F.col("employer_id") == "") |
       F.col("annual_salary").isNull() | (F.col("annual_salary") < 0) |
       F.col("employer_contribution").isNull() | (F.col("employer_contribution") < 0) |
       F.col("effective_date").isNull())

rejected = df.filter(bad).withColumn("rejection_reason", F.lit("Failed required-field, date, or compensation validation"))
valid = df.filter(~bad)

window = Window.partitionBy("employee_id", "effective_date").orderBy(F.col("updated_at").desc_nulls_last())
silver = (valid.withColumn("_rn", F.row_number().over(window))
               .filter(F.col("_rn") == 1)
               .drop("_rn"))

(rejected.write.format("delta").mode("append").save(reject_path))
(silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path))
display(silver)
