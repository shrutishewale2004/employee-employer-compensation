"""Local PySpark demonstration for the Employee & Employer Compensation project."""
from pathlib import Path
import logging
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

ROOT = Path(__file__).resolve().parents[1]
INPUT = str(ROOT / "data" / "sample" / "employee_compensation.csv")
OUTPUT = ROOT / "output"

SCHEMA = StructType([
    StructField("employee_id", StringType(), True),
    StructField("employee_name", StringType(), True),
    StructField("employer_id", StringType(), True),
    StructField("employer_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("annual_salary", DoubleType(), True),
    StructField("employer_contribution", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("effective_date", StringType(), True),
    StructField("updated_at", StringType(), True),
])

def build_spark():
    return (SparkSession.builder
            .appName("EmployeeEmployerCompensation")
            .master("local[*]")
            .getOrCreate())

def transform_data(df):
    """Return (valid_deduplicated_df, rejected_df) after applying business rules."""
    cleaned = (
        df.withColumn("employee_id", F.trim("employee_id"))
          .withColumn("employee_name", F.trim("employee_name"))
          .withColumn("employer_id", F.trim("employer_id"))
          .withColumn("employer_name", F.trim("employer_name"))
          .withColumn("department", F.trim("department"))
          .withColumn("currency", F.upper(F.trim("currency")))
          .withColumn("annual_salary", F.col("annual_salary").cast("double"))
          .withColumn("employer_contribution", F.col("employer_contribution").cast("double"))
          .withColumn("effective_date_parsed", F.to_date("effective_date", "yyyy-MM-dd"))
          .withColumn("updated_at_parsed", F.to_timestamp("updated_at"))
    )
    invalid = (
        F.col("employee_id").isNull() | (F.col("employee_id") == "") |
        F.col("employee_name").isNull() | (F.col("employee_name") == "") |
        F.col("employer_id").isNull() | (F.col("employer_id") == "") |
        F.col("employer_name").isNull() | (F.col("employer_name") == "") |
        F.col("annual_salary").isNull() | (F.col("annual_salary") < 0) |
        F.col("employer_contribution").isNull() | (F.col("employer_contribution") < 0) |
        F.col("effective_date_parsed").isNull()
    )
    rejected = cleaned.filter(invalid).withColumn(
        "rejection_reason",
        F.when(F.col("employee_id").isNull() | (F.col("employee_id") == ""), "Missing employee_id")
         .when(F.col("employee_name").isNull() | (F.col("employee_name") == ""), "Missing employee_name")
         .when(F.col("employer_id").isNull() | (F.col("employer_id") == ""), "Missing employer_id")
         .when(F.col("annual_salary").isNull(), "Invalid annual_salary")
         .when(F.col("annual_salary") < 0, "Negative annual_salary")
         .when(F.col("employer_contribution").isNull() | (F.col("employer_contribution") < 0), "Invalid employer_contribution")
         .when(F.col("effective_date_parsed").isNull(), "Invalid effective_date")
         .otherwise("Other validation failure")
    )
    valid = cleaned.filter(~invalid)
    window = Window.partitionBy("employee_id", "effective_date_parsed").orderBy(
        F.col("updated_at_parsed").desc_nulls_last()
    )
    valid = (valid.withColumn("row_num", F.row_number().over(window))
                  .filter(F.col("row_num") == 1)
                  .drop("row_num"))
    return valid, rejected

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    spark = build_spark()
    try:
        raw = (spark.read.option("header", True).schema(SCHEMA).csv(INPUT)
               .withColumn("ingestion_timestamp", F.current_timestamp()))
        logging.info("Bronze rows read: %s", raw.count())
        valid, rejected = transform_data(raw)
        valid = valid.withColumnRenamed("effective_date_parsed", "effective_date")
        valid = valid.withColumn("total_compensation_cost",
                                 F.col("annual_salary") + F.col("employer_contribution"))
        gold = (valid.groupBy("employer_id", "employer_name", "department", "currency")
                .agg(F.countDistinct("employee_id").alias("employee_count"),
                     F.sum("annual_salary").alias("total_annual_salary"),
                     F.sum("employer_contribution").alias("total_employer_contribution"),
                     F.sum("total_compensation_cost").alias("total_compensation_cost"))
                .orderBy("employer_name", "department"))
        raw.write.mode("overwrite").option("header", True).csv(str(OUTPUT / "bronze"))
        valid.write.mode("overwrite").option("header", True).csv(str(OUTPUT / "silver"))
        rejected.write.mode("overwrite").option("header", True).csv(str(OUTPUT / "rejected"))
        gold.write.mode("overwrite").option("header", True).csv(str(OUTPUT / "gold"))
        logging.info("Silver rows: %s; rejected rows: %s; Gold rows: %s",
                     valid.count(), rejected.count(), gold.count())
        gold.show(truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
