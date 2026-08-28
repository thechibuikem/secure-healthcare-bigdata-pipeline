"""
Spark SQL jobs building anonymized summary reports into /marts.
Only reads non-PHI or already-aggregated columns - never touches
encrypted PHI fields (FR-6.2).
"""

import argparse
import os
from datetime import date

from pyspark.sql.functions import col, count, floor, year, to_date

from shared.spark_session import get_spark_session

HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://localhost:9000")


def condition_prevalence_by_age(spark, run_date: str):
    conditions = spark.read.parquet(f"{HDFS_NAMENODE}/curated/conditions")
    patients = spark.read.parquet(f"{HDFS_NAMENODE}/curated/patients")

    # Join conditions with patient birthdates for age calculation.
    # Note: BIRTHDATE is non-PHI, allowing safe usage for analytical age banding.
    joined = conditions.join(
        patients.select("Id", "BIRTHDATE"),
        conditions["PATIENT"] == patients["Id"],
    ) #So we are joining matching patients id and birthdate to condition

    with_age = joined.withColumn(
        "age", floor((year(to_date(col("START"))) - year(to_date(col("BIRTHDATE")))))
    ) #with column creates a new coln age, and fills it with comp result

    with_age_band = with_age.withColumn(
        "age_band", (floor(col("age") / 10) * 10).cast("int")
    )

    result = (
        with_age_band
        .groupBy("DESCRIPTION", "age_band")
        .agg(count("*").alias("patient_count"))
        .orderBy("DESCRIPTION", "age_band")
    )

    result.write.mode("overwrite").parquet(
        f"{HDFS_NAMENODE}/marts/condition_prevalence/run_date={run_date}"
    )
    return result

