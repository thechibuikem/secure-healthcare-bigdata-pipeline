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


def encounter_volume_by_month(spark, run_date: str):
    encounters = spark.read.parquet(f"{HDFS_NAMENODE}/curated/encounters")

    result = (
        encounters
        .withColumn("month", col("START").substr(1, 7))  # YYYY-MM
        .groupBy("month")
        .agg(count("*").alias("encounter_count"))
        .orderBy("month")
    )

    result.write.mode("overwrite").parquet(
        f"{HDFS_NAMENODE}/marts/encounter_volume/run_date={run_date}"
    )
    return result


def medication_trend(spark, run_date: str):
    medications = spark.read.parquet(f"{HDFS_NAMENODE}/curated/medications")

    result = (
        medications
        .groupBy("DESCRIPTION")
        .agg(count("*").alias("prescription_count"))
        .orderBy(col("prescription_count").desc())
    )

    result.write.mode("overwrite").parquet(
        f"{HDFS_NAMENODE}/marts/medication_trend/run_date={run_date}"
    )
    return result

def run(run_date: str):
    spark = get_spark_session("aggregates")

    print("[1/3] condition prevalence by age band")
    r1 = condition_prevalence_by_age(spark, run_date)
    print(f"  wrote {r1.count()} rows")

    print("[2/3] encounter volume by month")
    r2 = encounter_volume_by_month(spark, run_date)
    print(f"  wrote {r2.count()} rows")

    print("[3/3] medication trend")
    r3 = medication_trend(spark, run_date)
    print(f"  wrote {r3.count()} rows")

    spark.stop()
    print("[done] all 3 marts written")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today()))
    args = parser.parse_args()
    run(args.date)
