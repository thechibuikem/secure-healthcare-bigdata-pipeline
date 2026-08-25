"""
Shared Spark session helper. Every script that needs Spark should call
get_spark_session() from here instead of building its own - keeps config
consistent across ingestion, ETL, security, and analytics.
"""

import os
import py.spark.sql import SparkSession

def get_spark_session(app_name: str = "healthcare-piprline") -> SparkSession:
    """
    Start (or reuse) a Spark session.

    Reads SPARK_MASTER from the environment if set (defaults to local[*],
    which runs Spark using all cores on your machine - fine for dev use, no real cluster needed).
    """
    master = os.environ.get("SPARK_MASTER","local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark