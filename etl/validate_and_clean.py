"""
Main Spark ETL job (TASK-2). Reads /raw/{run_date}/, validates each table,
rejects bad rows to /raw/_rejects, deduplicates, encrypts PHI columns, and writes clean Parquet to /curated/{table}/run_date={date}/.
"""

import argparse
import sys

from pyspark.sql.functions import col, lit

from shared.spark_session import get_spark_session
from shared.config.phi_fields import PHI_FIELDS
from security.encryption import encrypt_phi_columns
from etl.schemas import get_required_columns, get_dedupe_keys

# Dynamically pull all table names from your PHI fields configuration dictionary
TABLES = list(PHI_FIELDS.keys())

def split_valid_invalid(df, table: str):
    """
    Validates a Spark DataFrame against required columns for a given table.
    Splits the data into two separate DataFrames:
    1. valid_df: Rows that have all required fields populated.
    2. invalid_df: Rows missing one or more required fields, tagged with a rejection reason.
    """
    required = get_required_columns(table)

    # If no required columns are defined for this table, treat all rows as valid
    if not required:
        return df, df.limit(0).withColumn("_reject_reason", lit(None))

    # Dynamically build a condition to check if any required column is null/missing e.g patient_id IS NULL | admission_date IS NULL | encounters is NULL
    condition = None
    for c in required:
        is_null = col(c).isNull()
        condition = is_null if condition is None else (condition | is_null)

    # Filter out rows matching the failure condition and tag them with a rejection reason
    invalid_df = df.filter(condition).withColumn(
        "_reject_reason", lit(f"missing one of required fields: {required}")
    )
    
    # Keep rows that do NOT match the failure condition
    valid_df = df.filter(~condition)
    
    return valid_df, invalid_df

def dedupe(df, table: str):
    """
    Removes duplicate records from the DataFrame.
    - If specific deduplication keys are configured for the table, drops duplicates based on those columns.
    - Otherwise, falls back to dropping duplicates across the entire row.
    """
    keys = get_dedupe_keys(table)
    if keys:
        return df.dropDuplicates(keys)
    return df.dropDuplicates()

def process_table(spark, table: str, run_date: str) -> dict:
    """
    Processes a single table's dataset for a specific date:
    Reads raw CSV -> Validates -> Logs rejects -> Dedupes -> Encrypts PHI -> Writes Parquet.
    Returns a dictionary summary of processing counts.
    """
    raw_path = f"/raw/{run_date}/{table}.csv"

    # Attempt to read the raw CSV dataset from HDFS/storage
    try:
        df = spark.read.option("header", "true").csv(raw_path)
    except Exception as e:
        print(f"[skip] could not read {raw_path}: {e}")
        return {"table": table, "read": 0, "valid": 0, "rejected": 0, "written": 0}

    read_count = df.count()

    # Split rows into valid and invalid based on mandatory fields
    valid_df, invalid_df = split_valid_invalid(df, table)
    rejected_count = invalid_df.count()

    # If there are bad records, persist them to the rejects zone as JSON files
    if rejected_count > 0:
        reject_path = f"/raw/_rejects/{run_date}/{table}"
        invalid_df.write.mode("overwrite").json(reject_path)

    # Deduplicate the valid records
    deduped_df = dedupe(valid_df, table)
    valid_count = deduped_df.count()

    # Encrypt all designated Protected Health Information (PHI) columns
    encrypted_df = encrypt_phi_columns(deduped_df, table)

    # Write clean, encrypted data into the curated zone, partitioned by run_date
    curated_path = f"/curated/{table}"
    (
        encrypted_df
        .withColumn("run_date", lit(run_date))
        .write
        .mode("overwrite")
        .partitionBy("run_date")
        .parquet(curated_path) #group data by type (column)
    )

    # Return summary metrics for reporting
    return {
        "table": table,
        "read": read_count,
        "valid": valid_count,
        "rejected": rejected_count,
        "written": valid_count,
    }

def run(run_date: str):
    """
    Orchestrates the entire ETL pipeline execution across all defined tables for a given run date.
    """
    # Initialize a localized PySpark session
    spark = get_spark_session("etl-validate-and-clean")

    print(f"[start] processing run_date={run_date}")
    results = []

    # Loop through each table configuration, execute the pipeline, and track metrics
    for table in TABLES:
        result = process_table(spark, table, run_date)
        results.append(result)
        print(f"[{table}] read={result['read']} valid={result['valid']} "
              f"rejected={result['rejected']} written={result['written']}")

    # Clean up and shut down the Spark instance
    spark.stop()

    # Safety check: Ensure at least some data was successfully written across the pipeline
    total_written = sum(r["written"] for r in results)
    if total_written == 0:
        print("[error] no rows were written for any table - check /raw path and run_date.")
        sys.exit(1)

    print(f"[done] wrote {total_written} total rows across {len(results)} tables.")


if __name__ == "__main__":
    # Set up command-line argument parsing to require a target date (e.g., --date 2026-08-26)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Run date, YYYY-MM-DD, matching a folder under /raw")
    args = parser.parse_args()

    # Kick off the ETL process
    run(args.date)
    