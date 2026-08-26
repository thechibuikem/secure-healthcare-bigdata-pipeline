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