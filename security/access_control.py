"""
Role-gated views over /curated. This is what the API (TASK-7) will call -
it never exposes /curated directly, only through here.
"""

import os
from shared.config.spark_session import get_spark_session
from shared.config.phi_fields import get_phi_fields
from shared.config.roles import role_exists, can_access_table, can_view_phi
from security.decrypt_view import decrypt_if_allowed
from security.audit_log import log_access

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Fetch the HDFS cluster address from environment variables, defaulting to a local instance
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://localhost:9000")


def get_view(role: str, table: str):
    """
    Returns a Spark DataFrame for this role's permitted view of a table.
    PHI columns are decrypted if the role is allowed to see them,
    dropped entirely otherwise. Every call is logged.
    """
    # Authorization check: verify the role actually exists and has permission to query this table.
    # If unauthorized, log the failed attempt with an empty column list and return None.
    if not role_exists(role) or not can_access_table(role, table):
        log_access(role, table, columns=[], allowed=False)
        return None

    # Initialize a Spark session and load the secure, curated Parquet dataset for the requested table
    spark = get_spark_session("access-control")
    df = spark.read.parquet(f"{HDFS_NAMENODE}/curated/{table}")

    # Identify which columns contain Protected Health Information (PHI) and check if this role can view them
    phi_columns = get_phi_fields(table)
    allowed_to_view_phi = can_view_phi(role)

    if allowed_to_view_phi:
        # If authorized for PHI, define a Spark UDF that dynamically decrypts column values for this role
        decrypt_udf = udf(lambda v: decrypt_if_allowed(role, v), StringType())
        for column in phi_columns:
            if column in df.columns:
                df = df.withColumn(column, decrypt_udf(df[column]))
        returned_columns = df.columns
    else:
        # For lower-privileged roles (like analysts), completely drop PHI columns instead of exposing ciphertext
        returned_columns = [c for c in df.columns if c not in phi_columns]
        df = df.select(*returned_columns)

    # Log the successful data access event, tracking the user's role, table, and columns returned
    log_access(role, table, columns=returned_columns, allowed=True)
    return df

def request_column(role: str, table: str, column: str):
    """
    Explicit single-column request - used by the frontend's 'try a
    blocked column' demo button. Returns None and logs a denial if the
    role can't see that column.
    """
    phi_columns = get_phi_fields(table)
    is_phi = column in phi_columns
    allowed = (not is_phi) or can_view_phi(role)

    log_access(role, table, columns=[column], allowed=allowed)

    if not allowed:
        return None

    view = get_view(role, table)
    return view.select(column) if view is not None else None