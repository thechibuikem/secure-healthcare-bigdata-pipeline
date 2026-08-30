"""
Small FastAPI backend wrapping security/access_control.py and
security/audit_log.py for the React frontend. No new logic here -
this is purely a thin layer that turns Python function calls into
HTTP endpoints (FR-7 is a display layer, not new backend logic).
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from security.access_control import get_view, request_column
from security.audit_log import read_audit_log
from shared.config.spark_session import get_spark_session
from shared.config.roles import ROLES

app = FastAPI(title="Healthcare Pipeline Demo API")

# allow the React dev server to call this API from the browser
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# start one shared Spark session when the API boots, reuse it for every
# request - starting a new Spark session per request would be very slow
_spark = get_spark_session("api")

def df_to_records(df, limit: int = 50) -> list[dict]:
    if df is None:
        return []
    return [row.asDict() for row in df.limit(limit).collect()]

@app.get("/")
def greet():
    return {"ataraxia says pipeline is running mate!"}

@app.get("/roles")
def list_roles():
    return {"roles": list(ROLES.keys())}

@app.get("/view")
def view(role: str, table: str):
    """Returns this role's permitted view of a table."""
    df = get_view(role, table)
    if df is None:
        return {"allowed": False, "columns": [], "rows": []}
    return {
        "allowed": True,
        "columns": df.columns,
        "rows": df_to_records(df),
    }

@app.get("/request-column")
def request_column_endpoint(role: str, table: str, column: str):
    """Try to fetch a single column - used for the 'blocked column' demo."""
    result = request_column(role, table, column)
    if result is None:
        return {"allowed": False, "rows": []}
    return {"allowed": True, "rows": df_to_records(result)}

@app.get("/audit-log")
def audit_log():
    entries = read_audit_log()
    # most recent first
    return {"entries": list(reversed(entries))}

@app.get("/marts/{report_name}")
def marts(report_name: str):
    hdfs_namenode = os.environ.get("HDFS_NAMENODE", "hdfs://localhost:9000")
    df = _spark.read.parquet(f"{hdfs_namenode}/marts/{report_name}")
    return {"columns": df.columns, "rows": df_to_records(df, limit=200)}