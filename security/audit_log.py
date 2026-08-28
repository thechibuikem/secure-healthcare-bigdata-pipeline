"""
Append-only log of every access to PHI-bearing data. Never exposes an
update or delete function on purpose - once written, an entry is
permanent (FR-5.4).
"""

import json
import os
import subprocess
from datetime import datetime, timezone

# Resolve the HDFS NameNode address from the environment, defaulting to a local instance
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://localhost:9000")
# Define the HDFS destination path for the append-only JSONL audit log
AUDIT_LOG_PATH = f"{HDFS_NAMENODE}/curated/_audit_log/audit_log.jsonl"

def log_access(role: str, table: str, columns: list[str], allowed: bool) -> None:
    """
    Records a data access attempt (authorized or unauthorized) against PHI-bearing tables.
    Appends a new JSON line containing the access metadata and a UTC timestamp to HDFS.
    """
    entry = {
        "role": role,
        "table": table,
        "columns": columns,
        "allowed": allowed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    line = json.dumps(entry) + "\n" #like json.stringify

    # Write the single audit entry to a local temporary file first,
    # as Python does not have a direct HDFS line-append API.
    tmp_path = "/tmp/_audit_entry.jsonl"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(line)

# Attempt to append the temporary file to the existing audit log on HDFS.
    # Note: check=False because this fails if the audit log file doesn't exist yet.
    subprocess.run(
        ["hdfs", "dfs", "-appendToFile", tmp_path, AUDIT_LOG_PATH],
        check=False,
    )

    # Check whether the audit log file was successfully appended to or if it's missing.
    exists = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", AUDIT_LOG_PATH]
    ).returncode == 0
    
    # If the audit log file does not exist yet, create it by uploading the temporary file.
    if not exists:
        subprocess.run(["hdfs", "dfs", "-put", tmp_path, AUDIT_LOG_PATH], check=True)

def read_audit_log() -> list[dict]:
    """
    Reads back and parses every audit entry from HDFS.
    Used by the administrative API and frontend interface to display audit history.
    """
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", AUDIT_LOG_PATH],
        capture_output=True, text=True,
    )

    # Return an empty list if the log file cannot be read or doesn't exist
    if result.returncode != 0:
        return []
    
    # Filter out empty lines and parse each JSON line back into a dictionary
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    return [json.loads(l) for l in lines]

    # Split the output into individual lines and drop any empty ones
    raw_lines = result.stdout.splitlines()
    lines = []
    for l in raw_lines:
        if l.strip(): # evals to false on ""
            lines.append(l)

    # Convert each JSON string line back into a dictionary
    parsed_entries = []
    for l in lines:
        parsed_entries.append(json.loads(l))

    return parsed_entries