"""
Builds the manifest - a receipt listing what was loaded, so we can always
check nothing was lost or corrupted during ingestion.
"""

import hashlib # generates an MD5 hash of the file contents
import json
import os
from datetime import datetime, timezone

def compute_checksum(filepath: str) -> str:
    """MD5 checksum of a file's contents - good enough to detect corruption,
    not meant for security."""
    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def count_rows(filepath: str) -> int:
    """Count data rows in a CSV, not counting the header."""
    with open(filepath, "r", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1

def build_manifest(loaded_files: list[dict]) -> dict:
    """
    loaded_files: list of {"filename": str, "local_path": str}
    Returns the manifest dict, ready to write as JSON.
    """
    entries = []
    for file_info in loaded_files:
        entries.append({
            "filename": file_info["filename"],
            "row_count": count_rows(file_info["local_path"]),
            "checksum": compute_checksum(file_info["local_path"]),
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        })

    return {
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "files": entries,
    }

def write_manifest(manifest: dict, output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)