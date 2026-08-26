def write_manifest(manifest: dict, output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

import argparse #Parses command-line arguments (like --source and --date)
import os
import subprocess #Runs external shell commands directly from Python
import sys #control exit
from datetime import datetime, timezone

from ingestion.manifest import build_manifest, write_manifest

EXPECTED_FILES = [
    "patients.csv",
    "encounters.csv",
    "conditions.csv",
    "medications.csv",
    "observations.csv",
    "procedures.csv",
]

def hdfs_path_exists(path: str) -> bool:
    result = subprocess.run(["hdfs", "dfs", "-test", "-e", path])
    return result.returncode == 0

def hdfs_mkdir(path: str) -> None:
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", path], check=True)

def hdfs_put(local_path: str, hdfs_path: str) -> None:
    subprocess.run(["hdfs", "dfs", "-put", local_path, hdfs_path], check=True)

def load_to_hdfs(source_dir: str, run_date: str = None) -> None:
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    hdfs_target = f"/raw/{run_date}"

    # idempotent check for if we've already loaded this file
    if hdfs_path_exists(hdfs_target):
        print(f"[skip] {hdfs_target} already exists - load already ran for this date.")
        return

    hdfs_mkdir(hdfs_target)

    loaded_files = []
    skipped_files = []

    for filename in EXPECTED_FILES:
        local_path = os.path.join(source_dir, filename)

    # a check to determine wether we skip file
        if not os.path.exists(local_path):
            print(f"[skip] {filename} not found in {source_dir}")
            skipped_files.append(filename)
            continue

        try:
            # a very basic readability check before trusting the file
            with open(local_path, "r", encoding="utf-8") as f:
                f.readline()
        except Exception as e:
            print(f"[reject] {filename} could not be read: {e}")
            skipped_files.append(filename)
            continue

        
        hdfs_put(local_path, f"{hdfs_target}/{filename}") #  store file in hdfs
        loaded_files.append({"filename": filename, "local_path": local_path})
        print(f"[ok] loaded {filename}")

    # sanity check to ensure files were loaded
    if not loaded_files:
        print("[error] no files were loaded - aborting manifest write.")
        sys.exit(1)

    # manifest building
    manifest = build_manifest(loaded_files)
    manifest["skipped_files"] = skipped_files

    # stores manifest temporarily then sends it to json
    local_manifest_path = "/tmp/_manifest.json"
    write_manifest(manifest, local_manifest_path)
    hdfs_put(local_manifest_path, f"{hdfs_target}/_manifest.json")

    print(f"[done] loaded {len(loaded_files)} files, skipped {len(skipped_files)}. "
          f"Manifest written to {hdfs_target}/_manifest.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="tools/synthea/output/csv",
                         help="Local folder containing Synthea's CSV output")
    parser.add_argument("--date", default=None,
                         help="Run date, YYYY-MM-DD. Defaults to today.")
    args = parser.parse_args()

    load_to_hdfs(args.source, args.date)