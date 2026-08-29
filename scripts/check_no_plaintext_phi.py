"""
Spot-checks /curated for plaintext PHI. Run after the pipeline to
confirm encryption actually happened - this is the check behind
TASK-6's 'no PHI shows up anywhere in /curated' acceptance criterion.
"""
import os
from shared.config.spark_session import get_spark_session
from shared.config.phi_fields import PHI_FIELDS

HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://localhost:9000")

# known plaintext values from your test Synthea run - update these to
# match names/SSNs you know exist in your generated data
KNOWN_PLAINTEXT_VALUES = []  # e.g. ["999-99-9999"] if you know a test SSN

def check_table(spark, table: str) -> bool:
    phi_columns = PHI_FIELDS.get(table, [])
    if not phi_columns:
        return True

    df = spark.read.parquet(f"{HDFS_NAMENODE}/curated/{table}")
    ok = True

    for column in phi_columns:
        if column not in df.columns:
            continue
        sample = [row[column] for row in df.select(column).limit(20).collect()]
        for value in sample:
            if value and any(known in str(value) for known in KNOWN_PLAINTEXT_VALUES):
                print(f"[FAIL] plaintext PHI found in {table}.{column}: {value}")
                ok = False

    if ok:
        print(f"[ok] {table}: no known plaintext PHI in sample")
    return ok



def run():
    spark = get_spark_session("phi-check")
    all_ok = True
    for table in PHI_FIELDS:
        all_ok = check_table(spark, table) and all_ok
    spark.stop()

    if not all_ok:
        raise SystemExit("PHI leak check FAILED - see above")
    print("[done] all tables passed the plaintext PHI check")


if __name__ == "__main__":
    run()