"""Validation script for Bronze Delta Lake tables."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_bronze_path, get_lakehouse_path, get_spark_session


class BronzeValidation:
    """Collect and report Bronze validation checks."""

    def __init__(self):
        self.results = []

    def check(self, name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        self.results.append((name, status, detail))
        marker = "+" if condition else "X"
        print(f"  [{marker}] {name}: {detail}")

    def summary(self):
        passed = sum(1 for _, s, _ in self.results if s == "PASS")
        total = len(self.results)
        print(f"\n{'=' * 50}")
        print(f"  BRONZE VALIDATION: {passed}/{total} checks passed")
        print(f"{'=' * 50}")
        if passed < total:
            print("\n  Failed checks:")
            for name, status, detail in self.results:
                if status == "FAIL":
                    print(f"    - {name}: {detail}")
        return passed == total


def main():
    """Run all Bronze validation checks."""
    spark = get_spark_session("Bronze-Validation")
    v = BronzeValidation()

    # Table definitions: (name, min_rows, max_rows, source_tag)
    batch_tables = [
        ("users", 5000, 5000, "mysql"),
        ("subscriptions", 6000, 8000, "mysql"),
        ("content", 2000, 2000, "postgres"),
        ("ratings", 15000, 18000, "postgres"),
        ("content_metadata", 2000, 2000, "csv"),
        ("viewing_events_batch", 50000, 50000, "json"),
        ("campaigns", 50, 50, "excel"),
    ]

    print("\n--- Batch Bronze Tables ---")
    for table_name, min_rows, max_rows, source_tag in batch_tables:
        path = get_bronze_path(table_name)
        try:
            df = spark.read.format("delta").load(path)
            count = df.count()
            v.check(
                f"bronze/{table_name} row count",
                min_rows <= count <= max_rows,
                f"{count} rows (expected {min_rows}-{max_rows})",
            )
        except Exception as e:
            v.check(f"bronze/{table_name} row count", False, f"Error: {e}")

    print("\n--- Streaming Bronze Table ---")
    viewing_events_path = get_bronze_path("viewing_events")
    try:
        df = spark.read.format("delta").load(viewing_events_path)
        v.check("bronze/viewing_events exists", True, f"{df.count()} rows")
    except Exception:
        v.check("bronze/viewing_events exists", False, "Table not found (run Kafka streaming first)")

    print("\n--- Audit Columns ---")
    for table_name, _, _, source_tag in batch_tables:
        path = get_bronze_path(table_name)
        try:
            df = spark.read.format("delta").load(path)
            has_ingested = "_ingested_at" in df.columns
            has_source = "_source" in df.columns
            v.check(
                f"bronze/{table_name} has _ingested_at",
                has_ingested,
                "present" if has_ingested else "MISSING",
            )
            if has_source:
                actual_source = df.select("_source").first()[0]
                v.check(
                    f"bronze/{table_name} _source tag",
                    actual_source == source_tag,
                    f"'{actual_source}' (expected '{source_tag}')",
                )
        except Exception:
            pass  # Already reported above

    print("\n--- Delta Log ---")
    lakehouse = get_lakehouse_path()
    if lakehouse.startswith("s3a://"):
        import boto3
        s3 = boto3.client("s3")
        bucket = lakehouse.replace("s3a://", "").split("/")[0]
        for table_name, _, _, _ in batch_tables:
            prefix = f"bronze/{table_name}/_delta_log/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            has_log = resp.get("KeyCount", 0) > 0
            v.check(f"bronze/{table_name} _delta_log", has_log, "exists" if has_log else "MISSING")
    else:
        for table_name, _, _, _ in batch_tables:
            log_path = os.path.join(lakehouse, "bronze", table_name, "_delta_log")
            has_log = os.path.isdir(log_path)
            v.check(f"bronze/{table_name} _delta_log", has_log, "exists" if has_log else "MISSING")

    all_passed = v.summary()
    spark.stop()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
