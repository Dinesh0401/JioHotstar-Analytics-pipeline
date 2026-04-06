"""Validation script for Silver Delta Lake tables."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import (
    get_bronze_path,
    get_lakehouse_path,
    get_silver_path,
    get_spark_session,
)


class SilverValidation:
    """Collect and report Silver validation checks."""

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
        print(f"  SILVER VALIDATION: {passed}/{total} checks passed")
        print(f"{'=' * 50}")
        if passed < total:
            print("\n  Failed checks:")
            for name, status, detail in self.results:
                if status == "FAIL":
                    print(f"    - {name}: {detail}")
        return passed == total


def main():
    """Run all Silver validation checks."""
    spark = get_spark_session("Silver-Validation")
    v = SilverValidation()

    # ---- silver_users (checks 1-4) ----
    print("\n--- silver_users ---")
    try:
        silver_users = spark.read.format("delta").load(get_silver_path("users"))
        bronze_users = spark.read.format("delta").load(get_bronze_path("users"))

        su_count = silver_users.count()
        bu_count = bronze_users.count()
        v.check("1. silver_users row count", su_count == bu_count,
                f"{su_count} silver == {bu_count} bronze")

        null_uid = silver_users.filter("user_id IS NULL").count()
        v.check("2. silver_users no null user_id", null_uid == 0,
                f"{null_uid} nulls")

        null_email_valid = silver_users.filter("is_valid_email IS NULL").count()
        v.check("3. silver_users is_valid_email populated", null_email_valid == 0,
                f"{null_email_valid} nulls")

        null_os = silver_users.filter("device_os IS NULL").count()
        v.check("4. silver_users device_os populated", null_os == 0,
                f"{null_os} nulls")
    except Exception as e:
        v.check("silver_users load", False, str(e))

    # ---- silver_subscriptions (checks 5-6) ----
    print("\n--- silver_subscriptions ---")
    try:
        silver_subs = spark.read.format("delta").load(get_silver_path("subscriptions"))
        bronze_subs = spark.read.format("delta").load(get_bronze_path("subscriptions"))

        ss_count = silver_subs.count()
        bs_count = bronze_subs.count()
        v.check("5. silver_subscriptions row count", ss_count == bs_count,
                f"{ss_count} silver == {bs_count} bronze")

        null_dur = silver_subs.filter("duration_days IS NULL").count()
        v.check("6. silver_subscriptions duration_days no nulls", null_dur == 0,
                f"{null_dur} nulls")
    except Exception as e:
        v.check("silver_subscriptions load", False, str(e))

    # ---- silver_content_enriched (checks 7-9) ----
    print("\n--- silver_content_enriched ---")
    try:
        silver_content = spark.read.format("delta").load(get_silver_path("content_enriched"))
        bronze_content = spark.read.format("delta").load(get_bronze_path("content"))

        sc_count = silver_content.count()
        bc_count = bronze_content.count()
        v.check("7. silver_content_enriched row count", sc_count == bc_count,
                f"{sc_count} silver == {bc_count} bronze")

        null_rt = silver_content.filter("runtime_minutes IS NULL").count()
        v.check("8. silver_content_enriched runtime_minutes no nulls", null_rt == 0,
                f"{null_rt} nulls")

        # Check genre_array is actually an array type
        genre_type = str(silver_content.schema["genre_array"].dataType)
        is_array = "ArrayType" in genre_type
        v.check("9. silver_content_enriched genre_array is array", is_array,
                genre_type)
    except Exception as e:
        v.check("silver_content_enriched load", False, str(e))

    # ---- silver_ratings_clean (checks 10-12) ----
    print("\n--- silver_ratings_clean ---")
    try:
        silver_ratings = spark.read.format("delta").load(get_silver_path("ratings_clean"))
        bronze_ratings = spark.read.format("delta").load(get_bronze_path("ratings"))

        sr_count = silver_ratings.count()
        br_count = bronze_ratings.count()
        v.check("10. silver_ratings_clean dedup check", sr_count <= br_count,
                f"{sr_count} silver <= {br_count} bronze")

        # No duplicate (user_id, content_id) pairs
        distinct_pairs = silver_ratings.select("user_id", "content_id").distinct().count()
        v.check("11. silver_ratings_clean no duplicate user+content",
                distinct_pairs == sr_count,
                f"{distinct_pairs} distinct pairs == {sr_count} rows")

        # rating_value in 0-5 range
        from pyspark.sql.functions import max as spark_max, min as spark_min
        stats = silver_ratings.agg(
            spark_min("rating_value").alias("min_val"),
            spark_max("rating_value").alias("max_val"),
        ).first()
        min_val = float(stats["min_val"])
        max_val = float(stats["max_val"])
        v.check("12. silver_ratings_clean rating_value in 0-5",
                min_val >= 0.0 and max_val <= 5.0,
                f"range [{min_val}, {max_val}]")
    except Exception as e:
        v.check("silver_ratings_clean load", False, str(e))

    # ---- silver_viewing_events (checks 13-16) ----
    print("\n--- silver_viewing_events ---")
    try:
        silver_events = spark.read.format("delta").load(get_silver_path("viewing_events"))
        bronze_batch = spark.read.format("delta").load(get_bronze_path("viewing_events_batch"))

        se_count = silver_events.count()
        bb_count = bronze_batch.count()
        # Silver may have slightly fewer rows than batch due to filtering invalid events
        # (null timestamps), but should be close. Allow up to 1% difference.
        v.check("13. silver_viewing_events row count", se_count >= bb_count * 0.99,
                f"{se_count} silver ~= {bb_count} batch bronze")

        # No duplicate event_id
        distinct_events = silver_events.select("event_id").distinct().count()
        v.check("14. silver_viewing_events no duplicate event_id",
                distinct_events == se_count,
                f"{distinct_events} distinct == {se_count} total")

        # event_ts is timestamp type
        ts_type = str(silver_events.schema["event_ts"].dataType)
        v.check("15. silver_viewing_events event_ts is timestamp",
                "Timestamp" in ts_type, ts_type)

        # event_date populated
        null_date = silver_events.filter("event_date IS NULL").count()
        v.check("16. silver_viewing_events event_date populated", null_date == 0,
                f"{null_date} nulls")
    except Exception as e:
        v.check("silver_viewing_events load", False, str(e))

    # ---- silver_campaigns (checks 17-18) ----
    print("\n--- silver_campaigns ---")
    try:
        silver_campaigns = spark.read.format("delta").load(get_silver_path("campaigns"))

        null_amount = silver_campaigns.filter("budget_amount IS NULL").count()
        v.check("17. silver_campaigns budget_amount is numeric", null_amount == 0,
                f"{null_amount} nulls")

        null_currency = silver_campaigns.filter("budget_currency IS NULL").count()
        v.check("18. silver_campaigns budget_currency populated", null_currency == 0,
                f"{null_currency} nulls")
    except Exception as e:
        v.check("silver_campaigns load", False, str(e))

    # ---- Cross-table checks (19-20) ----
    print("\n--- Cross-table checks ---")
    silver_tables = ["users", "subscriptions", "content_enriched",
                     "ratings_clean", "viewing_events", "campaigns"]

    # Check 19: _silver_processed_at exists in all tables
    all_have_ts = True
    missing_ts = []
    for table in silver_tables:
        try:
            df = spark.read.format("delta").load(get_silver_path(table))
            if "_silver_processed_at" not in df.columns:
                all_have_ts = False
                missing_ts.append(table)
        except Exception:
            all_have_ts = False
            missing_ts.append(table)
    v.check("19. All silver tables have _silver_processed_at", all_have_ts,
            "all present" if all_have_ts else f"missing in: {missing_ts}")

    # Check 20: _delta_log exists for all tables
    lakehouse = get_lakehouse_path()
    all_have_log = True
    missing_log = []
    if lakehouse.startswith("s3a://"):
        import boto3
        s3 = boto3.client("s3")
        bucket = lakehouse.replace("s3a://", "").split("/")[0]
        for table in silver_tables:
            prefix = f"silver/{table}/_delta_log/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                all_have_log = False
                missing_log.append(table)
    else:
        for table in silver_tables:
            log_path = os.path.join(lakehouse, "silver", table, "_delta_log")
            if not os.path.isdir(log_path):
                all_have_log = False
                missing_log.append(table)
    v.check("20. All silver tables _delta_log exists", all_have_log,
            "all present" if all_have_log else f"missing: {missing_log}")

    all_passed = v.summary()
    spark.stop()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
