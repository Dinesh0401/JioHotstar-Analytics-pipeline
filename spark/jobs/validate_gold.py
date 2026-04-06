"""Validation script for Gold Delta Lake tables."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_gold_path, get_lakehouse_path, get_spark_session


class GoldValidation:
    """Collect and report Gold validation checks."""

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
        print(f"  GOLD VALIDATION: {passed}/{total} checks passed")
        print(f"{'=' * 50}")
        if passed < total:
            print("\n  Failed checks:")
            for name, status, detail in self.results:
                if status == "FAIL":
                    print(f"    - {name}: {detail}")
        return passed == total


def main():
    """Run all Gold validation checks."""
    spark = get_spark_session("Gold-Validation")
    v = GoldValidation()

    # ---- gold_daily_active_users (checks 1-2) ----
    print("\n--- gold_daily_active_users ---")
    try:
        dau = spark.read.format("delta").load(get_gold_path("daily_active_users"))
        dau_count = dau.count()
        v.check("1. gold_daily_active_users row count > 0", dau_count > 0,
                f"{dau_count} rows")
        null_dates = dau.filter("event_date IS NULL").count()
        v.check("2. gold_daily_active_users no null event_date", null_dates == 0,
                f"{null_dates} nulls")
    except Exception as e:
        v.check("gold_daily_active_users load", False, str(e))

    # ---- gold_content_watch_metrics (checks 3-4) ----
    print("\n--- gold_content_watch_metrics ---")
    try:
        cwm = spark.read.format("delta").load(get_gold_path("content_watch_metrics"))
        cwm_count = cwm.count()
        v.check("3. gold_content_watch_metrics row count > 0", cwm_count > 0,
                f"{cwm_count} rows")
        from pyspark.sql.functions import min as spark_min
        min_views = cwm.agg(spark_min("total_views")).first()[0]
        v.check("4. gold_content_watch_metrics total_views > 0", min_views >= 1,
                f"min total_views = {min_views}")
    except Exception as e:
        v.check("gold_content_watch_metrics load", False, str(e))

    # ---- gold_genre_popularity (checks 5-6) ----
    print("\n--- gold_genre_popularity ---")
    try:
        genre = spark.read.format("delta").load(get_gold_path("genre_popularity"))
        genre_count = genre.count()
        v.check("5. gold_genre_popularity row count > 0", genre_count > 0,
                f"{genre_count} rows")
        null_genre = genre.filter("genre IS NULL").count()
        v.check("6. gold_genre_popularity no null genre", null_genre == 0,
                f"{null_genre} nulls")
    except Exception as e:
        v.check("gold_genre_popularity load", False, str(e))

    # ---- gold_user_engagement (checks 7-8) ----
    print("\n--- gold_user_engagement ---")
    try:
        engage = spark.read.format("delta").load(get_gold_path("user_engagement"))
        engage_count = engage.count()
        v.check("7. gold_user_engagement row count > 0", engage_count > 0,
                f"{engage_count} rows")
        null_uid = engage.filter("user_id IS NULL").count()
        v.check("8. gold_user_engagement no null user_id", null_uid == 0,
                f"{null_uid} nulls")
    except Exception as e:
        v.check("gold_user_engagement load", False, str(e))

    # ---- gold_subscription_metrics (checks 9) ----
    print("\n--- gold_subscription_metrics ---")
    try:
        sub_metrics = spark.read.format("delta").load(get_gold_path("subscription_metrics"))
        from spark.conf.spark_settings import get_silver_path
        silver_subs = spark.read.format("delta").load(get_silver_path("subscriptions"))
        distinct_plans = silver_subs.select("plan_id").distinct().count()
        sm_count = sub_metrics.count()
        v.check("9. gold_subscription_metrics row count == distinct plans",
                sm_count == distinct_plans,
                f"{sm_count} gold rows == {distinct_plans} distinct plans")
    except Exception as e:
        v.check("gold_subscription_metrics load", False, str(e))

    # ---- gold_content_ratings_summary (checks 10-11) ----
    print("\n--- gold_content_ratings_summary ---")
    try:
        ratings = spark.read.format("delta").load(get_gold_path("content_ratings_summary"))
        rat_count = ratings.count()
        v.check("10. gold_content_ratings_summary row count > 0", rat_count > 0,
                f"{rat_count} rows")
        from pyspark.sql.functions import max as spark_max, min as spark_min
        stats = ratings.agg(
            spark_min("avg_rating").alias("min_avg"),
            spark_max("avg_rating").alias("max_avg"),
        ).first()
        min_avg = float(stats["min_avg"])
        max_avg = float(stats["max_avg"])
        v.check("11. gold_content_ratings_summary avg_rating in 0-5",
                min_avg >= 0.0 and max_avg <= 5.0,
                f"range [{min_avg:.2f}, {max_avg:.2f}]")
    except Exception as e:
        v.check("gold_content_ratings_summary load", False, str(e))

    # ---- Cross-table checks (12-13) ----
    print("\n--- Cross-table checks ---")
    gold_tables = ["daily_active_users", "content_watch_metrics", "genre_popularity",
                   "user_engagement", "subscription_metrics", "content_ratings_summary"]

    # Check 12: _gold_processed_at in all tables
    all_have_ts = True
    missing_ts = []
    for table in gold_tables:
        try:
            df = spark.read.format("delta").load(get_gold_path(table))
            if "_gold_processed_at" not in df.columns:
                all_have_ts = False
                missing_ts.append(table)
        except Exception:
            all_have_ts = False
            missing_ts.append(table)
    v.check("12. All gold tables have _gold_processed_at", all_have_ts,
            "all present" if all_have_ts else f"missing in: {missing_ts}")

    # Check 13: _delta_log exists
    lakehouse = get_lakehouse_path()
    all_have_log = True
    missing_log = []
    if lakehouse.startswith("s3a://"):
        import boto3
        s3 = boto3.client("s3")
        bucket = lakehouse.replace("s3a://", "").split("/")[0]
        for table in gold_tables:
            prefix = f"gold/{table}/_delta_log/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                all_have_log = False
                missing_log.append(table)
    else:
        for table in gold_tables:
            log_path = os.path.join(lakehouse, "gold", table, "_delta_log")
            if not os.path.isdir(log_path):
                all_have_log = False
                missing_log.append(table)
    v.check("13. All gold tables _delta_log exists", all_have_log,
            "all present" if all_have_log else f"missing: {missing_log}")

    all_passed = v.summary()
    spark.stop()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
