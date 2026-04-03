"""Bronze batch orchestrator: runs all batch ingestion pipelines."""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_bronze_path, get_spark_session


def main():
    print("=" * 60)
    print("  BRONZE BATCH INGESTION")
    print("=" * 60)

    start_time = time.time()
    spark = get_spark_session("Bronze-Orchestrator")

    try:
        print("\n[1/3] MySQL ingestion (users + subscriptions)")
        print("-" * 40)
        from spark.bronze.batch.ingest_mysql import ingest_subscriptions, ingest_users
        user_count = ingest_users(spark)
        sub_count = ingest_subscriptions(spark)

        print("\n[2/3] PostgreSQL ingestion (content + ratings)")
        print("-" * 40)
        from spark.bronze.batch.ingest_postgres import ingest_content, ingest_ratings
        content_count = ingest_content(spark)
        rating_count = ingest_ratings(spark)

        print("\n[3/3] File sources ingestion (CSV + JSON + Excel)")
        print("-" * 40)
        from spark.bronze.batch.ingest_files import ingest_csv, ingest_excel, ingest_json
        csv_count = ingest_csv(spark)
        json_count = ingest_json(spark)
        excel_count = ingest_excel(spark)

        elapsed = time.time() - start_time

        print("\n" + "=" * 60)
        print("  BRONZE BATCH INGESTION COMPLETE")
        print("=" * 60)
        print(f"\n  Time: {elapsed:.1f}s")
        print(f"\n  Bronze tables written:")
        print(f"    users:                {user_count:>6} rows")
        print(f"    subscriptions:        {sub_count:>6} rows")
        print(f"    content:              {content_count:>6} rows")
        print(f"    ratings:              {rating_count:>6} rows")
        print(f"    content_metadata:     {csv_count:>6} rows")
        print(f"    viewing_events_batch: {json_count:>6} rows")
        print(f"    campaigns:            {excel_count:>6} rows")
        print(f"\n  Lakehouse path: {get_bronze_path('')}")
        print(f"\n  Next: python -m spark.jobs.validate_bronze")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
