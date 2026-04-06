"""Silver orchestrator: runs all Silver transforms with a shared SparkSession."""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_silver_path, get_spark_session


def main():
    print("=" * 60)
    print("  SILVER LAYER TRANSFORMS")
    print("=" * 60)

    start_time = time.time()
    spark = get_spark_session("Silver-Orchestrator")

    try:
        # Dimensions first, then facts
        print("\n[1/6] Users")
        print("-" * 40)
        from spark.silver.transforms.transform_users import transform_users
        user_count = transform_users(spark)

        print("\n[2/6] Subscriptions")
        print("-" * 40)
        from spark.silver.transforms.transform_subscriptions import transform_subscriptions
        sub_count = transform_subscriptions(spark)

        print("\n[3/6] Content Enriched")
        print("-" * 40)
        from spark.silver.transforms.transform_content import transform_content_enriched
        content_count = transform_content_enriched(spark)

        print("\n[4/6] Campaigns")
        print("-" * 40)
        from spark.silver.transforms.transform_campaigns import transform_campaigns
        campaign_count = transform_campaigns(spark)

        print("\n[5/6] Ratings")
        print("-" * 40)
        from spark.silver.transforms.transform_ratings import transform_ratings_clean
        rating_count = transform_ratings_clean(spark)

        print("\n[6/6] Viewing Events")
        print("-" * 40)
        from spark.silver.transforms.transform_events import transform_viewing_events
        event_count = transform_viewing_events(spark)

        elapsed = time.time() - start_time

        print("\n" + "=" * 60)
        print("  SILVER LAYER TRANSFORMS COMPLETE")
        print("=" * 60)
        print(f"\n  Time: {elapsed:.1f}s")
        print(f"\n  Silver tables written:")
        print(f"    users:            {user_count:>6} rows")
        print(f"    subscriptions:    {sub_count:>6} rows")
        print(f"    content_enriched: {content_count:>6} rows")
        print(f"    campaigns:        {campaign_count:>6} rows")
        print(f"    ratings_clean:    {rating_count:>6} rows")
        print(f"    viewing_events:   {event_count:>6} rows")
        print(f"\n  Silver path: {get_silver_path('')}")
        print(f"\n  Next: python -m spark.jobs.validate_silver")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
