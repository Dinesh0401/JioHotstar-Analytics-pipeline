"""Gold orchestrator: runs all Gold transforms with a shared SparkSession."""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_gold_path, get_spark_session


def main():
    print("=" * 60)
    print("  GOLD LAYER ANALYTICS")
    print("=" * 60)

    start_time = time.time()
    spark = get_spark_session("Gold-Orchestrator")

    try:
        print("\n[1/6] Daily Active Users")
        print("-" * 40)
        from spark.gold.transforms.gold_daily_active_users import gold_daily_active_users
        dau_count = gold_daily_active_users(spark)

        print("\n[2/6] Content Watch Metrics")
        print("-" * 40)
        from spark.gold.transforms.gold_content_watch_metrics import gold_content_watch_metrics
        cwm_count = gold_content_watch_metrics(spark)

        print("\n[3/6] Genre Popularity")
        print("-" * 40)
        from spark.gold.transforms.gold_genre_popularity import gold_genre_popularity
        genre_count = gold_genre_popularity(spark)

        print("\n[4/6] User Engagement")
        print("-" * 40)
        from spark.gold.transforms.gold_user_engagement import gold_user_engagement
        engage_count = gold_user_engagement(spark)

        print("\n[5/6] Subscription Metrics")
        print("-" * 40)
        from spark.gold.transforms.gold_subscription_metrics import gold_subscription_metrics
        sub_count = gold_subscription_metrics(spark)

        print("\n[6/6] Content Ratings Summary")
        print("-" * 40)
        from spark.gold.transforms.gold_content_ratings_summary import gold_content_ratings_summary
        rating_count = gold_content_ratings_summary(spark)

        elapsed = time.time() - start_time

        print("\n" + "=" * 60)
        print("  GOLD LAYER ANALYTICS COMPLETE")
        print("=" * 60)
        print(f"\n  Time: {elapsed:.1f}s")
        print(f"\n  Gold tables written:")
        print(f"    daily_active_users:       {dau_count:>6} rows")
        print(f"    content_watch_metrics:    {cwm_count:>6} rows")
        print(f"    genre_popularity:         {genre_count:>6} rows")
        print(f"    user_engagement:          {engage_count:>6} rows")
        print(f"    subscription_metrics:     {sub_count:>6} rows")
        print(f"    content_ratings_summary:  {rating_count:>6} rows")
        print(f"\n  Gold path: {get_gold_path('')}")
        print(f"\n  Next: python -m spark.jobs.validate_gold")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
