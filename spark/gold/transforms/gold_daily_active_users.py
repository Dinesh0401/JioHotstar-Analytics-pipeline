"""Gold transform: daily active users — platform usage metrics per day."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    sum as spark_sum,
)

from spark.conf.spark_settings import get_gold_path, get_silver_path


def gold_daily_active_users(spark):
    """Aggregate silver_viewing_events by event_date for daily platform metrics."""
    print("  Computing gold_daily_active_users...")

    events = spark.read.format("delta").load(get_silver_path("viewing_events"))

    result = events.groupBy("event_date").agg(
        countDistinct("user_id").alias("daily_active_users"),
        count("*").alias("total_views"),
        spark_sum("watch_duration_sec").alias("total_watch_time_sec"),
    ).withColumn("_gold_processed_at", current_timestamp())

    output_path = get_gold_path("daily_active_users")
    result.write.format("delta").mode("overwrite").save(output_path)
    row_count = result.count()
    print(f"    gold/daily_active_users: {row_count} rows -> {output_path}")
    return row_count
