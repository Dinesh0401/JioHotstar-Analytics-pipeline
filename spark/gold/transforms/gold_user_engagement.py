"""Gold transform: user engagement — watch behavior metrics per user."""

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


def gold_user_engagement(spark):
    """Aggregate silver_viewing_events by user_id for engagement metrics."""
    print("  Computing gold_user_engagement...")

    events = spark.read.format("delta").load(get_silver_path("viewing_events"))

    result = events.groupBy("user_id").agg(
        countDistinct("session_id").alias("total_sessions"),
        count("*").alias("total_views"),
        spark_sum("watch_duration_sec").alias("total_watch_time_sec"),
        countDistinct("content_id").alias("distinct_content_watched"),
    )

    # Compute avg_session_watch_sec = total_watch_time / total_sessions
    result = result.withColumn(
        "avg_session_watch_sec",
        col("total_watch_time_sec") / col("total_sessions"),
    ).withColumn("_gold_processed_at", current_timestamp())

    # Reorder columns
    result = result.select(
        "user_id", "total_sessions", "total_views", "total_watch_time_sec",
        "avg_session_watch_sec", "distinct_content_watched", "_gold_processed_at",
    )

    output_path = get_gold_path("user_engagement")
    result.write.format("delta").mode("overwrite").save(output_path)
    row_count = result.count()
    print(f"    gold/user_engagement: {row_count} rows -> {output_path}")
    return row_count
