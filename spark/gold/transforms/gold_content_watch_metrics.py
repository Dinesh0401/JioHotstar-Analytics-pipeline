"""Gold transform: content watch metrics — popularity and watch time per content."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    avg,
    count,
    countDistinct,
    current_timestamp,
    sum as spark_sum,
)

from spark.conf.spark_settings import get_gold_path, get_silver_path


def gold_content_watch_metrics(spark):
    """Join viewing events with content, aggregate by content_id."""
    print("  Computing gold_content_watch_metrics...")

    events = spark.read.format("delta").load(get_silver_path("viewing_events"))
    content = spark.read.format("delta").load(get_silver_path("content_enriched"))

    joined = events.join(content, "content_id", "inner")

    result = joined.groupBy("content_id", "title", "content_type").agg(
        count("*").alias("total_views"),
        countDistinct("user_id").alias("unique_viewers"),
        spark_sum("watch_duration_sec").alias("total_watch_time_sec"),
        avg("watch_duration_sec").alias("avg_watch_time_sec"),
    ).withColumn("_gold_processed_at", current_timestamp())

    output_path = get_gold_path("content_watch_metrics")
    result.write.format("delta").mode("overwrite").save(output_path)
    row_count = result.count()
    print(f"    gold/content_watch_metrics: {row_count} rows -> {output_path}")
    return row_count
