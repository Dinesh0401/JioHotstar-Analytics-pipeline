"""Gold transform: genre popularity — views per genre using explode."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    explode,
    sum as spark_sum,
)

from spark.conf.spark_settings import get_gold_path, get_silver_path


def gold_genre_popularity(spark):
    """Join events with content, explode genre_array, aggregate by genre."""
    print("  Computing gold_genre_popularity...")

    events = spark.read.format("delta").load(get_silver_path("viewing_events"))
    content = spark.read.format("delta").load(get_silver_path("content_enriched"))

    joined = events.join(content, "content_id", "inner")

    # Explode genre_array so each genre gets full credit per view
    exploded = joined.withColumn("genre", explode(col("genre_array")))

    result = exploded.groupBy("genre").agg(
        count("*").alias("total_views"),
        countDistinct("user_id").alias("unique_viewers"),
        spark_sum("watch_duration_sec").alias("total_watch_time_sec"),
    ).withColumn("_gold_processed_at", current_timestamp())

    output_path = get_gold_path("genre_popularity")
    result.write.format("delta").mode("overwrite").save(output_path)
    row_count = result.count()
    print(f"    gold/genre_popularity: {row_count} rows -> {output_path}")
    return row_count
