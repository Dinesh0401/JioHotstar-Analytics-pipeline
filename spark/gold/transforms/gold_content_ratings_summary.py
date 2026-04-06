"""Gold transform: content ratings summary — rating analytics per content."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    avg,
    count,
    current_timestamp,
    max as spark_max,
    min as spark_min,
)

from spark.conf.spark_settings import get_gold_path, get_silver_path


def gold_content_ratings_summary(spark):
    """Join ratings with content, aggregate by content_id for rating analytics."""
    print("  Computing gold_content_ratings_summary...")

    ratings = spark.read.format("delta").load(get_silver_path("ratings_clean"))
    content = spark.read.format("delta").load(get_silver_path("content_enriched"))

    joined = ratings.join(content, "content_id", "inner")

    result = joined.groupBy("content_id", "title", "content_type").agg(
        avg("rating_value").alias("avg_rating"),
        count("*").alias("rating_count"),
        spark_min("rating_value").cast("double").alias("min_rating"),
        spark_max("rating_value").cast("double").alias("max_rating"),
    ).withColumn("_gold_processed_at", current_timestamp())

    output_path = get_gold_path("content_ratings_summary")
    result.write.format("delta").mode("overwrite").save(output_path)
    row_count = result.count()
    print(f"    gold/content_ratings_summary: {row_count} rows -> {output_path}")
    return row_count
