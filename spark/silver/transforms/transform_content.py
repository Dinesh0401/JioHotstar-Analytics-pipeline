"""Silver transform: content enriched — join content + metadata, normalize runtime/genre/cast."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_replace,
    split,
    trim,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_content_enriched(spark):
    """Read bronze/content + bronze/content_metadata, join and transform to silver/content_enriched."""
    print("  Transforming silver_content_enriched...")

    content = spark.read.format("delta").load(get_bronze_path("content"))
    metadata = spark.read.format("delta").load(get_bronze_path("content_metadata"))

    # LEFT JOIN on content_id — drop metadata audit columns before join
    metadata_clean = metadata.select(
        col("content_id").alias("meta_content_id"),
        "budget",
        "cast_members",
        "content_rating",
    )
    joined = content.join(metadata_clean, content["content_id"] == metadata_clean["meta_content_id"], "left") \
        .drop("meta_content_id")

    # Convert runtime to minutes: seconds -> divide by 60, minutes -> keep as-is
    joined = joined.withColumn(
        "runtime_minutes",
        when(col("runtime_unit") == "seconds", (col("runtime_value") / 60).cast("int"))
        .otherwise(col("runtime_value").cast("int")),
    )

    # Split genre on mixed separators: first replace all separators with comma, then split
    # Genre uses: "," or ", " or "|"
    joined = joined.withColumn(
        "genre_normalized", regexp_replace(col("genre"), r"\s*[,|]\s*", ",")
    )
    joined = joined.withColumn("genre_array", split(col("genre_normalized"), ","))

    # Split cast_members on "|"
    joined = joined.withColumn("cast_members", split(col("cast_members"), r"\|"))

    # Select final columns
    result = joined.select(
        "content_id",
        trim(col("title")).alias("title"),
        "content_type",
        "genre_array",
        "runtime_minutes",
        "release_year",
        "language",
        col("budget").cast("long").alias("budget"),
        "cast_members",
        "content_rating",
        content["created_at"],
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("content_enriched")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/content_enriched: {count} rows -> {output_path}")
    return count
