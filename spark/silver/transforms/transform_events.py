"""Silver transform: viewing events — union batch+streaming, dedup, cast timestamps."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lower,
    row_number,
    to_date,
    to_timestamp,
)
from pyspark.sql.window import Window

from spark.conf.spark_settings import get_bronze_path, get_silver_path

# Common columns shared between batch and streaming sources
_COMMON_COLS = [
    "event_id", "user_id", "content_id", "session_id",
    "device_category", "event_type", "watch_duration_ms",
    "seek_position_ms", "event_ts", "session_start_ts", "referrer",
]


def transform_viewing_events(spark):
    """Read bronze viewing events (batch + streaming), union, dedup, transform."""
    print("  Transforming silver_viewing_events...")

    # Read batch source (always exists)
    batch = spark.read.format("delta").load(get_bronze_path("viewing_events_batch"))
    batch = batch.select(*_COMMON_COLS)

    # Try to read streaming source (may not exist if Kafka wasn't run)
    try:
        streaming = spark.read.format("delta").load(get_bronze_path("viewing_events"))
        streaming = streaming.select(*_COMMON_COLS)
        combined = batch.unionByName(streaming)
        print("    Merged batch + streaming sources")
    except Exception:
        combined = batch
        print("    Using batch source only (streaming table not found)")

    # Dedup by event_id: keep first occurrence (arbitrary — events are unique by design,
    # but streaming may re-ingest some batch events)
    window = Window.partitionBy("event_id").orderBy("event_ts")
    combined = combined.withColumn("_rn", row_number().over(window))
    combined = combined.filter(col("_rn") == 1).drop("_rn")

    # Cast timestamps from string to timestamp
    combined = combined.withColumn("event_ts", to_timestamp(col("event_ts")))
    combined = combined.withColumn("session_start_ts", to_timestamp(col("session_start_ts")))

    # Compute derived columns
    combined = combined.withColumn("watch_duration_sec", (col("watch_duration_ms") / 1000).cast("double"))
    combined = combined.withColumn("event_date", to_date(col("event_ts")))
    combined = combined.withColumn("event_type", lower(col("event_type")))

    # Select final columns
    result = combined.select(
        "event_id",
        "user_id",
        "content_id",
        "session_id",
        "device_category",
        "event_type",
        "watch_duration_ms",
        "watch_duration_sec",
        "seek_position_ms",
        "event_ts",
        "session_start_ts",
        "event_date",
        "referrer",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("viewing_events")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/viewing_events: {count} rows -> {output_path}")
    return count
