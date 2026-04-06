"""Silver transform: campaigns — parse budget string, compute duration."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    datediff,
    regexp_extract,
    to_date,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_campaigns(spark):
    """Read bronze/campaigns, parse budget, compute duration, write to silver/campaigns."""
    print("  Transforming silver_campaigns...")

    df = spark.read.format("delta").load(get_bronze_path("campaigns"))

    # Cast budget to string for uniform processing (some are int, some are "USD 12345")
    df = df.withColumn("budget_str", col("budget").cast("string"))

    # Extract currency: "USD" if string starts with letters, else "INR" (default for plain numbers)
    df = df.withColumn(
        "budget_currency",
        when(
            col("budget_str").rlike("^[A-Za-z]"),
            regexp_extract(col("budget_str"), r"^([A-Za-z]+)", 1),
        ).otherwise("INR"),
    )

    # Extract amount: digits after space if string format, otherwise the whole value
    df = df.withColumn(
        "budget_amount",
        when(
            col("budget_str").rlike("^[A-Za-z]"),
            regexp_extract(col("budget_str"), r"(\d+)$", 1).cast("long"),
        ).otherwise(col("budget_str").cast("long")),
    )

    # Cast dates and compute duration
    df = df.withColumn("start_date", to_date(col("start_date")))
    df = df.withColumn("end_date", to_date(col("end_date")))
    df = df.withColumn(
        "campaign_duration_days",
        datediff(col("end_date"), col("start_date")).cast("int"),
    )

    # Select final columns
    result = df.select(
        "campaign_id",
        "campaign_name",
        "budget_amount",
        "budget_currency",
        "start_date",
        "end_date",
        "campaign_duration_days",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("campaigns")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/campaigns: {count} rows -> {output_path}")
    return count
