"""Silver transform: subscriptions — compute is_active, duration_days."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    coalesce,
    col,
    current_date,
    current_timestamp,
    datediff,
    lit,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_subscriptions(spark):
    """Read bronze/subscriptions, apply Silver transformations, write to silver/subscriptions."""
    print("  Transforming silver_subscriptions...")

    df = spark.read.format("delta").load(get_bronze_path("subscriptions"))

    # Compute is_active: end_date is null OR end_date >= today
    df = df.withColumn(
        "is_active",
        when(col("end_date").isNull(), True)
        .when(col("end_date") >= current_date(), True)
        .otherwise(False),
    )

    # Compute duration_days: datediff(coalesce(end_date, today), start_date)
    df = df.withColumn(
        "duration_days",
        datediff(coalesce(col("end_date"), current_date()), col("start_date")).cast("int"),
    )

    # Fill null cancel_reason with "active"
    df = df.withColumn(
        "cancel_reason",
        coalesce(col("cancel_reason"), lit("active")),
    )

    # Select final columns, dropping bronze audit columns
    result = df.select(
        "subscription_id",
        "user_id",
        "plan_id",
        "start_date",
        "end_date",
        "cancel_reason",
        "is_active",
        "duration_days",
        "created_at",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("subscriptions")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/subscriptions: {count} rows -> {output_path}")
    return count
