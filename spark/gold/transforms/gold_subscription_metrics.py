"""Gold transform: subscription metrics — business KPIs per plan."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    first,
    sum as spark_sum,
    when,
)
from pyspark.sql.window import Window

from spark.conf.spark_settings import get_gold_path, get_silver_path


def gold_subscription_metrics(spark):
    """Aggregate silver_subscriptions by plan_id for business KPIs."""
    print("  Computing gold_subscription_metrics...")

    subs = spark.read.format("delta").load(get_silver_path("subscriptions"))

    # Main aggregation by plan_id
    metrics = subs.groupBy("plan_id").agg(
        count("*").alias("total_subscriptions"),
        spark_sum(when(col("is_active"), 1).otherwise(0)).cast("long").alias("active_subscriptions"),
        spark_sum(when(~col("is_active"), 1).otherwise(0)).cast("long").alias("cancelled_subscriptions"),
        avg("duration_days").alias("avg_duration_days"),
    )

    # Find top cancel reason per plan: filter cancelled, group by plan+reason, count, rank
    cancelled = subs.filter(~col("is_active")).filter(col("cancel_reason") != "active")
    reason_counts = cancelled.groupBy("plan_id", "cancel_reason").agg(
        count("*").alias("reason_count")
    )
    window = Window.partitionBy("plan_id").orderBy(col("reason_count").desc())
    from pyspark.sql.functions import row_number
    top_reasons = reason_counts.withColumn("_rn", row_number().over(window)) \
        .filter(col("_rn") == 1) \
        .select(col("plan_id").alias("_plan_id"), col("cancel_reason").alias("top_cancel_reason"))

    # Join top reason back to metrics
    result = metrics.join(top_reasons, metrics["plan_id"] == top_reasons["_plan_id"], "left") \
        .drop("_plan_id") \
        .withColumn("_gold_processed_at", current_timestamp())

    result = result.select(
        "plan_id", "total_subscriptions", "active_subscriptions",
        "cancelled_subscriptions", "avg_duration_days", "top_cancel_reason",
        "_gold_processed_at",
    )

    output_path = get_gold_path("subscription_metrics")
    result.write.format("delta").mode("overwrite").save(output_path)
    row_count = result.count()
    print(f"    gold/subscription_metrics: {row_count} rows -> {output_path}")
    return row_count
