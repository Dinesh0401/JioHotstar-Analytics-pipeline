"""Silver transform: ratings — dedup by user+content, validate rating range."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_ratings_clean(spark):
    """Read bronze/ratings, dedup and validate, write to silver/ratings_clean."""
    print("  Transforming silver_ratings_clean...")

    df = spark.read.format("delta").load(get_bronze_path("ratings"))

    # Filter rating_value to 0.0-5.0 range
    df = df.filter((col("rating_value") >= 0.0) & (col("rating_value") <= 5.0))

    # Dedup: keep latest rating per (user_id, content_id)
    window = Window.partitionBy("user_id", "content_id").orderBy(col("rated_at").desc())
    df = df.withColumn("_rn", row_number().over(window))
    df = df.filter(col("_rn") == 1).drop("_rn")

    # Select final columns, dropping bronze audit columns
    result = df.select(
        "rating_id",
        "user_id",
        "content_id",
        col("rating_value").cast("decimal(2,1)").alias("rating_value"),
        "rating_source",
        "rated_at",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("ratings_clean")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/ratings_clean: {count} rows -> {output_path}")
    return count
