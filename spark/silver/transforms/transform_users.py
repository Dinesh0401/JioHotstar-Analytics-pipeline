"""Silver transform: users — clean, extract device_os, validate email."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    upper,
    when,
)

from spark.conf.spark_settings import get_bronze_path, get_silver_path


def transform_users(spark):
    """Read bronze/users, apply Silver transformations, write to silver/users."""
    print("  Transforming silver_users...")

    df = spark.read.format("delta").load(get_bronze_path("users"))

    # Extract device_os from user_agent string
    df = df.withColumn(
        "device_os",
        when(col("user_agent").rlike("(?i)iphone|ios"), "iOS")
        .when(col("user_agent").rlike("(?i)android"), "Android")
        .when(col("user_agent").rlike("(?i)windows"), "Windows")
        .when(col("user_agent").rlike("(?i)macintosh|mac os"), "macOS")
        .when(col("user_agent").rlike("(?i)linux|tizen"), "Linux")
        .otherwise("Other"),
    )

    # Normalize country to uppercase
    df = df.withColumn("country", upper(col("country")))

    # Validate email with basic regex
    df = df.withColumn(
        "is_valid_email",
        col("email").rlike("^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"),
    )

    # Select final columns, dropping user_agent and bronze audit columns
    result = df.select(
        "user_id",
        "email",
        "is_valid_email",
        "full_name",
        "signup_date",
        "device_category",
        "device_os",
        "country",
        "created_at",
        current_timestamp().alias("_silver_processed_at"),
    )

    output_path = get_silver_path("users")
    result.write.format("delta").mode("overwrite").save(output_path)
    count = result.count()
    print(f"    silver/users: {count} rows -> {output_path}")
    return count
