"""Spark session factory with environment-aware configuration."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.settings import (
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP,
    LAKEHOUSE_PATH,
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

EXTRA_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.2",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "com.mysql:mysql-connector-j:8.3.0",
    "org.postgresql:postgresql:42.7.3",
]


def get_spark_session(app_name="JioHotstar-Bronze"):
    # On Windows, Python is 'python' not 'python3'
    import shutil
    if not shutil.which("python3") and shutil.which("python"):
        os.environ.setdefault("PYSPARK_PYTHON", "python")
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    if LAKEHOUSE_PATH.startswith("s3://"):
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    spark = configure_spark_with_delta_pip(builder, extra_packages=EXTRA_PACKAGES).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_lakehouse_path():
    path = LAKEHOUSE_PATH
    if path.startswith("s3://"):
        return path.replace("s3://", "s3a://")
    return os.path.abspath(path)


def get_bronze_path(table_name):
    base = get_lakehouse_path()
    return f"{base}/bronze/{table_name}"


def get_checkpoint_path(name):
    base = get_lakehouse_path()
    return f"{base}/checkpoints/{name}"


def get_mysql_jdbc_url():
    return f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"


def get_mysql_jdbc_props():
    return {"user": MYSQL_USER, "password": MYSQL_PASSWORD, "driver": "com.mysql.cj.jdbc.Driver"}


def get_postgres_jdbc_url():
    return f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"


def get_postgres_jdbc_props():
    return {"user": POSTGRES_USER, "password": POSTGRES_PASSWORD, "driver": "org.postgresql.Driver"}
