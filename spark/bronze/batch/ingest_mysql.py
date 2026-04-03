"""Bronze ingestion: MySQL users and subscriptions via JDBC."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import current_timestamp, lit

from spark.conf.spark_settings import (
    get_bronze_path,
    get_mysql_jdbc_props,
    get_mysql_jdbc_url,
    get_spark_session,
)


def ingest_users(spark):
    print("Ingesting MySQL users...")
    df = spark.read.jdbc(url=get_mysql_jdbc_url(), table="users", properties=get_mysql_jdbc_props())
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("mysql"))
    output_path = get_bronze_path("users")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/users: {count} rows written to {output_path}")
    return count


def ingest_subscriptions(spark):
    print("Ingesting MySQL subscriptions...")
    df = spark.read.jdbc(url=get_mysql_jdbc_url(), table="subscriptions", properties=get_mysql_jdbc_props())
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("mysql"))
    output_path = get_bronze_path("subscriptions")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/subscriptions: {count} rows written to {output_path}")
    return count


def main():
    spark = get_spark_session("Bronze-MySQL")
    try:
        ingest_users(spark)
        ingest_subscriptions(spark)
        print("MySQL Bronze ingestion complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
