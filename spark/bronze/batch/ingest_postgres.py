"""Bronze ingestion: PostgreSQL content_catalogue and ratings via JDBC."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import current_timestamp, lit

from spark.conf.spark_settings import (
    get_bronze_path,
    get_postgres_jdbc_props,
    get_postgres_jdbc_url,
    get_spark_session,
)


def ingest_content(spark):
    print("Ingesting PostgreSQL content_catalogue...")
    df = spark.read.jdbc(url=get_postgres_jdbc_url(), table="content_catalogue", properties=get_postgres_jdbc_props())
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("postgres"))
    output_path = get_bronze_path("content")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/content: {count} rows written to {output_path}")
    return count


def ingest_ratings(spark):
    print("Ingesting PostgreSQL ratings...")
    df = spark.read.jdbc(url=get_postgres_jdbc_url(), table="ratings", properties=get_postgres_jdbc_props())
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("postgres"))
    output_path = get_bronze_path("ratings")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/ratings: {count} rows written to {output_path}")
    return count


def main():
    spark = get_spark_session("Bronze-PostgreSQL")
    try:
        ingest_content(spark)
        ingest_ratings(spark)
        print("PostgreSQL Bronze ingestion complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
