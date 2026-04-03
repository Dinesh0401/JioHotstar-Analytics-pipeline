"""Bronze ingestion: CSV, JSON, and Excel file sources."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import pandas as pd
from pyspark.sql.functions import current_timestamp, lit

from config.settings import DATA_SOURCES_DIR
from spark.conf.spark_settings import get_bronze_path, get_spark_session


def ingest_csv(spark):
    print("Ingesting CSV content_metadata...")
    csv_path = os.path.join(DATA_SOURCES_DIR, "csv", "content_metadata.csv")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("csv"))
    output_path = get_bronze_path("content_metadata")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/content_metadata: {count} rows written to {output_path}")
    return count


def ingest_json(spark):
    print("Ingesting JSON viewing_events_batch...")
    json_path = os.path.join(DATA_SOURCES_DIR, "json", "viewing_events_batch.json")
    df = spark.read.option("multiLine", "true").json(json_path)
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("json"))
    output_path = get_bronze_path("viewing_events_batch")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/viewing_events_batch: {count} rows written to {output_path}")
    return count


def ingest_excel(spark):
    print("Ingesting Excel ad_campaigns...")
    excel_path = os.path.join(DATA_SOURCES_DIR, "excel", "ad_campaigns.xlsx")
    pdf = pd.read_excel(excel_path)
    df = spark.createDataFrame(pdf)
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source", lit("excel"))
    output_path = get_bronze_path("campaigns")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/campaigns: {count} rows written to {output_path}")
    return count


def main():
    spark = get_spark_session("Bronze-Files")
    try:
        ingest_csv(spark)
        ingest_json(spark)
        ingest_excel(spark)
        print("File sources Bronze ingestion complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
