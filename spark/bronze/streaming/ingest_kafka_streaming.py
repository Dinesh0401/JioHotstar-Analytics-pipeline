"""Bronze ingestion: Kafka streaming events via Spark Structured Streaming."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import LongType, StringType, StructField, StructType

from config.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from spark.conf.spark_settings import get_bronze_path, get_checkpoint_path, get_spark_session

VIEWING_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device_category", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("watch_duration_ms", LongType(), True),
    StructField("seek_position_ms", LongType(), True),
    StructField("event_ts", StringType(), True),
    StructField("session_start_ts", StringType(), True),
    StructField("referrer", StringType(), True),
])


def main():
    spark = get_spark_session("Bronze-Kafka-Streaming")

    print(f"Starting Kafka streaming from topic: {KAFKA_TOPIC}")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP}")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value", "partition", "offset") \
        .select(
            from_json(col("json_value"), VIEWING_EVENT_SCHEMA).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
        ) \
        .select("data.*", "_kafka_partition", "_kafka_offset") \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("kafka"))

    output_path = get_bronze_path("viewing_events")
    checkpoint_path = get_checkpoint_path("bronze_viewing_events")

    print(f"Writing to: {output_path}")
    print(f"Checkpoint: {checkpoint_path}")

    query = parsed_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="30 seconds") \
        .start(output_path)

    print("Streaming started. Press Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming...")
        query.stop()
        print("Streaming stopped.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
