"""Post-generation validation for Phase 1 data sources."""

import csv
import json
import os
import subprocess
import sys

import psycopg2
import pymysql


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import (
    DATA_SOURCES_DIR,
    KAFKA_TOPIC,
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


class ValidationResult:
    """Collect validation checks and print a summary."""

    def __init__(self):
        self.results = []

    def check(self, name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        self.results.append((name, status, detail))
        print(f"  [{'+' if condition else 'X'}] {name}: {detail}")

    def summary(self):
        passed = sum(1 for _, status, _ in self.results if status == "PASS")
        total = len(self.results)
        print(f"\n{'=' * 50}")
        print(f"  VALIDATION: {passed}/{total} checks passed")
        print(f"{'=' * 50}")
        if passed < total:
            print("\n  Failed checks:")
            for name, status, detail in self.results:
                if status == "FAIL":
                    print(f"    - {name}: {detail}")
        return passed == total


def validate():
    """Run all Phase-1 validation checks."""
    result = ValidationResult()

    print("\n--- MySQL ---")
    try:
        mysql_conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
        cursor = mysql_conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        result.check("MySQL users", user_count == 5000, f"{user_count} rows")

        cursor.execute("SELECT COUNT(*) FROM subscriptions")
        subscription_count = cursor.fetchone()[0]
        result.check("MySQL subscriptions", 6000 <= subscription_count <= 8000, f"{subscription_count} rows")

        cursor.execute(
            "SELECT country, COUNT(*) AS cnt FROM users GROUP BY country ORDER BY cnt DESC LIMIT 1"
        )
        top_country = cursor.fetchone()
        result.check(
            "MySQL country distribution",
            top_country[0] == "India",
            f"Top country: {top_country[0]} ({top_country[1]})",
        )
        mysql_conn.close()
    except Exception as exc:
        result.check("MySQL connection", False, str(exc))

    print("\n--- PostgreSQL ---")
    try:
        pg_conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DATABASE,
        )
        cursor = pg_conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM content_catalogue")
        content_count = cursor.fetchone()[0]
        result.check("PG content_catalogue", content_count == 2000, f"{content_count} rows")

        cursor.execute("SELECT COUNT(*) FROM ratings")
        rating_count = cursor.fetchone()[0]
        result.check("PG ratings", 15000 <= rating_count <= 20000, f"{rating_count} rows")

        cursor.execute(
            """
            SELECT COUNT(DISTINCT user_id) FROM ratings
            WHERE user_id LIKE 'USR-%%'
            """
        )
        valid_users = cursor.fetchone()[0]
        result.check("PG ratings user_id format", valid_users > 0, f"{valid_users} distinct users with valid IDs")

        cursor.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT user_id, content_id, COUNT(*) AS cnt
                FROM ratings GROUP BY user_id, content_id HAVING COUNT(*) > 1
            ) dupes
            """
        )
        duplicate_count = cursor.fetchone()[0]
        result.check("PG ratings has duplicates (intended)", duplicate_count > 0, f"{duplicate_count} duplicate pairs")
        pg_conn.close()
    except Exception as exc:
        result.check("PostgreSQL connection", False, str(exc))

    print("\n--- Files ---")
    csv_path = os.path.join(DATA_SOURCES_DIR, "csv", "content_metadata.csv")
    if os.path.exists(csv_path):
        with open(csv_path, encoding="utf-8") as file:
            row_count = sum(1 for _ in csv.reader(file)) - 1
        result.check("CSV content_metadata.csv", row_count == 2000, f"{row_count} rows")
    else:
        result.check("CSV content_metadata.csv", False, "File not found")

    json_path = os.path.join(DATA_SOURCES_DIR, "json", "viewing_events_batch.json")
    if os.path.exists(json_path):
        with open(json_path, encoding="utf-8") as file:
            events = json.load(file)
        result.check("JSON viewing_events_batch.json", len(events) == 50000, f"{len(events)} events")
        if events:
            required_keys = {
                "event_id",
                "user_id",
                "content_id",
                "session_id",
                "device_category",
                "event_type",
                "watch_duration_ms",
                "seek_position_ms",
                "event_ts",
                "session_start_ts",
                "referrer",
            }
            actual_keys = set(events[0].keys())
            result.check(
                "JSON schema complete",
                required_keys.issubset(actual_keys),
                "All fields present" if required_keys.issubset(actual_keys) else f"Missing: {required_keys - actual_keys}",
            )
    else:
        result.check("JSON viewing_events_batch.json", False, "File not found")

    excel_path = os.path.join(DATA_SOURCES_DIR, "excel", "ad_campaigns.xlsx")
    result.check(
        "Excel ad_campaigns.xlsx",
        os.path.exists(excel_path),
        "Exists" if os.path.exists(excel_path) else "File not found",
    )

    reviews_dir = os.path.join(DATA_SOURCES_DIR, "reviews")
    if os.path.isdir(reviews_dir):
        review_count = len([name for name in os.listdir(reviews_dir) if name.endswith(".txt")])
        result.check("TXT reviews", 450 <= review_count <= 550, f"{review_count} files")
    else:
        result.check("TXT reviews", False, "Directory not found")

    thumbnails_dir = os.path.join(DATA_SOURCES_DIR, "thumbnails")
    if os.path.isdir(thumbnails_dir):
        thumbnail_count = len([name for name in os.listdir(thumbnails_dir) if name.endswith(".jpg")])
        result.check("Image thumbnails", 2500 <= thumbnail_count <= 3100, f"{thumbnail_count} files")
    else:
        result.check("Image thumbnails", False, "Directory not found")

    print("\n--- Kafka ---")
    try:
        kafka_topics = subprocess.run(
            [
                "docker",
                "exec",
                "streaming_kafka",
                "kafka-topics",
                "--list",
                "--bootstrap-server",
                "localhost:9092",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        topics = kafka_topics.stdout.strip().splitlines()
        result.check(
            "Kafka topic exists",
            KAFKA_TOPIC in topics,
            f"'{KAFKA_TOPIC}' {'found' if KAFKA_TOPIC in topics else 'not found'}",
        )
    except Exception as exc:
        result.check("Kafka topic check", False, str(exc))

    sys.exit(0 if result.summary() else 1)


if __name__ == "__main__":
    validate()
