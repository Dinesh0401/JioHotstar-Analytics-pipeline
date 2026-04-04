"""Phase 1 orchestrator: starts infrastructure and runs all data generators."""

import logging
import os
import subprocess
import sys
import time
from datetime import datetime

import psycopg2
import pymysql


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import (
    KAFKA_TOPIC,
    LOGS_DIR,
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


os.makedirs(LOGS_DIR, exist_ok=True)
log_filename = f"phase1_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
log_path = os.path.join(LOGS_DIR, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("phase1")

DOCKER_COMPOSE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docker")


def step(num, total, description):
    """Log a step header."""
    log.info("")
    log.info("=" * 60)
    log.info(f"  STEP {num}/{total}: {description}")
    log.info("=" * 60)


def find_docker():
    """Locate the docker executable, checking common Windows paths."""
    import shutil

    path = shutil.which("docker")
    if path:
        return path
    # Common Docker Desktop install locations on Windows
    for candidate in [
        r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
        r"C:\Program Files\Docker\Docker\resources\docker.exe",
    ]:
        if os.path.isfile(candidate):
            return candidate
    return None


DOCKER_EXE = find_docker()


def check_docker():
    """Verify Docker is available and running."""
    if DOCKER_EXE is None:
        log.error("Docker not found. Please install Docker Desktop.")
        sys.exit(1)

    try:
        result = subprocess.run(
            [DOCKER_EXE, "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except FileNotFoundError:
        log.error("Docker not found. Please install Docker Desktop.")
        sys.exit(1)

    if result.returncode != 0:
        log.error("Docker is not running. Please start Docker Desktop.")
        sys.exit(1)

    log.info(f"Docker is running. ({DOCKER_EXE})")


def start_infrastructure():
    """Run docker compose up -d."""
    result = subprocess.run(
        [DOCKER_EXE, "compose", "up", "-d"],
        cwd=DOCKER_COMPOSE_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.error(f"docker compose failed:\n{result.stderr}")
        sys.exit(1)
    log.info("Docker containers started.")


def wait_for_mysql(timeout=60):
    """Wait until MySQL is ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
            )
            conn.close()
            log.info("MySQL is ready.")
            return
        except pymysql.err.OperationalError:
            time.sleep(2)

    log.error(f"MySQL did not become ready within {timeout}s")
    sys.exit(1)


def wait_for_postgres(timeout=60):
    """Wait until PostgreSQL is ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DATABASE,
            )
            conn.close()
            log.info("PostgreSQL is ready.")
            return
        except psycopg2.OperationalError:
            time.sleep(2)

    log.error(f"PostgreSQL did not become ready within {timeout}s")
    sys.exit(1)


def create_kafka_topic(timeout=90):
    """Create Kafka topic via docker exec, retrying until broker is ready."""
    start = time.time()
    while time.time() - start < timeout:
        result = subprocess.run(
            [
                DOCKER_EXE,
                "exec",
                "streaming_kafka",
                "kafka-topics",
                "--create",
                "--topic",
                KAFKA_TOPIC,
                "--bootstrap-server",
                "localhost:9092",
                "--partitions",
                "3",
                "--replication-factor",
                "1",
                "--if-not-exists",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode == 0 or "already exists" in (result.stderr or ""):
            log.info(f"Kafka topic '{KAFKA_TOPIC}' created (or already exists).")
            return

        # Broker not ready yet — retry
        log.info("Kafka broker not ready yet, retrying in 5s...")
        time.sleep(5)

    log.error(f"Could not create Kafka topic within {timeout}s")
    sys.exit(1)


def run_generator(module_name, description):
    """Run a generator module as a subprocess."""
    start = time.time()
    result = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info(f"  {line}")

    if result.returncode != 0:
        log.error(f"{description} FAILED ({elapsed:.1f}s):\n{result.stderr}")
        sys.exit(1)

    log.info(f"{description} completed in {elapsed:.1f}s")


def print_summary():
    """Print completion summary."""
    log.info("")
    log.info("=" * 60)
    log.info("  PHASE 1 COMPLETE")
    log.info("=" * 60)
    log.info("")
    log.info("Data sources generated:")
    log.info("  - MySQL:      streaming_users (users, subscriptions)")
    log.info("  - PostgreSQL: streaming_content (content_catalogue, ratings)")
    log.info("  - CSV:        data_sources/csv/content_metadata.csv")
    log.info("  - JSON:       data_sources/json/viewing_events_batch.json")
    log.info("  - Excel:      data_sources/excel/ad_campaigns.xlsx")
    log.info("  - Reviews:    data_sources/reviews/ (~500 files)")
    log.info("  - Thumbnails: data_sources/thumbnails/ (~2800 files)")
    log.info("  - Kafka:      topic 'media.viewing.live' created")
    log.info("")
    log.info("Next steps:")
    log.info("  1. Validate:  python validate_phase1.py")
    log.info("  2. Kafka batch:  python -m data_generation.kafka_producer --mode batch")
    log.info("  3. Kafka live:   python -m data_generation.kafka_producer --mode live")
    log.info("")
    log.info(f"Log saved to: {log_path}")


def main():
    """Run the full Phase-1 setup."""
    total_steps = 9
    log.info(f"Phase 1 Data Generation - Started at {datetime.now()}")

    step(1, total_steps, "Verify Docker is running")
    check_docker()

    step(2, total_steps, "Start infrastructure (docker compose up)")
    start_infrastructure()

    step(3, total_steps, "Wait for MySQL to be ready")
    wait_for_mysql()

    step(4, total_steps, "Wait for PostgreSQL to be ready")
    wait_for_postgres()

    step(5, total_steps, "Create Kafka topic")
    create_kafka_topic()

    step(6, total_steps, "Generate master entity IDs")
    run_generator("data_generation.master_ids", "Master IDs generation")

    step(7, total_steps, "Populate MySQL (users + subscriptions)")
    run_generator("data_generation.generate_mysql_data", "MySQL data generation")

    step(8, total_steps, "Populate PostgreSQL (content + ratings)")
    run_generator("data_generation.generate_postgres_data", "PostgreSQL data generation")

    step(9, total_steps, "Generate file sources (CSV, JSON, Excel, reviews, thumbnails)")
    from data_generation.generate_file_sources import generate_file_sources

    generate_file_sources()
    print_summary()


if __name__ == "__main__":
    main()
