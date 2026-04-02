"""Shared configuration for all data generation scripts."""
import os

# Project root (two levels up from config/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── MySQL ──
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "streaming_pass"
MYSQL_DATABASE = "streaming_users"

# ── PostgreSQL ──
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "streaming_pass"
POSTGRES_DATABASE = "streaming_content"

# ── Kafka ──
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "media.viewing.live"

# ── Paths (absolute, derived from PROJECT_ROOT) ──
DATA_SOURCES_DIR = os.path.join(PROJECT_ROOT, "data_sources")
MASTER_IDS_PATH = os.path.join(PROJECT_ROOT, "data_generation", "master_ids.json")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")

# ── Entity Counts ──
NUM_USERS = 5000
NUM_CONTENT = 2000
NUM_CAMPAIGNS = 50
NUM_VIEWING_EVENTS = 50000
NUM_REVIEWS = 500
NUM_RATINGS_PER_CONTENT_AVG = 8  # ~16,000 total ratings

# ── Deterministic Seed ──
SEED = 42

# ── SQLAlchemy Connection URLs ──
MYSQL_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
POSTGRES_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
