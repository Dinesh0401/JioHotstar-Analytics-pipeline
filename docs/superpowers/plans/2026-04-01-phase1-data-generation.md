# Phase 1: Data Generation & Source System Setup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a complete synthetic data generation system that populates MySQL, PostgreSQL, Kafka, and flat files with realistic, joinable datasets simulating a JioHotstar-style Indian OTT streaming platform.

**Architecture:** Modular Python scripts sharing entity IDs via `master_ids.json`, orchestrated by `run_phase1.py`. All infrastructure runs in Docker containers. Each generator is independently runnable. Deterministic seeds ensure reproducible output.

**Tech Stack:** Python 3.11+, SQLAlchemy, Faker, kafka-python, Pillow, openpyxl, Docker Compose (MySQL 8.0, PostgreSQL 15, Confluent Kafka 7.5.0)

**Spec:** `docs/superpowers/specs/2026-04-01-phase1-data-generation-design.md`

---

## File Map

| File | Responsibility |
|------|---------------|
| `requirements.txt` | Python dependencies |
| `config/__init__.py` | Package marker |
| `config/settings.py` | All shared configuration (DB hosts, ports, passwords, paths, counts) |
| `data_generation/__init__.py` | Package marker |
| `data_generation/master_ids.py` | Generate and load shared entity IDs |
| `data_generation/generate_mysql_data.py` | Populate MySQL `users` + `subscriptions` tables |
| `data_generation/generate_postgres_data.py` | Populate PostgreSQL `content_catalogue` + `ratings` tables |
| `data_generation/generate_file_sources.py` | Generate CSV, JSON, Excel, TXT reviews, image thumbnails |
| `data_generation/kafka_producer.py` | Kafka streaming producer with batch/live IPL match simulation |
| `docker/docker-compose.yml` | MySQL, PostgreSQL, Zookeeper, Kafka containers |
| `run_phase1.py` | Orchestrator — checks Docker, starts infra, runs generators in order |
| `validate_phase1.py` | Post-generation validation — row counts, joins, file checks |

---

### Task 1: Project Scaffolding & Dependencies

**Files:**
- Create: `requirements.txt`
- Create: `config/__init__.py`
- Create: `config/settings.py`
- Create: `data_generation/__init__.py`
- Create directories: `data_sources/csv/`, `data_sources/json/`, `data_sources/excel/`, `data_sources/reviews/`, `data_sources/thumbnails/`, `docker/`, `logs/`

- [ ] **Step 1: Create directory structure**

```bash
cd /c/Users/sjdin/media_stream_analytics
mkdir -p config data_generation data_sources/csv data_sources/json data_sources/excel data_sources/reviews data_sources/thumbnails docker logs
```

- [ ] **Step 2: Create `requirements.txt`**

```
faker==28.0.0
sqlalchemy==2.0.30
pymysql==1.1.0
psycopg2-binary==2.9.9
kafka-python==2.0.2
openpyxl==3.1.2
Pillow==10.3.0
numpy==1.26.4
```

Note: `numpy` added for realistic statistical distributions (normal, beta for ratings, etc.).

- [ ] **Step 3: Create `config/__init__.py`**

Empty file — package marker.

- [ ] **Step 4: Create `config/settings.py`**

```python
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
```

- [ ] **Step 5: Create `data_generation/__init__.py`**

Empty file — package marker.

- [ ] **Step 6: Install dependencies**

```bash
cd /c/Users/sjdin/media_stream_analytics
pip install -r requirements.txt
```

- [ ] **Step 7: Verify imports work**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -c "from config.settings import MYSQL_URL, POSTGRES_URL, SEED; print('Settings OK:', MYSQL_URL[:30])"
```

Expected: `Settings OK: mysql+pymysql://root:stream`

- [ ] **Step 8: Commit**

```bash
git init
git add requirements.txt config/ data_generation/__init__.py
git commit -m "feat: project scaffolding with config and dependencies"
```

---

### Task 2: Docker Compose Infrastructure

**Files:**
- Create: `docker/docker-compose.yml`

- [ ] **Step 1: Create `docker/docker-compose.yml`**

```yaml
version: "3.9"

services:
  mysql:
    image: mysql:8.0
    container_name: streaming_mysql
    environment:
      MYSQL_ROOT_PASSWORD: streaming_pass
      MYSQL_DATABASE: streaming_users
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-pstreaming_pass"]
      interval: 5s
      timeout: 5s
      retries: 20

  postgres:
    image: postgres:15
    container_name: streaming_postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: streaming_pass
      POSTGRES_DB: streaming_content
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 20

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: streaming_zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: streaming_kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    volumes:
      - kafka_data:/var/lib/kafka/data

volumes:
  mysql_data:
  postgres_data:
  kafka_data:
```

- [ ] **Step 2: Verify Docker Compose is valid**

```bash
cd /c/Users/sjdin/media_stream_analytics/docker
docker compose config --quiet && echo "Docker Compose config valid"
```

Expected: `Docker Compose config valid`

- [ ] **Step 3: Start infrastructure**

```bash
cd /c/Users/sjdin/media_stream_analytics/docker
docker compose up -d
```

- [ ] **Step 4: Verify all containers are running**

```bash
docker compose -f /c/Users/sjdin/media_stream_analytics/docker/docker-compose.yml ps
```

Expected: 4 containers (streaming_mysql, streaming_postgres, streaming_zookeeper, streaming_kafka) all "running" or "healthy".

- [ ] **Step 5: Verify MySQL is reachable**

```bash
docker exec streaming_mysql mysql -u root -pstreaming_pass -e "SELECT 1 AS test;"
```

Expected: Returns `test: 1`

- [ ] **Step 6: Verify PostgreSQL is reachable**

```bash
docker exec streaming_postgres psql -U postgres -d streaming_content -c "SELECT 1 AS test;"
```

Expected: Returns `test: 1`

- [ ] **Step 7: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add docker/
git commit -m "feat: Docker Compose with MySQL, PostgreSQL, Kafka, Zookeeper"
```

---

### Task 3: Master IDs Generator

**Files:**
- Create: `data_generation/master_ids.py`

- [ ] **Step 1: Write `data_generation/master_ids.py`**

```python
"""Generate and load shared entity IDs used across all data sources."""
import json
import os
import random
import sys

# Allow running both as module and standalone script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import NUM_USERS, NUM_CONTENT, NUM_CAMPAIGNS, MASTER_IDS_PATH, SEED


def generate_master_ids():
    """Generate deterministic master entity IDs and save to JSON."""
    random.seed(SEED)

    user_ids = [f"USR-{1000001 + i}" for i in range(NUM_USERS)]
    content_ids = [f"CNT-{i + 1:06d}" for i in range(NUM_CONTENT)]
    campaign_ids = [f"CAM-{i + 1:05d}" for i in range(NUM_CAMPAIGNS)]

    master_ids = {
        "user_ids": user_ids,
        "content_ids": content_ids,
        "campaign_ids": campaign_ids,
    }

    os.makedirs(os.path.dirname(MASTER_IDS_PATH), exist_ok=True)
    with open(MASTER_IDS_PATH, "w", encoding="utf-8") as f:
        json.dump(master_ids, f, indent=2)

    print(f"Generated master IDs: {len(user_ids)} users, {len(content_ids)} content, {len(campaign_ids)} campaigns")
    print(f"Saved to: {MASTER_IDS_PATH}")
    return master_ids


def load_master_ids():
    """Load master IDs from JSON file. Raises FileNotFoundError if not generated yet."""
    if not os.path.exists(MASTER_IDS_PATH):
        raise FileNotFoundError(
            f"Master IDs not found at {MASTER_IDS_PATH}. Run master_ids.py first."
        )
    with open(MASTER_IDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def generate_session_id():
    """Generate a single dynamic session ID (SES-XXXXXXXX hex format)."""
    return f"SES-{random.getrandbits(32):08X}"


def generate_event_id():
    """Generate a single dynamic event ID (EVT-XXXXXXXX hex format)."""
    return f"EVT-{random.getrandbits(32):08X}"


if __name__ == "__main__":
    generate_master_ids()
```

- [ ] **Step 2: Run and verify**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -m data_generation.master_ids
```

Expected output:
```
Generated master IDs: 5000 users, 2000 content, 50 campaigns
Saved to: .../data_generation/master_ids.json
```

- [ ] **Step 3: Verify JSON content**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -c "
import json
with open('data_generation/master_ids.json') as f:
    ids = json.load(f)
print('Users:', ids['user_ids'][0], '...', ids['user_ids'][-1], f'({len(ids[\"user_ids\"])})')
print('Content:', ids['content_ids'][0], '...', ids['content_ids'][-1], f'({len(ids[\"content_ids\"])})')
print('Campaigns:', ids['campaign_ids'][0], '...', ids['campaign_ids'][-1], f'({len(ids[\"campaign_ids\"])})')
"
```

Expected:
```
Users: USR-1000001 ... USR-1005000 (5000)
Content: CNT-000001 ... CNT-002000 (2000)
Campaigns: CAM-00001 ... CAM-00050 (50)
```

- [ ] **Step 4: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add data_generation/master_ids.py data_generation/master_ids.json
git commit -m "feat: master ID generator with deterministic seeds"
```

---

### Task 4: MySQL Data Generator (Users + Subscriptions)

**Files:**
- Create: `data_generation/generate_mysql_data.py`

- [ ] **Step 1: Write `data_generation/generate_mysql_data.py`**

```python
"""Generate and insert users + subscriptions into MySQL."""
import os
import sys
import random
import uuid
from datetime import date, timedelta

from faker import Faker
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MYSQL_URL, SEED, NUM_USERS
from data_generation.master_ids import load_master_ids

# ── Deterministic seeds ──
random.seed(SEED)
Faker.seed(SEED)
fake = Faker(["en_IN", "en_US", "en_GB"])

# ── Distribution constants ──
COUNTRY_WEIGHTS = {
    "India": 70, "United States": 10, "United Kingdom": 10,
    "Canada": 2, "Australia": 2, "Germany": 2, "UAE": 2, "Singapore": 2,
}
COUNTRY_LIST = list(COUNTRY_WEIGHTS.keys())
COUNTRY_PROBS = [w / sum(COUNTRY_WEIGHTS.values()) for w in COUNTRY_WEIGHTS.values()]

DEVICE_CATEGORIES = ["mobile", "desktop", "smart_tv"]
DEVICE_PROBS = [0.60, 0.25, 0.15]

USER_AGENTS = {
    "mobile": [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.64 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
        "JioHotstar/6.1.2 (Android 14; OnePlus 12)",
        "Android",  # abbreviated noise
        "iPhone",   # abbreviated noise
    ],
    "desktop": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Chrome",   # abbreviated noise
        "Desktop",  # abbreviated noise
    ],
    "smart_tv": [
        "Mozilla/5.0 (SMART-TV; LINUX; Tizen 7.0) AppleWebKit/537.36 (KHTML, like Gecko) Version/7.0 TV Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; BRAVIA 4K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        "Roku/DVP-13.0 (13.0.0.4100)",
        "Fire TV",  # abbreviated noise
        "SmartTV",  # abbreviated noise
    ],
}

PLANS = ["FREE", "BASIC", "PREMIUM", "VIP"]
PLAN_PROBS = [0.30, 0.35, 0.25, 0.10]

CANCEL_REASONS = [
    "too expensive", "bahut mehenga hai", "switched to Netflix",
    "not enough content", "content pasand nahi aaya",
    "poor streaming quality", "buffering issues",
    "free trial ended", "moved abroad",
    "using someone else's account", "paisa barbaad",
    "no IPL this season", "bad app experience",
    "student budget mein nahi aata", "Too Expensive!!!",
    None,  # some have no reason even when cancelled
]

NAME_CASING_FUNCS = [
    lambda n: n,                    # Title Case (default)
    lambda n: n.upper(),            # ALL CAPS
    lambda n: n.lower(),            # all lowercase
    lambda n: n.swapcase(),         # sWAP cASE
]


def _create_tables(engine):
    """Create MySQL tables with indexes."""
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS subscriptions"))
        conn.execute(text("DROP TABLE IF EXISTS users"))
        conn.execute(text("""
            CREATE TABLE users (
                user_id VARCHAR(20) PRIMARY KEY,
                email VARCHAR(255),
                full_name VARCHAR(255),
                signup_date DATE,
                device_category VARCHAR(20),
                user_agent TEXT,
                country VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_users_country (country)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE subscriptions (
                subscription_id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(20),
                plan_id VARCHAR(20),
                start_date DATE,
                end_date DATE NULL,
                cancel_reason TEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_subscriptions_user (user_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()
    print("MySQL tables created: users, subscriptions")


def _generate_users(user_ids):
    """Generate user records with realistic noise."""
    users = []
    for uid in user_ids:
        device_cat = random.choices(DEVICE_CATEGORIES, DEVICE_PROBS)[0]
        ua = random.choice(USER_AGENTS[device_cat])
        country = random.choices(COUNTRY_LIST, COUNTRY_PROBS)[0]

        name = fake.name()
        casing_fn = random.choices(NAME_CASING_FUNCS, weights=[60, 15, 15, 10])[0]
        name = casing_fn(name)

        email = fake.email()
        # ~10% add +alias
        if random.random() < 0.10:
            local, domain = email.split("@")
            alias = random.choice(["test", "work", "jio", "ipl", "stream"])
            email = f"{local}+{alias}@{domain}"

        signup = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2025, 12, 31))

        users.append({
            "user_id": uid,
            "email": email,
            "full_name": name,
            "signup_date": signup,
            "device_category": device_cat,
            "user_agent": ua,
            "country": country,
        })
    return users


def _generate_subscriptions(users):
    """Generate subscription records. Some users get 1-3 subscriptions."""
    subs = []
    for user in users:
        num_subs = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
        current_start = user["signup_date"]

        for i in range(num_subs):
            plan = random.choices(PLANS, PLAN_PROBS)[0]
            start = current_start + timedelta(days=random.randint(0, 30))
            is_active = (i == num_subs - 1) and random.random() < 0.40

            if is_active:
                end = None
                reason = None
            else:
                end = start + timedelta(days=random.randint(30, 365))
                reason = random.choice(CANCEL_REASONS)

            subs.append({
                "subscription_id": str(uuid.uuid4()),
                "user_id": user["user_id"],
                "plan_id": plan,
                "start_date": start,
                "end_date": end,
                "cancel_reason": reason,
            })
            if end:
                current_start = end
    return subs


def _insert_batch(engine, table_name, records, batch_size=1000):
    """Insert records in batches using parameterized queries."""
    if not records:
        return
    columns = list(records[0].keys())
    placeholders = ", ".join([f":{col}" for col in columns])
    col_names = ", ".join(columns)
    sql = text(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})")

    with engine.connect() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            conn.execute(sql, batch)
        conn.commit()


def generate_mysql_data():
    """Main entry point: create tables, generate data, insert into MySQL."""
    engine = create_engine(MYSQL_URL, echo=False)
    master_ids = load_master_ids()

    print("Creating MySQL tables...")
    _create_tables(engine)

    print(f"Generating {NUM_USERS} users...")
    users = _generate_users(master_ids["user_ids"])

    print("Generating subscriptions...")
    subs = _generate_subscriptions(users)

    print(f"Inserting {len(users)} users...")
    _insert_batch(engine, "users", users)

    print(f"Inserting {len(subs)} subscriptions...")
    _insert_batch(engine, "subscriptions", subs)

    print(f"MySQL complete: {len(users)} users, {len(subs)} subscriptions")
    engine.dispose()
    return len(users), len(subs)


if __name__ == "__main__":
    generate_mysql_data()
```

- [ ] **Step 2: Run with Docker infrastructure up**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -m data_generation.generate_mysql_data
```

Expected:
```
Creating MySQL tables...
MySQL tables created: users, subscriptions
Generating 5000 users...
Generating subscriptions...
Inserting 5000 users...
Inserting XXXX subscriptions...
MySQL complete: 5000 users, XXXX subscriptions
```

- [ ] **Step 3: Verify data in MySQL**

```bash
docker exec streaming_mysql mysql -u root -pstreaming_pass streaming_users -e "
  SELECT COUNT(*) AS user_count FROM users;
  SELECT COUNT(*) AS sub_count FROM subscriptions;
  SELECT country, COUNT(*) AS cnt FROM users GROUP BY country ORDER BY cnt DESC LIMIT 5;
  SELECT device_category, COUNT(*) AS cnt FROM users GROUP BY device_category ORDER BY cnt DESC;
"
```

Expected: ~5000 users, ~6000-8000 subscriptions, India dominant in country distribution.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add data_generation/generate_mysql_data.py
git commit -m "feat: MySQL data generator with users and subscriptions"
```

---

### Task 5: PostgreSQL Data Generator (Content Catalogue + Ratings)

**Files:**
- Create: `data_generation/generate_postgres_data.py`

- [ ] **Step 1: Write `data_generation/generate_postgres_data.py`**

```python
"""Generate and insert content catalogue + ratings into PostgreSQL."""
import os
import sys
import random
from datetime import datetime, timedelta
from decimal import Decimal

import numpy as np
from faker import Faker
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import POSTGRES_URL, SEED, NUM_CONTENT, NUM_RATINGS_PER_CONTENT_AVG
from data_generation.master_ids import load_master_ids

# ── Deterministic seeds ──
random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker(["en_IN", "en_US"])

# ── Content type distribution ──
CONTENT_TYPES = ["movie", "series", "sports", "highlight", "documentary"]
CONTENT_TYPE_PROBS = [0.50, 0.25, 0.10, 0.05, 0.10]

# ── Language distribution ──
LANGUAGES = ["Hindi", "English", "Tamil", "Telugu", "Kannada", "Malayalam", "Bengali", "Marathi"]
LANGUAGE_PROBS = [0.40, 0.25, 0.15, 0.10, 0.025, 0.025, 0.025, 0.025]

# ── Genre pools by content type ──
GENRE_POOLS = {
    "movie": ["Drama", "Thriller", "Comedy", "Romance", "Action", "Horror", "Crime", "Family"],
    "series": ["Drama", "Thriller", "Comedy", "Mystery", "Sci-Fi", "Crime", "Romance"],
    "sports": ["Sports", "Cricket", "Live"],
    "highlight": ["Sports", "Cricket", "Highlights"],
    "documentary": ["Documentary", "Nature", "History", "Science", "True Crime", "Biography"],
}

# ── Realistic title templates ──
MOVIE_TITLES = [
    "Dil Ka Raasta", "Midnight Express", "The Last Over", "Saathi",
    "Chandni Raat", "Urban Legends", "Fire and Ice", "Lakshmi",
    "The Dark Hour", "Rangeen", "Kabhi Alvida", "Street Kings",
    "Ankhon Dekhi", "The Pursuit", "Badlapur Returns", "Gulaal",
    "Mumbai Diaries", "Pathaan Returns", "Tiger Zinda Hai 2", "Jawan 2",
]
SERIES_TITLES = [
    "The Family Man S{}", "Sacred Games S{}", "Panchayat S{}",
    "Mirzapur S{}", "Scam 20{}", "Made in Heaven S{}",
    "Kota Factory S{}", "Aspirants S{}", "Delhi Crime S{}",
    "Breathe S{}", "Inside Edge S{}", "Paatal Lok S{}",
]
SPORTS_TITLES = [
    "IPL 2025: {} vs {}", "IPL 2024: {} vs {}",
    "T20 World Cup: {} vs {}", "Asia Cup: {} vs {}",
]
IPL_TEAMS = ["MI", "CSK", "RCB", "KKR", "DC", "SRH", "RR", "PBKS", "GT", "LSG"]
HIGHLIGHT_TITLES = [
    "IPL Highlights: {} vs {}", "Best Catches IPL 2025",
    "Top 10 Sixes: {}", "Player of the Match: Match {}",
]
DOC_TITLES = [
    "Inside IPL: The Business of Cricket", "Wild Karnataka",
    "India from Above", "The Cricket Revolution",
    "Bollywood: Behind the Scenes", "Street Food India S{}",
    "Cosmic India", "The Tiger's Trail",
]

RATING_SOURCES = ["app", "web", "tv"]
RATING_SOURCE_PROBS = [0.50, 0.30, 0.20]


def _create_tables(engine):
    """Create PostgreSQL tables with indexes."""
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ratings"))
        conn.execute(text("DROP TABLE IF EXISTS content_catalogue"))
        conn.execute(text("""
            CREATE TABLE content_catalogue (
                content_id VARCHAR(20) PRIMARY KEY,
                title VARCHAR(500),
                content_type VARCHAR(30),
                genre VARCHAR(255),
                runtime_value INTEGER,
                runtime_unit VARCHAR(10),
                release_year INTEGER,
                language VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE ratings (
                rating_id SERIAL PRIMARY KEY,
                user_id VARCHAR(20),
                content_id VARCHAR(20) REFERENCES content_catalogue(content_id),
                rating_value DECIMAL(2,1),
                rating_source VARCHAR(20),
                rated_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("CREATE INDEX idx_ratings_user ON ratings(user_id)"))
        conn.execute(text("CREATE INDEX idx_ratings_content ON ratings(content_id)"))
        conn.execute(text("CREATE INDEX idx_ratings_time ON ratings(rated_at)"))
        conn.execute(text("CREATE INDEX idx_ratings_pair ON ratings(user_id, content_id)"))
        conn.commit()
    print("PostgreSQL tables created: content_catalogue, ratings")


def _generate_title(content_type):
    """Generate a realistic title based on content type."""
    if content_type == "movie":
        title = random.choice(MOVIE_TITLES) + f" ({random.randint(2015, 2025)})"
    elif content_type == "series":
        template = random.choice(SERIES_TITLES)
        title = template.format(random.randint(1, 5))
    elif content_type == "sports":
        template = random.choice(SPORTS_TITLES)
        teams = random.sample(IPL_TEAMS, 2)
        title = template.format(teams[0], teams[1])
    elif content_type == "highlight":
        template = random.choice(HIGHLIGHT_TITLES)
        if "{}" in template:
            title = template.format(random.choice(IPL_TEAMS), random.choice(IPL_TEAMS))
        else:
            title = template
    else:  # documentary
        template = random.choice(DOC_TITLES)
        if "{}" in template:
            title = template.format(random.randint(1, 4))
        else:
            title = template

    # Noise: ~10% leading/trailing whitespace
    if random.random() < 0.10:
        title = random.choice(["  ", " ", "\t"]) + title + random.choice(["  ", " ", ""])
    return title


def _generate_genre(content_type):
    """Generate comma-separated genre with inconsistent separators."""
    pool = GENRE_POOLS[content_type]
    num_genres = random.choices([1, 2, 3], weights=[40, 40, 20])[0]
    genres = random.sample(pool, min(num_genres, len(pool)))

    # Noise: inconsistent separators
    sep = random.choice([",", ", ", "|"])
    return sep.join(genres)


def _generate_content(content_ids):
    """Generate content catalogue records."""
    content = []
    for cid in content_ids:
        ctype = random.choices(CONTENT_TYPES, CONTENT_TYPE_PROBS)[0]
        lang = random.choices(LANGUAGES, LANGUAGE_PROBS)[0]

        # Runtime: 80% in seconds, 20% in minutes
        if random.random() < 0.80:
            runtime_unit = "seconds"
            if ctype == "movie":
                runtime_value = random.randint(5400, 10800)  # 90-180 min
            elif ctype == "series":
                runtime_value = random.randint(1200, 3600)   # 20-60 min per episode
            elif ctype in ("sports",):
                runtime_value = random.randint(10800, 14400)  # 3-4 hours
            elif ctype == "highlight":
                runtime_value = random.randint(300, 1200)     # 5-20 min
            else:  # documentary
                runtime_value = random.randint(2400, 7200)   # 40-120 min
        else:
            runtime_unit = "minutes"
            if ctype == "movie":
                runtime_value = random.randint(90, 180)
            elif ctype == "series":
                runtime_value = random.randint(20, 60)
            elif ctype in ("sports",):
                runtime_value = random.randint(180, 240)
            elif ctype == "highlight":
                runtime_value = random.randint(5, 20)
            else:
                runtime_value = random.randint(40, 120)

        content.append({
            "content_id": cid,
            "title": _generate_title(ctype),
            "content_type": ctype,
            "genre": _generate_genre(ctype),
            "runtime_value": runtime_value,
            "runtime_unit": runtime_unit,
            "release_year": random.randint(2015, 2025),
            "language": lang,
        })
    return content


def _generate_ratings(user_ids, content_ids):
    """Generate ratings with positive skew, duplicates, and clock skew."""
    ratings = []
    total = NUM_CONTENT * NUM_RATINGS_PER_CONTENT_AVG  # ~16,000

    for _ in range(total):
        uid = random.choice(user_ids)
        cid = random.choice(content_ids)

        # Rating distribution: beta distribution skewed toward 3.5-4.5
        raw = np.random.beta(5, 2) * 4 + 1  # range 1-5, skewed high
        rating_val = round(round(raw * 2) / 2, 1)  # snap to 0.5 increments
        rating_val = max(1.0, min(5.0, rating_val))

        source = random.choices(RATING_SOURCES, RATING_SOURCE_PROBS)[0]

        # Timestamp with ~2% future clock skew
        days_ago = random.randint(0, 365 * 3)
        rated_at = datetime.now() - timedelta(days=days_ago)
        if random.random() < 0.02:
            rated_at = datetime.now() + timedelta(days=random.randint(1, 30))

        ratings.append({
            "user_id": uid,
            "content_id": cid,
            "rating_value": Decimal(str(rating_val)),
            "rating_source": source,
            "rated_at": rated_at,
        })

    # Inject ~5% explicit duplicates (same user+content, different rating)
    num_dupes = int(total * 0.05)
    for _ in range(num_dupes):
        original = random.choice(ratings)
        dupe = original.copy()
        raw = np.random.beta(5, 2) * 4 + 1
        dupe["rating_value"] = Decimal(str(max(1.0, min(5.0, round(round(raw * 2) / 2, 1)))))
        dupe["rated_at"] = original["rated_at"] + timedelta(days=random.randint(1, 60))
        ratings.append(dupe)

    return ratings


def _insert_batch(engine, table_name, records, batch_size=1000):
    """Insert records in batches."""
    if not records:
        return
    columns = list(records[0].keys())
    placeholders = ", ".join([f":{col}" for col in columns])
    col_names = ", ".join(columns)
    sql = text(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})")

    with engine.connect() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            conn.execute(sql, batch)
        conn.commit()


def generate_postgres_data():
    """Main entry point: create tables, generate data, insert into PostgreSQL."""
    engine = create_engine(POSTGRES_URL, echo=False)
    master_ids = load_master_ids()

    print("Creating PostgreSQL tables...")
    _create_tables(engine)

    print(f"Generating {NUM_CONTENT} content items...")
    content = _generate_content(master_ids["content_ids"])

    print("Generating ratings...")
    ratings = _generate_ratings(master_ids["user_ids"], master_ids["content_ids"])

    print(f"Inserting {len(content)} content items...")
    _insert_batch(engine, "content_catalogue", content)

    print(f"Inserting {len(ratings)} ratings...")
    _insert_batch(engine, "ratings", ratings)

    print(f"PostgreSQL complete: {len(content)} content, {len(ratings)} ratings")
    engine.dispose()
    return len(content), len(ratings)


if __name__ == "__main__":
    generate_postgres_data()
```

- [ ] **Step 2: Run with Docker infrastructure up**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -m data_generation.generate_postgres_data
```

Expected:
```
Creating PostgreSQL tables...
PostgreSQL tables created: content_catalogue, ratings
Generating 2000 content items...
Generating ratings...
Inserting 2000 content items...
Inserting ~16800 ratings...
PostgreSQL complete: 2000 content, ~16800 ratings
```

- [ ] **Step 3: Verify data in PostgreSQL**

```bash
docker exec streaming_postgres psql -U postgres -d streaming_content -c "
  SELECT COUNT(*) AS content_count FROM content_catalogue;
  SELECT content_type, COUNT(*) FROM content_catalogue GROUP BY content_type ORDER BY count DESC;
  SELECT COUNT(*) AS rating_count FROM ratings;
  SELECT rating_value, COUNT(*) FROM ratings GROUP BY rating_value ORDER BY rating_value;
"
```

Expected: 2000 content items, ~16000+ ratings, movie dominant, ratings skewed to 3.5-4.5.

- [ ] **Step 4: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add data_generation/generate_postgres_data.py
git commit -m "feat: PostgreSQL data generator with content catalogue and ratings"
```

---

### Task 6: File Sources Generator (CSV, JSON, Excel, Reviews, Thumbnails)

**Files:**
- Create: `data_generation/generate_file_sources.py`

- [ ] **Step 1: Write `data_generation/generate_file_sources.py`**

```python
"""Generate CSV, JSON, Excel, TXT reviews, and image thumbnail files."""
import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta

from faker import Faker
from openpyxl import Workbook
from PIL import Image, ImageDraw, ImageFont

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    DATA_SOURCES_DIR, SEED, NUM_CONTENT, NUM_CAMPAIGNS,
    NUM_VIEWING_EVENTS, NUM_REVIEWS,
)
from data_generation.master_ids import load_master_ids, generate_session_id, generate_event_id

# ── Deterministic seeds ──
random.seed(SEED)
Faker.seed(SEED)
fake = Faker(["en_IN", "en_US"])

# ── Constants ──
CBFC_RATINGS = ["U", "UA", "A", "S"]
CBFC_PROBS = [0.30, 0.40, 0.25, 0.05]

EVENT_TYPES = ["PLAY", "PAUSE", "SEEK", "BUFFER_START", "BUFFER_END", "COMPLETE"]
EVENT_PROBS = [0.35, 0.20, 0.15, 0.10, 0.10, 0.10]

DEVICE_CATEGORIES = ["mobile", "desktop", "smart_tv"]
DEVICE_PROBS = [0.60, 0.25, 0.15]

REFERRERS = ["home_banner", "search", "recommendation", "deeplink", "push_notification", "social_share"]

INDIAN_ACTORS = [
    "Shah Rukh Khan", "Aamir Khan", "Salman Khan", "Deepika Padukone",
    "Alia Bhatt", "Ranveer Singh", "Priyanka Chopra", "Akshay Kumar",
    "Ranbir Kapoor", "Katrina Kaif", "Hrithik Roshan", "Kareena Kapoor",
    "Vijay", "Rajinikanth", "Prabhas", "Allu Arjun", "Ram Charan",
    "Samantha Ruth Prabhu", "Rashmika Mandanna", "Tamannaah Bhatia",
    "Nawazuddin Siddiqui", "Pankaj Tripathi", "Manoj Bajpayee",
    "Radhika Apte", "Tabu", "Vidya Balan", "Ayushmann Khurrana",
    "Kartik Aaryan", "Janhvi Kapoor", "Ananya Panday",
]

CAMPAIGN_NAMES = [
    "IPL Season 2025 Splash", "Diwali Mega Sale", "Republic Day Special",
    "Summer Blockbuster", "Holi Color Burst", "New Year Countdown",
    "Cricket Fever", "Monsoon Magic", "Navratri Nights",
    "Back to School", "Valentine's Binge", "Independence Day Marathon",
    "Ganesh Chaturthi Special", "Durga Puja Stream", "Christmas Carnival",
    "Eid Celebrations", "Pongal Premiere", "Baisakhi Blast",
    "Women's Day Special", "Teacher's Day Marathon",
]

REVIEW_TEMPLATES = [
    "Bahut accha {type}! Must watch {emoji}",
    "Kya baat hai, {name} ne kamaal kar diya {emoji}",
    "Waste of time. Paisa barbaad. Don't watch.",
    "One of the best {type}s I have watched this year. {emoji}",
    "Average {type}. Nothing special but timepass hai.",
    "Mind-blowing performance by the entire cast! {emoji}{emoji}",
    "Story was predictable but acting was superb.",
    "Not as good as the original. Disappointed {emoji}",
    "Perfect for family viewing. Everyone enjoyed it {emoji}",
    "Too long. Should have been 30 min shorter.",
    "The IPL coverage is unmatched! Best streaming quality {emoji}",
    "Buffering was terrible during the match. Fix your servers!",
    "Downloaded and watched offline. Smooth experience {emoji}",
    "Subtitles were wrong in many places. Please fix!",
    "I watch this every night before sleeping. Comfort {type} {emoji}",
]

EMOJIS = ["🔥", "👍", "❤️", "🎬", "⭐", "💯", "🏏", "😍", "👎", "😢", "🤩", "💪"]

HINDI_PHRASES = [
    "bahut badiya", "ekdum mast", "paisa vasool", "time waste",
    "dekhne layak", "must watch", "bekar picture", "kamaal ki acting",
]


def _generate_csv(content_ids):
    """Generate content_metadata.csv."""
    csv_dir = os.path.join(DATA_SOURCES_DIR, "csv")
    os.makedirs(csv_dir, exist_ok=True)
    path = os.path.join(csv_dir, "content_metadata.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["content_id", "budget", "cast_members", "content_rating"])
        writer.writeheader()

        for cid in content_ids:
            # Budget: 0 for ~15% unknown
            if random.random() < 0.15:
                budget = 0
            else:
                budget = random.randint(10_00_000, 300_00_00_000)  # 10L to 300Cr INR

            # Cast: 2-6 actors, pipe-separated
            num_cast = random.randint(2, 6)
            cast = random.sample(INDIAN_ACTORS, min(num_cast, len(INDIAN_ACTORS)))
            cast_str = "|".join(cast)

            rating = random.choices(CBFC_RATINGS, CBFC_PROBS)[0]

            writer.writerow({
                "content_id": cid,
                "budget": budget,
                "cast_members": cast_str,
                "content_rating": rating,
            })

    print(f"CSV generated: {path} ({len(content_ids)} rows)")


def _generate_json_events(user_ids, content_ids):
    """Generate viewing_events_batch.json with ~50,000 events."""
    json_dir = os.path.join(DATA_SOURCES_DIR, "json")
    os.makedirs(json_dir, exist_ok=True)
    path = os.path.join(json_dir, "viewing_events_batch.json")

    events = []
    # Generate session pool (reuse some across events)
    session_pool = [generate_session_id() for _ in range(NUM_VIEWING_EVENTS // 5)]

    for _ in range(NUM_VIEWING_EVENTS):
        event_type = random.choices(EVENT_TYPES, EVENT_PROBS)[0]
        device_cat = random.choices(DEVICE_CATEGORIES, DEVICE_PROBS)[0]

        # Session: ~5% null, rest from pool
        if random.random() < 0.05:
            session_id = None
            session_start_ts = None
        else:
            session_id = random.choice(session_pool)
            session_start_ts = (
                datetime(2025, 1, 1) + timedelta(days=random.randint(0, 180))
            ).isoformat() + "Z"

        # Evening peak distribution (7-11 PM IST = 13:30-17:30 UTC)
        hour = random.choices(
            range(24),
            weights=[1,1,1,1,1,1,2,3,3,3,4,4,5,6,8,10,10,8,6,12,15,15,10,5],
        )[0]
        event_ts_dt = datetime(2025, 1, 1) + timedelta(
            days=random.randint(0, 180),
            hours=hour,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
            milliseconds=random.randint(0, 999),
        )
        event_ts = event_ts_dt.isoformat(timespec="milliseconds") + "Z"

        # watch_duration_ms / seek_position_ms logic
        if event_type == "SEEK":
            watch_duration_ms = None
            seek_position_ms = random.randint(0, 7200000)  # 0 to 2 hours
        else:
            watch_duration_ms = random.randint(1000, 7200000)
            seek_position_ms = None

        # ~3% SEEK misclassified as PLAY
        if event_type == "SEEK" and random.random() < 0.03:
            event_type = "PLAY"

        events.append({
            "event_id": generate_event_id(),
            "user_id": random.choice(user_ids),
            "content_id": random.choice(content_ids),
            "session_id": session_id,
            "device_category": device_cat,
            "event_type": event_type,
            "watch_duration_ms": watch_duration_ms,
            "seek_position_ms": seek_position_ms,
            "event_ts": event_ts,
            "session_start_ts": session_start_ts,
            "referrer": random.choice(REFERRERS),
        })

    with open(path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, default=str)

    print(f"JSON generated: {path} ({len(events)} events)")


def _generate_excel(campaign_ids):
    """Generate ad_campaigns.xlsx with budget noise."""
    excel_dir = os.path.join(DATA_SOURCES_DIR, "excel")
    os.makedirs(excel_dir, exist_ok=True)
    path = os.path.join(excel_dir, "ad_campaigns.xlsx")

    wb = Workbook()
    ws = wb.active
    ws.title = "Ad Campaigns"
    ws.append(["campaign_id", "campaign_name", "budget", "start_date", "end_date"])

    for i, cid in enumerate(campaign_ids):
        name = CAMPAIGN_NAMES[i % len(CAMPAIGN_NAMES)]
        if i >= len(CAMPAIGN_NAMES):
            name = f"{name} Vol.{i // len(CAMPAIGN_NAMES) + 1}"

        raw_budget = random.randint(100000, 50000000)
        # ~30% stored as string with "USD" prefix
        if random.random() < 0.30:
            budget = f"USD {raw_budget}"
        else:
            budget = raw_budget

        start = fake.date_between(start_date="-1y", end_date="today")
        end = start + timedelta(days=random.randint(7, 90))

        ws.append([cid, name, budget, start, end])

    wb.save(path)
    print(f"Excel generated: {path} ({len(campaign_ids)} campaigns)")


def _generate_reviews(user_ids, content_ids):
    """Generate ~500 review .txt files with emojis, URLs, multilingual text."""
    reviews_dir = os.path.join(DATA_SOURCES_DIR, "reviews")
    os.makedirs(reviews_dir, exist_ok=True)

    used_pairs = set()
    count = 0
    while count < NUM_REVIEWS:
        uid = random.choice(user_ids)
        cid = random.choice(content_ids)
        pair = (uid, cid)
        if pair in used_pairs:
            continue
        used_pairs.add(pair)

        review_date = fake.date_between(start_date="-2y", end_date="today")
        filename = f"{uid}_{cid}_{review_date}.txt"
        filepath = os.path.join(reviews_dir, filename)

        # Build review text (1-3 paragraphs)
        paragraphs = []
        for _ in range(random.randint(1, 3)):
            template = random.choice(REVIEW_TEMPLATES)
            text_content = template.format(
                type=random.choice(["movie", "show", "series", "match"]),
                name=random.choice(INDIAN_ACTORS),
                emoji=random.choice(EMOJIS),
            )
            # Sprinkle Hindi phrases
            if random.random() < 0.40:
                text_content += f" {random.choice(HINDI_PHRASES)}"
            # Sprinkle URLs
            if random.random() < 0.15:
                text_content += f" https://example.com/review/{random.randint(1000, 9999)}"
            paragraphs.append(text_content)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n\n".join(paragraphs))

        count += 1

    print(f"Reviews generated: {reviews_dir}/ ({count} files)")


def _generate_thumbnails(content_ids):
    """Generate placeholder thumbnail images with PIL."""
    thumb_dir = os.path.join(DATA_SOURCES_DIR, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    colors = [
        (220, 50, 50), (50, 120, 220), (50, 180, 50), (220, 180, 50),
        (150, 50, 200), (50, 200, 200), (200, 100, 50), (100, 100, 100),
    ]

    for cid in content_ids:
        # All content gets v1
        for version in ["v1"]:
            _create_thumbnail(thumb_dir, cid, version, colors)

        # ~40% also get v2
        if random.random() < 0.40:
            _create_thumbnail(thumb_dir, cid, "v2", colors)

    v1_count = len(content_ids)
    v2_count = len([f for f in os.listdir(thumb_dir) if "_v2.jpg" in f])
    print(f"Thumbnails generated: {thumb_dir}/ ({v1_count} v1 + {v2_count} v2 = {v1_count + v2_count} files)")


def _create_thumbnail(thumb_dir, content_id, version, colors):
    """Create a single 320x180 placeholder thumbnail."""
    img = Image.new("RGB", (320, 180), random.choice(colors))
    draw = ImageDraw.Draw(img)

    # Draw content ID text
    label = f"{content_id}\n{version}"
    try:
        font = ImageFont.truetype("arial.ttf", 20)
    except (OSError, IOError):
        font = ImageFont.load_default()

    bbox = draw.textbbox((0, 0), label, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    x = (320 - text_w) // 2
    y = (180 - text_h) // 2
    draw.text((x, y), label, fill=(255, 255, 255), font=font)

    filename = f"{content_id}_{version}.jpg"
    img.save(os.path.join(thumb_dir, filename), "JPEG", quality=85)


def generate_file_sources():
    """Main entry point: generate all file-based data sources."""
    master_ids = load_master_ids()
    user_ids = master_ids["user_ids"]
    content_ids = master_ids["content_ids"]
    campaign_ids = master_ids["campaign_ids"]

    print("Generating CSV...")
    _generate_csv(content_ids)

    print("Generating JSON viewing events...")
    _generate_json_events(user_ids, content_ids)

    print("Generating Excel ad campaigns...")
    _generate_excel(campaign_ids)

    print("Generating text reviews...")
    _generate_reviews(user_ids, content_ids)

    print("Generating thumbnail images...")
    _generate_thumbnails(content_ids)

    print("All file sources generated.")


if __name__ == "__main__":
    generate_file_sources()
```

- [ ] **Step 2: Run the file generator**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -m data_generation.generate_file_sources
```

Expected output (takes ~30-60 seconds):
```
Generating CSV...
CSV generated: .../data_sources/csv/content_metadata.csv (2000 rows)
Generating JSON viewing events...
JSON generated: .../data_sources/json/viewing_events_batch.json (50000 events)
Generating Excel ad campaigns...
Excel generated: .../data_sources/excel/ad_campaigns.xlsx (50 campaigns)
Generating text reviews...
Reviews generated: .../data_sources/reviews/ (500 files)
Generating thumbnail images...
Thumbnails generated: .../data_sources/thumbnails/ (2000 v1 + ~800 v2 = ~2800 files)
All file sources generated.
```

- [ ] **Step 3: Spot-check generated files**

```bash
cd /c/Users/sjdin/media_stream_analytics
# CSV first/last lines
python -c "
import csv
with open('data_sources/csv/content_metadata.csv') as f:
    reader = list(csv.DictReader(f))
    print(f'CSV rows: {len(reader)}')
    print(f'First: {reader[0]}')
"

# JSON event count and sample
python -c "
import json
with open('data_sources/json/viewing_events_batch.json') as f:
    events = json.load(f)
print(f'JSON events: {len(events)}')
print(f'Sample: {json.dumps(events[0], indent=2)}')
"

# Review file count
python -c "import os; files = os.listdir('data_sources/reviews'); print(f'Reviews: {len(files)}')"

# Thumbnail count
python -c "import os; files = os.listdir('data_sources/thumbnails'); print(f'Thumbnails: {len(files)}')"
```

- [ ] **Step 4: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add data_generation/generate_file_sources.py
git commit -m "feat: file sources generator (CSV, JSON, Excel, reviews, thumbnails)"
```

Note: Do NOT commit generated data files — only the generator script. Add `data_sources/` and `data_generation/master_ids.json` to `.gitignore`.

- [ ] **Step 5: Create `.gitignore`**

```
# Generated data
data_sources/csv/
data_sources/json/
data_sources/excel/
data_sources/reviews/
data_sources/thumbnails/
data_generation/master_ids.json
logs/

# Python
__pycache__/
*.pyc
.venv/
```

```bash
cd /c/Users/sjdin/media_stream_analytics
git add .gitignore
git commit -m "chore: add gitignore for generated data and Python artifacts"
```

---

### Task 7: Kafka Producer with IPL Match Simulation

**Files:**
- Create: `data_generation/kafka_producer.py`

- [ ] **Step 1: Write `data_generation/kafka_producer.py`**

```python
"""Kafka streaming producer simulating IPL match day viewing events."""
import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC, SEED
from data_generation.master_ids import load_master_ids, generate_session_id, generate_event_id

# ── Deterministic seed ──
random.seed(SEED)

# ── Constants ──
EVENT_TYPES = ["PLAY", "PAUSE", "SEEK", "BUFFER_START", "BUFFER_END", "COMPLETE"]
REFERRERS = ["home_banner", "search", "recommendation", "deeplink", "push_notification", "social_share"]
DEVICE_CATEGORIES = ["mobile", "desktop", "smart_tv"]
DEVICE_PROBS = [0.60, 0.25, 0.15]

# ── IPL Match Phases ──
# Each phase: (name, duration_minutes, event_count, event_type_weights)
MATCH_PHASES = [
    {
        "name": "pre_match",
        "duration_min": 30,
        "events": 500,
        "weights": [0.55, 0.15, 0.05, 0.10, 0.10, 0.05],  # heavy PLAY
    },
    {
        "name": "innings_1",
        "duration_min": 90,
        "events": 2500,
        "weights": [0.35, 0.15, 0.15, 0.12, 0.12, 0.11],  # balanced, more engagement
    },
    {
        "name": "mid_innings_break",
        "duration_min": 20,
        "events": 500,
        "weights": [0.10, 0.50, 0.05, 0.05, 0.05, 0.25],  # heavy PAUSE + COMPLETE
    },
    {
        "name": "innings_2",
        "duration_min": 90,
        "events": 3000,
        "weights": [0.30, 0.12, 0.15, 0.18, 0.15, 0.10],  # peak load, more BUFFER
    },
    {
        "name": "post_match",
        "duration_min": 30,
        "events": 1000,
        "weights": [0.15, 0.10, 0.05, 0.05, 0.05, 0.60],  # heavy COMPLETE
    },
]

# Graceful shutdown
_running = True


def _signal_handler(sig, frame):
    global _running
    print("\nShutting down gracefully...")
    _running = False


signal.signal(signal.SIGINT, _signal_handler)


def _create_producer():
    """Create Kafka producer with retries."""
    retries = 10
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            if attempt < retries - 1:
                print(f"Kafka not ready, retrying ({attempt + 1}/{retries})...")
                time.sleep(3)
            else:
                raise


def _generate_event(user_ids, content_ids, session_pool, phase_weights, phase_time_base, duration_min):
    """Generate a single streaming event."""
    event_type = random.choices(EVENT_TYPES, phase_weights)[0]
    device_cat = random.choices(DEVICE_CATEGORIES, DEVICE_PROBS)[0]

    # Session management
    if random.random() < 0.10:
        # 10% reuse old sessions (cross-day reuse noise)
        session_id = random.choice(session_pool) if session_pool else generate_session_id()
    else:
        session_id = generate_session_id()
        session_pool.append(session_id)

    # Keep session pool bounded
    if len(session_pool) > 5000:
        session_pool.pop(0)

    session_start_ts = phase_time_base.isoformat(timespec="milliseconds") + "Z"

    # Event timestamp with clock skew ±30 seconds
    offset_minutes = random.uniform(0, duration_min)
    event_ts_dt = phase_time_base + timedelta(minutes=offset_minutes)
    clock_skew = timedelta(seconds=random.uniform(-30, 30))
    event_ts_dt += clock_skew
    event_ts = event_ts_dt.isoformat(timespec="milliseconds") + "Z"

    # watch_duration_ms / seek_position_ms
    if event_type == "SEEK":
        watch_duration_ms = None
        seek_position_ms = random.randint(0, 7200000)
        # 3% misclassification
        if random.random() < 0.03:
            event_type = "PLAY"
    else:
        watch_duration_ms = random.randint(1000, 7200000)
        seek_position_ms = None

    return {
        "event_id": generate_event_id(),
        "user_id": random.choice(user_ids),
        "content_id": random.choice(content_ids),
        "session_id": session_id,
        "device_category": device_cat,
        "event_type": event_type,
        "watch_duration_ms": watch_duration_ms,
        "seek_position_ms": seek_position_ms,
        "event_ts": event_ts,
        "session_start_ts": session_start_ts,
        "referrer": random.choice(REFERRERS),
    }


def run_batch_mode(producer, user_ids, content_ids):
    """Produce ~7500 events simulating one complete IPL match, then exit."""
    print("=== BATCH MODE: Simulating one IPL match day ===")
    session_pool = []
    total_sent = 0

    # Match starts at 7:30 PM IST today
    match_start = datetime.now().replace(hour=19, minute=30, second=0, microsecond=0)
    phase_time = match_start

    for phase in MATCH_PHASES:
        print(f"\n  Phase: {phase['name']} ({phase['events']} events, {phase['duration_min']} min)")

        for i in range(phase["events"]):
            if not _running:
                break

            event = _generate_event(
                user_ids, content_ids, session_pool,
                phase["weights"], phase_time, phase["duration_min"],
            )

            producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
            total_sent += 1

            if total_sent % 500 == 0:
                print(f"    Sent {total_sent} events...")

            # Small delay to avoid overwhelming local Kafka
            time.sleep(random.uniform(0.01, 0.03))

        phase_time += timedelta(minutes=phase["duration_min"])

        if not _running:
            break

    producer.flush()
    print(f"\n=== BATCH COMPLETE: {total_sent} events sent ===")


def run_live_mode(producer, user_ids, content_ids):
    """Continuously produce events until Ctrl+C. Cycles through match phases."""
    print("=== LIVE MODE: Continuous IPL streaming (Ctrl+C to stop) ===")
    session_pool = []
    total_sent = 0
    phase_idx = 0

    match_start = datetime.now().replace(hour=19, minute=30, second=0, microsecond=0)

    while _running:
        phase = MATCH_PHASES[phase_idx % len(MATCH_PHASES)]
        phase_time = match_start + timedelta(
            minutes=sum(p["duration_min"] for p in MATCH_PHASES[:phase_idx % len(MATCH_PHASES)])
        )

        print(f"\n  Phase: {phase['name']}")

        for _ in range(phase["events"]):
            if not _running:
                break

            event = _generate_event(
                user_ids, content_ids, session_pool,
                phase["weights"], phase_time, phase["duration_min"],
            )

            producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
            total_sent += 1

            if total_sent % 500 == 0:
                print(f"    Sent {total_sent} events...")

            time.sleep(random.uniform(0.1, 0.3))

        phase_idx += 1

    producer.flush()
    print(f"\n=== LIVE MODE STOPPED: {total_sent} events sent ===")


def main():
    parser = argparse.ArgumentParser(description="Kafka IPL streaming event producer")
    parser.add_argument(
        "--mode", choices=["batch", "live"], default="batch",
        help="batch: ~7500 events then exit | live: continuous until Ctrl+C",
    )
    args = parser.parse_args()

    master_ids = load_master_ids()
    user_ids = master_ids["user_ids"]
    content_ids = master_ids["content_ids"]

    producer = _create_producer()

    if args.mode == "batch":
        run_batch_mode(producer, user_ids, content_ids)
    else:
        run_live_mode(producer, user_ids, content_ids)

    producer.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify script loads without Kafka**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -c "from data_generation.kafka_producer import main; print('Kafka producer module loaded OK')"
```

Expected: `Kafka producer module loaded OK`

- [ ] **Step 3: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add data_generation/kafka_producer.py
git commit -m "feat: Kafka producer with IPL match simulation (batch/live modes)"
```

---

### Task 8: Orchestrator — `run_phase1.py`

**Files:**
- Create: `run_phase1.py`

- [ ] **Step 1: Write `run_phase1.py`**

```python
"""Phase 1 orchestrator: starts infrastructure and runs all data generators."""
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

import pymysql
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config.settings import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE,
    KAFKA_TOPIC, LOGS_DIR,
)

# ── Logging setup ──
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
    log.info(f"\n{'='*60}")
    log.info(f"  STEP {num}/{total}: {description}")
    log.info(f"{'='*60}")


def check_docker():
    """Verify Docker is running."""
    try:
        result = subprocess.run(
            ["docker", "info"], capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            log.error("Docker is not running. Please start Docker Desktop.")
            sys.exit(1)
        log.info("Docker is running.")
    except FileNotFoundError:
        log.error("Docker not found. Please install Docker Desktop.")
        sys.exit(1)


def start_infrastructure():
    """Run docker compose up -d."""
    result = subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=DOCKER_COMPOSE_DIR,
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.error(f"docker compose failed:\n{result.stderr}")
        sys.exit(1)
    log.info("Docker containers started.")


def wait_for_mysql(timeout=60):
    """Wait until MySQL is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymysql.connect(
                host=MYSQL_HOST, port=MYSQL_PORT,
                user=MYSQL_USER, password=MYSQL_PASSWORD,
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
    """Wait until PostgreSQL is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DATABASE,
            )
            conn.close()
            log.info("PostgreSQL is ready.")
            return
        except psycopg2.OperationalError:
            time.sleep(2)
    log.error(f"PostgreSQL did not become ready within {timeout}s")
    sys.exit(1)


def create_kafka_topic():
    """Create Kafka topic explicitly via docker exec."""
    result = subprocess.run(
        [
            "docker", "exec", "streaming_kafka",
            "kafka-topics", "--create",
            "--topic", KAFKA_TOPIC,
            "--bootstrap-server", "localhost:9092",
            "--partitions", "3",
            "--replication-factor", "1",
            "--if-not-exists",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0 and "already exists" not in result.stderr:
        log.warning(f"Kafka topic creation warning: {result.stderr.strip()}")
    else:
        log.info(f"Kafka topic '{KAFKA_TOPIC}' created (or already exists).")


def run_generator(module_name, description):
    """Run a Python generator module as a subprocess."""
    start = time.time()
    result = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        capture_output=True, text=True,
    )
    elapsed = time.time() - start

    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            log.info(f"  {line}")

    if result.returncode != 0:
        log.error(f"{description} FAILED ({elapsed:.1f}s):\n{result.stderr}")
        sys.exit(1)

    log.info(f"{description} completed in {elapsed:.1f}s")


def print_summary():
    """Print final summary and Kafka instructions."""
    log.info(f"\n{'='*60}")
    log.info("  PHASE 1 COMPLETE")
    log.info(f"{'='*60}")
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
    TOTAL_STEPS = 9
    log.info(f"Phase 1 Data Generation — Started at {datetime.now()}")

    step(1, TOTAL_STEPS, "Verify Docker is running")
    check_docker()

    step(2, TOTAL_STEPS, "Start infrastructure (docker compose up)")
    start_infrastructure()

    step(3, TOTAL_STEPS, "Wait for MySQL to be ready")
    wait_for_mysql()

    step(4, TOTAL_STEPS, "Wait for PostgreSQL to be ready")
    wait_for_postgres()

    step(5, TOTAL_STEPS, "Create Kafka topic")
    create_kafka_topic()

    step(6, TOTAL_STEPS, "Generate master entity IDs")
    run_generator("data_generation.master_ids", "Master IDs generation")

    step(7, TOTAL_STEPS, "Populate MySQL (users + subscriptions)")
    run_generator("data_generation.generate_mysql_data", "MySQL data generation")

    step(8, TOTAL_STEPS, "Populate PostgreSQL (content + ratings)")
    run_generator("data_generation.generate_postgres_data", "PostgreSQL data generation")

    # File sources don't need Docker — run inline for better logging
    step(9, TOTAL_STEPS, "Generate file sources (CSV, JSON, Excel, reviews, thumbnails)")
    from data_generation.generate_file_sources import generate_file_sources
    generate_file_sources()

    print_summary()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify script loads**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -c "from run_phase1 import main; print('Orchestrator module loaded OK')"
```

- [ ] **Step 3: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add run_phase1.py
git commit -m "feat: Phase 1 orchestrator with Docker checks and step logging"
```

---

### Task 9: Validation Script

**Files:**
- Create: `validate_phase1.py`

- [ ] **Step 1: Write `validate_phase1.py`**

```python
"""Post-generation validation for Phase 1 data sources."""
import csv
import json
import os
import subprocess
import sys

import pymysql
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config.settings import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE,
    DATA_SOURCES_DIR, KAFKA_TOPIC,
)


class ValidationResult:
    def __init__(self):
        self.results = []

    def check(self, name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        self.results.append((name, status, detail))
        marker = "+" if condition else "X"
        print(f"  [{marker}] {name}: {detail}")

    def summary(self):
        passed = sum(1 for _, s, _ in self.results if s == "PASS")
        total = len(self.results)
        print(f"\n{'='*50}")
        print(f"  VALIDATION: {passed}/{total} checks passed")
        print(f"{'='*50}")
        if passed < total:
            print("\n  Failed checks:")
            for name, status, detail in self.results:
                if status == "FAIL":
                    print(f"    - {name}: {detail}")
        return passed == total


def validate():
    v = ValidationResult()

    # ── MySQL checks ──
    print("\n--- MySQL ---")
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM users")
        user_count = cur.fetchone()[0]
        v.check("MySQL users", user_count == 5000, f"{user_count} rows")

        cur.execute("SELECT COUNT(*) FROM subscriptions")
        sub_count = cur.fetchone()[0]
        v.check("MySQL subscriptions", 6000 <= sub_count <= 8000, f"{sub_count} rows")

        # Country distribution check
        cur.execute("SELECT country, COUNT(*) as cnt FROM users GROUP BY country ORDER BY cnt DESC LIMIT 1")
        top_country = cur.fetchone()
        v.check("MySQL country distribution", top_country[0] == "India", f"Top country: {top_country[0]} ({top_country[1]})")

        conn.close()
    except Exception as e:
        v.check("MySQL connection", False, str(e))

    # ── PostgreSQL checks ──
    print("\n--- PostgreSQL ---")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DATABASE,
        )
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM content_catalogue")
        content_count = cur.fetchone()[0]
        v.check("PG content_catalogue", content_count == 2000, f"{content_count} rows")

        cur.execute("SELECT COUNT(*) FROM ratings")
        rating_count = cur.fetchone()[0]
        v.check("PG ratings", 15000 <= rating_count <= 20000, f"{rating_count} rows")

        # Cross-system join: ratings.user_id should match master IDs used in MySQL
        cur.execute("""
            SELECT COUNT(DISTINCT user_id) FROM ratings
            WHERE user_id LIKE 'USR-%%'
        """)
        valid_users = cur.fetchone()[0]
        v.check("PG ratings user_id format", valid_users > 0, f"{valid_users} distinct users with valid IDs")

        # Duplicate check
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT user_id, content_id, COUNT(*) as cnt
                FROM ratings GROUP BY user_id, content_id HAVING COUNT(*) > 1
            ) dupes
        """)
        dupe_count = cur.fetchone()[0]
        v.check("PG ratings has duplicates (intended)", dupe_count > 0, f"{dupe_count} duplicate pairs")

        conn.close()
    except Exception as e:
        v.check("PostgreSQL connection", False, str(e))

    # ── File checks ──
    print("\n--- Files ---")

    # CSV
    csv_path = os.path.join(DATA_SOURCES_DIR, "csv", "content_metadata.csv")
    if os.path.exists(csv_path):
        with open(csv_path, encoding="utf-8") as f:
            row_count = sum(1 for _ in csv.reader(f)) - 1  # minus header
        v.check("CSV content_metadata.csv", row_count == 2000, f"{row_count} rows")
    else:
        v.check("CSV content_metadata.csv", False, "File not found")

    # JSON
    json_path = os.path.join(DATA_SOURCES_DIR, "json", "viewing_events_batch.json")
    if os.path.exists(json_path):
        with open(json_path, encoding="utf-8") as f:
            events = json.load(f)
        v.check("JSON viewing_events_batch.json", len(events) == 50000, f"{len(events)} events")
        # Check schema
        if events:
            required_keys = {"event_id", "user_id", "content_id", "session_id", "device_category",
                             "event_type", "watch_duration_ms", "seek_position_ms", "event_ts",
                             "session_start_ts", "referrer"}
            actual_keys = set(events[0].keys())
            v.check("JSON schema complete", required_keys.issubset(actual_keys),
                     f"Missing: {required_keys - actual_keys}" if not required_keys.issubset(actual_keys) else "All fields present")
    else:
        v.check("JSON viewing_events_batch.json", False, "File not found")

    # Excel
    excel_path = os.path.join(DATA_SOURCES_DIR, "excel", "ad_campaigns.xlsx")
    v.check("Excel ad_campaigns.xlsx", os.path.exists(excel_path),
            "Exists" if os.path.exists(excel_path) else "File not found")

    # Reviews
    reviews_dir = os.path.join(DATA_SOURCES_DIR, "reviews")
    if os.path.isdir(reviews_dir):
        review_count = len([f for f in os.listdir(reviews_dir) if f.endswith(".txt")])
        v.check("TXT reviews", 450 <= review_count <= 550, f"{review_count} files")
    else:
        v.check("TXT reviews", False, "Directory not found")

    # Thumbnails
    thumb_dir = os.path.join(DATA_SOURCES_DIR, "thumbnails")
    if os.path.isdir(thumb_dir):
        thumb_count = len([f for f in os.listdir(thumb_dir) if f.endswith(".jpg")])
        v.check("Image thumbnails", 2500 <= thumb_count <= 3100, f"{thumb_count} files")
    else:
        v.check("Image thumbnails", False, "Directory not found")

    # ── Kafka topic check ──
    print("\n--- Kafka ---")
    try:
        result = subprocess.run(
            [
                "docker", "exec", "streaming_kafka",
                "kafka-topics", "--list",
                "--bootstrap-server", "localhost:9092",
            ],
            capture_output=True, text=True, timeout=10,
        )
        topics = result.stdout.strip().split("\n")
        v.check("Kafka topic exists", KAFKA_TOPIC in topics,
                f"'{KAFKA_TOPIC}' {'found' if KAFKA_TOPIC in topics else 'not found'}")
    except Exception as e:
        v.check("Kafka topic check", False, str(e))

    # ── Summary ──
    all_passed = v.summary()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    validate()
```

- [ ] **Step 2: Commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add validate_phase1.py
git commit -m "feat: Phase 1 validation script with row counts, joins, and file checks"
```

---

### Task 10: End-to-End Test Run

- [ ] **Step 1: Clean slate — remove any previously generated data**

```bash
cd /c/Users/sjdin/media_stream_analytics
rm -rf data_sources/csv/* data_sources/json/* data_sources/excel/* data_sources/reviews/* data_sources/thumbnails/* data_generation/master_ids.json logs/*
```

- [ ] **Step 2: Run the full orchestrator**

```bash
cd /c/Users/sjdin/media_stream_analytics
python run_phase1.py
```

Expected: All 8 steps complete successfully with timing information logged.

- [ ] **Step 3: Run validation**

```bash
cd /c/Users/sjdin/media_stream_analytics
python validate_phase1.py
```

Expected: All checks PASS:
```
  [+] MySQL users: 5000 rows
  [+] MySQL subscriptions: ~7000 rows
  [+] MySQL country distribution: Top country: India
  [+] PG content_catalogue: 2000 rows
  [+] PG ratings: ~16800 rows
  [+] PG ratings user_id format: valid IDs
  [+] PG ratings has duplicates (intended): ~800 duplicate pairs
  [+] CSV content_metadata.csv: 2000 rows
  [+] JSON viewing_events_batch.json: 50000 events
  [+] JSON schema complete: All fields present
  [+] Excel ad_campaigns.xlsx: Exists
  [+] TXT reviews: ~500 files
  [+] Image thumbnails: ~2800 files
  [+] Kafka topic exists: 'media.viewing.live' found

  VALIDATION: 14/14 checks passed
```

- [ ] **Step 4: Test Kafka producer in batch mode**

```bash
cd /c/Users/sjdin/media_stream_analytics
python -m data_generation.kafka_producer --mode batch
```

Expected: ~7500 events sent across 5 match phases, then exits.

- [ ] **Step 5: Verify Kafka messages were produced**

```bash
docker exec streaming_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic media.viewing.live \
  --from-beginning \
  --max-messages 5
```

Expected: 5 JSON events printed with all fields (event_id, user_id, content_id, session_id, device_category, event_type, etc.).

- [ ] **Step 6: Final commit**

```bash
cd /c/Users/sjdin/media_stream_analytics
git add -A
git commit -m "feat: Phase 1 data generation system complete and validated"
```
