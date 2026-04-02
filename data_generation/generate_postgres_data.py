"""Generate and insert content catalogue and ratings into PostgreSQL."""

import os
import random
import sys
from datetime import datetime, timedelta
from decimal import Decimal

import numpy as np
from faker import Faker
from sqlalchemy import create_engine, text


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import NUM_CONTENT, NUM_RATINGS_PER_CONTENT_AVG, POSTGRES_URL, SEED
from data_generation.master_ids import load_master_ids


random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker(["en_IN", "en_US"])

CONTENT_TYPES = ["movie", "series", "sports", "highlight", "documentary"]
CONTENT_TYPE_PROBS = [0.50, 0.25, 0.10, 0.05, 0.10]

LANGUAGES = [
    "Hindi",
    "English",
    "Tamil",
    "Telugu",
    "Kannada",
    "Malayalam",
    "Bengali",
    "Marathi",
]
LANGUAGE_PROBS = [0.40, 0.25, 0.15, 0.10, 0.025, 0.025, 0.025, 0.025]

GENRE_POOLS = {
    "movie": ["Drama", "Thriller", "Comedy", "Romance", "Action", "Horror", "Crime", "Family"],
    "series": ["Drama", "Thriller", "Comedy", "Mystery", "Sci-Fi", "Crime", "Romance"],
    "sports": ["Sports", "Cricket", "Live"],
    "highlight": ["Sports", "Cricket", "Highlights"],
    "documentary": ["Documentary", "Nature", "History", "Science", "True Crime", "Biography"],
}

MOVIE_TITLES = [
    "Dil Ka Raasta",
    "Midnight Express",
    "The Last Over",
    "Saathi",
    "Chandni Raat",
    "Urban Legends",
    "Fire and Ice",
    "Lakshmi",
    "The Dark Hour",
    "Rangeen",
    "Kabhi Alvida",
    "Street Kings",
    "Ankhon Dekhi",
    "The Pursuit",
    "Badlapur Returns",
    "Gulaal",
    "Mumbai Diaries",
    "Pathaan Returns",
    "Tiger Zinda Hai 2",
    "Jawan 2",
]
SERIES_TITLES = [
    "The Family Man S{}",
    "Sacred Games S{}",
    "Panchayat S{}",
    "Mirzapur S{}",
    "Scam 20{}",
    "Made in Heaven S{}",
    "Kota Factory S{}",
    "Aspirants S{}",
    "Delhi Crime S{}",
    "Breathe S{}",
    "Inside Edge S{}",
    "Paatal Lok S{}",
]
SPORTS_TITLES = [
    "IPL 2025: {} vs {}",
    "IPL 2024: {} vs {}",
    "T20 World Cup: {} vs {}",
    "Asia Cup: {} vs {}",
]
IPL_TEAMS = ["MI", "CSK", "RCB", "KKR", "DC", "SRH", "RR", "PBKS", "GT", "LSG"]
HIGHLIGHT_TITLES = [
    "IPL Highlights: {} vs {}",
    "Best Catches IPL 2025",
    "Top 10 Sixes: {}",
    "Player of the Match: Match {}",
]
DOC_TITLES = [
    "Inside IPL: The Business of Cricket",
    "Wild Karnataka",
    "India from Above",
    "The Cricket Revolution",
    "Bollywood: Behind the Scenes",
    "Street Food India S{}",
    "Cosmic India",
    "The Tiger's Trail",
]

RATING_SOURCES = ["app", "web", "tv"]
RATING_SOURCE_PROBS = [0.50, 0.30, 0.20]


def _create_tables(engine):
    """Create PostgreSQL tables and indexes."""
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ratings"))
        conn.execute(text("DROP TABLE IF EXISTS content_catalogue"))
        conn.execute(
            text(
                """
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
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE ratings (
                    rating_id SERIAL PRIMARY KEY,
                    user_id VARCHAR(20),
                    content_id VARCHAR(20) REFERENCES content_catalogue(content_id),
                    rating_value DECIMAL(2,1),
                    rating_source VARCHAR(20),
                    rated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX idx_ratings_user ON ratings(user_id)"))
        conn.execute(text("CREATE INDEX idx_ratings_content ON ratings(content_id)"))
        conn.execute(text("CREATE INDEX idx_ratings_time ON ratings(rated_at)"))
        conn.execute(text("CREATE INDEX idx_ratings_pair ON ratings(user_id, content_id)"))
        conn.commit()
    print("PostgreSQL tables created: content_catalogue, ratings")


def _generate_title(content_type):
    """Generate a realistic title based on content type."""
    if content_type == "movie":
        title = f"{random.choice(MOVIE_TITLES)} ({random.randint(2015, 2025)})"
    elif content_type == "series":
        title = random.choice(SERIES_TITLES).format(random.randint(1, 5))
    elif content_type == "sports":
        team_a, team_b = random.sample(IPL_TEAMS, 2)
        title = random.choice(SPORTS_TITLES).format(team_a, team_b)
    elif content_type == "highlight":
        template = random.choice(HIGHLIGHT_TITLES)
        if template.count("{}") == 2:
            team_a, team_b = random.sample(IPL_TEAMS, 2)
            title = template.format(team_a, team_b)
        elif template.count("{}") == 1:
            title = template.format(random.choice(IPL_TEAMS))
        else:
            title = template
    else:
        template = random.choice(DOC_TITLES)
        title = template.format(random.randint(1, 4)) if "{}" in template else template

    if random.random() < 0.10:
        title = random.choice([" ", "  ", "\t"]) + title + random.choice(["", " ", "  "])
    return title


def _generate_genre(content_type):
    """Generate a genre string with inconsistent separators."""
    genres = random.sample(
        GENRE_POOLS[content_type],
        min(random.choices([1, 2, 3], weights=[40, 40, 20])[0], len(GENRE_POOLS[content_type])),
    )
    return random.choice([",", ", ", "|"]).join(genres)


def _generate_content(content_ids):
    """Generate content catalogue records."""
    content_rows = []
    for content_id in content_ids:
        content_type = random.choices(CONTENT_TYPES, CONTENT_TYPE_PROBS)[0]

        if random.random() < 0.80:
            runtime_unit = "seconds"
            if content_type == "movie":
                runtime_value = random.randint(5400, 10800)
            elif content_type == "series":
                runtime_value = random.randint(1200, 3600)
            elif content_type == "sports":
                runtime_value = random.randint(10800, 14400)
            elif content_type == "highlight":
                runtime_value = random.randint(300, 1200)
            else:
                runtime_value = random.randint(2400, 7200)
        else:
            runtime_unit = "minutes"
            if content_type == "movie":
                runtime_value = random.randint(90, 180)
            elif content_type == "series":
                runtime_value = random.randint(20, 60)
            elif content_type == "sports":
                runtime_value = random.randint(180, 240)
            elif content_type == "highlight":
                runtime_value = random.randint(5, 20)
            else:
                runtime_value = random.randint(40, 120)

        content_rows.append(
            {
                "content_id": content_id,
                "title": _generate_title(content_type),
                "content_type": content_type,
                "genre": _generate_genre(content_type),
                "runtime_value": runtime_value,
                "runtime_unit": runtime_unit,
                "release_year": random.randint(2015, 2025),
                "language": random.choices(LANGUAGES, LANGUAGE_PROBS)[0],
            }
        )

    return content_rows


def _generate_ratings(user_ids, content_ids):
    """Generate skewed ratings with duplicates and clock skew."""
    ratings = []
    total = NUM_CONTENT * NUM_RATINGS_PER_CONTENT_AVG

    for _ in range(total):
        raw = np.random.beta(5, 2) * 4 + 1
        rating_value = max(1.0, min(5.0, round(round(raw * 2) / 2, 1)))
        rated_at = datetime.now() - timedelta(days=random.randint(0, 365 * 3))
        if random.random() < 0.02:
            rated_at = datetime.now() + timedelta(days=random.randint(1, 30))

        ratings.append(
            {
                "user_id": random.choice(user_ids),
                "content_id": random.choice(content_ids),
                "rating_value": Decimal(str(rating_value)),
                "rating_source": random.choices(RATING_SOURCES, RATING_SOURCE_PROBS)[0],
                "rated_at": rated_at,
            }
        )

    for _ in range(int(total * 0.05)):
        duplicate = random.choice(ratings).copy()
        raw = np.random.beta(5, 2) * 4 + 1
        duplicate["rating_value"] = Decimal(str(max(1.0, min(5.0, round(round(raw * 2) / 2, 1)))))
        duplicate["rated_at"] = duplicate["rated_at"] + timedelta(days=random.randint(1, 60))
        ratings.append(duplicate)

    return ratings


def _insert_batch(engine, table_name, records, batch_size=1000):
    """Insert records in batches."""
    if not records:
        return

    columns = list(records[0].keys())
    placeholders = ", ".join(f":{column}" for column in columns)
    column_names = ", ".join(columns)
    statement = text(f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})")

    with engine.connect() as conn:
        for index in range(0, len(records), batch_size):
            conn.execute(statement, records[index : index + batch_size])
        conn.commit()


def generate_postgres_data():
    """Create tables, generate data, and insert it into PostgreSQL."""
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
