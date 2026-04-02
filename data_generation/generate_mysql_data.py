"""Generate and insert users and subscriptions into MySQL."""

import os
import random
import sys
import uuid
from datetime import date, timedelta

from faker import Faker
from sqlalchemy import create_engine, text


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import MYSQL_URL, NUM_USERS, SEED
from data_generation.master_ids import load_master_ids


random.seed(SEED)
Faker.seed(SEED)
fake = Faker(["en_IN", "en_US", "en_GB"])

COUNTRY_WEIGHTS = {
    "India": 70,
    "United States": 10,
    "United Kingdom": 10,
    "Canada": 2,
    "Australia": 2,
    "Germany": 2,
    "UAE": 2,
    "Singapore": 2,
}
COUNTRY_LIST = list(COUNTRY_WEIGHTS.keys())
COUNTRY_PROBS = [weight / sum(COUNTRY_WEIGHTS.values()) for weight in COUNTRY_WEIGHTS.values()]

DEVICE_CATEGORIES = ["mobile", "desktop", "smart_tv"]
DEVICE_PROBS = [0.60, 0.25, 0.15]

USER_AGENTS = {
    "mobile": [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.64 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
        "JioHotstar/6.1.2 (Android 14; OnePlus 12)",
        "Android",
        "iPhone",
    ],
    "desktop": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Chrome",
        "Desktop",
    ],
    "smart_tv": [
        "Mozilla/5.0 (SMART-TV; LINUX; Tizen 7.0) AppleWebKit/537.36 (KHTML, like Gecko) Version/7.0 TV Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; BRAVIA 4K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        "Roku/DVP-13.0 (13.0.0.4100)",
        "Fire TV",
        "SmartTV",
    ],
}

PLANS = ["FREE", "BASIC", "PREMIUM", "VIP"]
PLAN_PROBS = [0.30, 0.35, 0.25, 0.10]

CANCEL_REASONS = [
    "too expensive",
    "bahut mehenga hai",
    "switched to Netflix",
    "not enough content",
    "content pasand nahi aaya",
    "poor streaming quality",
    "buffering issues",
    "free trial ended",
    "moved abroad",
    "using someone else's account",
    "paisa barbaad",
    "no IPL this season",
    "bad app experience",
    "student budget mein nahi aata",
    "Too Expensive!!!",
    None,
]

NAME_CASING_FUNCS = [
    lambda name: name,
    lambda name: name.upper(),
    lambda name: name.lower(),
    lambda name: name.swapcase(),
]


def _create_tables(engine):
    """Create MySQL tables with indexes."""
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS subscriptions"))
        conn.execute(text("DROP TABLE IF EXISTS users"))
        conn.execute(
            text(
                """
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
                """
            )
        )
        conn.execute(
            text(
                """
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
                """
            )
        )
        conn.commit()
    print("MySQL tables created: users, subscriptions")


def _generate_users(user_ids):
    """Generate user records."""
    users = []
    for user_id in user_ids:
        device_category = random.choices(DEVICE_CATEGORIES, DEVICE_PROBS)[0]
        user_agent = random.choice(USER_AGENTS[device_category])
        country = random.choices(COUNTRY_LIST, COUNTRY_PROBS)[0]

        full_name = random.choices(NAME_CASING_FUNCS, weights=[60, 15, 15, 10])[0](fake.name())

        email = fake.email()
        if random.random() < 0.10:
            local, domain = email.split("@")
            email = f"{local}+{random.choice(['test', 'work', 'jio', 'ipl', 'stream'])}@{domain}"

        users.append(
            {
                "user_id": user_id,
                "email": email,
                "full_name": full_name,
                "signup_date": fake.date_between(
                    start_date=date(2020, 1, 1),
                    end_date=date(2025, 12, 31),
                ),
                "device_category": device_category,
                "user_agent": user_agent,
                "country": country,
            }
        )

    return users


def _generate_subscriptions(users):
    """Generate subscription history."""
    subscriptions = []
    for user in users:
        num_subscriptions = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
        current_start = user["signup_date"]

        for index in range(num_subscriptions):
            start_date = current_start + timedelta(days=random.randint(0, 30))
            is_active = index == num_subscriptions - 1 and random.random() < 0.40

            if is_active:
                end_date = None
                cancel_reason = None
            else:
                end_date = start_date + timedelta(days=random.randint(30, 365))
                cancel_reason = random.choice(CANCEL_REASONS)

            subscriptions.append(
                {
                    "subscription_id": str(uuid.uuid4()),
                    "user_id": user["user_id"],
                    "plan_id": random.choices(PLANS, PLAN_PROBS)[0],
                    "start_date": start_date,
                    "end_date": end_date,
                    "cancel_reason": cancel_reason,
                }
            )

            if end_date:
                current_start = end_date

    return subscriptions


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


def generate_mysql_data():
    """Create tables, generate data, and insert it into MySQL."""
    engine = create_engine(MYSQL_URL, echo=False)
    master_ids = load_master_ids()

    print("Creating MySQL tables...")
    _create_tables(engine)

    print(f"Generating {NUM_USERS} users...")
    users = _generate_users(master_ids["user_ids"])

    print("Generating subscriptions...")
    subscriptions = _generate_subscriptions(users)

    print(f"Inserting {len(users)} users...")
    _insert_batch(engine, "users", users)

    print(f"Inserting {len(subscriptions)} subscriptions...")
    _insert_batch(engine, "subscriptions", subscriptions)

    print(f"MySQL complete: {len(users)} users, {len(subscriptions)} subscriptions")
    engine.dispose()
    return len(users), len(subscriptions)


if __name__ == "__main__":
    generate_mysql_data()
