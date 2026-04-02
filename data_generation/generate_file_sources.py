"""Generate CSV, JSON, Excel, TXT reviews, and image thumbnails."""

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

from config.settings import DATA_SOURCES_DIR, NUM_REVIEWS, NUM_VIEWING_EVENTS, SEED
from data_generation.master_ids import generate_event_id, generate_session_id, load_master_ids


random.seed(SEED)
Faker.seed(SEED)
fake = Faker(["en_IN", "en_US"])

CBFC_RATINGS = ["U", "UA", "A", "S"]
CBFC_PROBS = [0.30, 0.40, 0.25, 0.05]

EVENT_TYPES = ["PLAY", "PAUSE", "SEEK", "BUFFER_START", "BUFFER_END", "COMPLETE"]
EVENT_PROBS = [0.35, 0.20, 0.15, 0.10, 0.10, 0.10]

DEVICE_CATEGORIES = ["mobile", "desktop", "smart_tv"]
DEVICE_PROBS = [0.60, 0.25, 0.15]

REFERRERS = [
    "home_banner",
    "search",
    "recommendation",
    "deeplink",
    "push_notification",
    "social_share",
]

INDIAN_ACTORS = [
    "Shah Rukh Khan",
    "Aamir Khan",
    "Salman Khan",
    "Deepika Padukone",
    "Alia Bhatt",
    "Ranveer Singh",
    "Priyanka Chopra",
    "Akshay Kumar",
    "Ranbir Kapoor",
    "Katrina Kaif",
    "Hrithik Roshan",
    "Kareena Kapoor",
    "Vijay",
    "Rajinikanth",
    "Prabhas",
    "Allu Arjun",
    "Ram Charan",
    "Samantha Ruth Prabhu",
    "Rashmika Mandanna",
    "Tamannaah Bhatia",
    "Nawazuddin Siddiqui",
    "Pankaj Tripathi",
    "Manoj Bajpayee",
    "Radhika Apte",
    "Tabu",
    "Vidya Balan",
    "Ayushmann Khurrana",
    "Kartik Aaryan",
    "Janhvi Kapoor",
    "Ananya Panday",
]

CAMPAIGN_NAMES = [
    "IPL Season 2025 Splash",
    "Diwali Mega Sale",
    "Republic Day Special",
    "Summer Blockbuster",
    "Holi Color Burst",
    "New Year Countdown",
    "Cricket Fever",
    "Monsoon Magic",
    "Navratri Nights",
    "Back to School",
    "Valentine's Binge",
    "Independence Day Marathon",
    "Ganesh Chaturthi Special",
    "Durga Puja Stream",
    "Christmas Carnival",
    "Eid Celebrations",
    "Pongal Premiere",
    "Baisakhi Blast",
    "Women's Day Special",
    "Teacher's Day Marathon",
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

EMOJIS = [
    "\U0001F525",
    "\U0001F44D",
    "\u2764\ufe0f",
    "\U0001F3AC",
    "\u2B50",
    "\U0001F4AF",
    "\U0001F3CF",
    "\U0001F60D",
    "\U0001F44E",
    "\U0001F622",
    "\U0001F929",
    "\U0001F4AA",
]

HINDI_PHRASES = [
    "bahut badiya",
    "ekdum mast",
    "paisa vasool",
    "time waste",
    "dekhne layak",
    "must watch",
    "bekar picture",
    "kamaal ki acting",
]


def _generate_csv(content_ids):
    """Generate content_metadata.csv."""
    csv_dir = os.path.join(DATA_SOURCES_DIR, "csv")
    os.makedirs(csv_dir, exist_ok=True)
    path = os.path.join(csv_dir, "content_metadata.csv")

    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["content_id", "budget", "cast_members", "content_rating"],
        )
        writer.writeheader()

        for content_id in content_ids:
            budget = 0 if random.random() < 0.15 else random.randint(10_00_000, 300_00_00_000)
            cast_members = "|".join(random.sample(INDIAN_ACTORS, random.randint(2, 6)))
            writer.writerow(
                {
                    "content_id": content_id,
                    "budget": budget,
                    "cast_members": cast_members,
                    "content_rating": random.choices(CBFC_RATINGS, CBFC_PROBS)[0],
                }
            )

    print(f"CSV generated: {path} ({len(content_ids)} rows)")


def _generate_json_events(user_ids, content_ids):
    """Generate viewing_events_batch.json."""
    json_dir = os.path.join(DATA_SOURCES_DIR, "json")
    os.makedirs(json_dir, exist_ok=True)
    path = os.path.join(json_dir, "viewing_events_batch.json")

    events = []
    session_pool = [generate_session_id() for _ in range(NUM_VIEWING_EVENTS // 5)]

    for _ in range(NUM_VIEWING_EVENTS):
        event_type = random.choices(EVENT_TYPES, EVENT_PROBS)[0]
        device_category = random.choices(DEVICE_CATEGORIES, DEVICE_PROBS)[0]

        if random.random() < 0.05:
            session_id = None
            session_start_ts = None
        else:
            session_id = random.choice(session_pool)
            session_start_ts = (
                datetime(2025, 1, 1) + timedelta(days=random.randint(0, 180))
            ).isoformat() + "Z"

        event_dt = datetime(2025, 1, 1) + timedelta(
            days=random.randint(0, 180),
            hours=random.choices(
                range(24),
                weights=[1, 1, 1, 1, 1, 1, 2, 3, 3, 3, 4, 4, 5, 6, 8, 10, 10, 8, 6, 12, 15, 15, 10, 5],
            )[0],
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
            milliseconds=random.randint(0, 999),
        )

        if event_type == "SEEK":
            watch_duration_ms = None
            seek_position_ms = random.randint(0, 7_200_000)
            if random.random() < 0.03:
                event_type = "PLAY"
        else:
            watch_duration_ms = random.randint(1000, 7_200_000)
            seek_position_ms = None

        events.append(
            {
                "event_id": generate_event_id(),
                "user_id": random.choice(user_ids),
                "content_id": random.choice(content_ids),
                "session_id": session_id,
                "device_category": device_category,
                "event_type": event_type,
                "watch_duration_ms": watch_duration_ms,
                "seek_position_ms": seek_position_ms,
                "event_ts": event_dt.isoformat(timespec="milliseconds") + "Z",
                "session_start_ts": session_start_ts,
                "referrer": random.choice(REFERRERS),
            }
        )

    with open(path, "w", encoding="utf-8") as file:
        json.dump(events, file, indent=2)

    print(f"JSON generated: {path} ({len(events)} events)")


def _generate_excel(campaign_ids):
    """Generate ad_campaigns.xlsx."""
    excel_dir = os.path.join(DATA_SOURCES_DIR, "excel")
    os.makedirs(excel_dir, exist_ok=True)
    path = os.path.join(excel_dir, "ad_campaigns.xlsx")

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Ad Campaigns"
    worksheet.append(["campaign_id", "campaign_name", "budget", "start_date", "end_date"])

    for index, campaign_id in enumerate(campaign_ids):
        name = CAMPAIGN_NAMES[index % len(CAMPAIGN_NAMES)]
        if index >= len(CAMPAIGN_NAMES):
            name = f"{name} Vol.{index // len(CAMPAIGN_NAMES) + 1}"

        budget_value = random.randint(100_000, 50_000_000)
        budget = f"USD {budget_value}" if random.random() < 0.30 else budget_value
        start_date = fake.date_between(start_date="-1y", end_date="today")
        end_date = start_date + timedelta(days=random.randint(7, 90))
        worksheet.append([campaign_id, name, budget, start_date, end_date])

    workbook.save(path)
    print(f"Excel generated: {path} ({len(campaign_ids)} campaigns)")


def _generate_reviews(user_ids, content_ids):
    """Generate review txt files."""
    reviews_dir = os.path.join(DATA_SOURCES_DIR, "reviews")
    os.makedirs(reviews_dir, exist_ok=True)

    used_pairs = set()
    count = 0
    while count < NUM_REVIEWS:
        user_id = random.choice(user_ids)
        content_id = random.choice(content_ids)
        if (user_id, content_id) in used_pairs:
            continue
        used_pairs.add((user_id, content_id))

        paragraphs = []
        for _ in range(random.randint(1, 3)):
            paragraph = random.choice(REVIEW_TEMPLATES).format(
                type=random.choice(["movie", "show", "series", "match"]),
                name=random.choice(INDIAN_ACTORS),
                emoji=random.choice(EMOJIS),
            )
            if random.random() < 0.40:
                paragraph += f" {random.choice(HINDI_PHRASES)}"
            if random.random() < 0.15:
                paragraph += f" https://example.com/review/{random.randint(1000, 9999)}"
            paragraphs.append(paragraph)

        review_date = fake.date_between(start_date="-2y", end_date="today")
        with open(
            os.path.join(reviews_dir, f"{user_id}_{content_id}_{review_date}.txt"),
            "w",
            encoding="utf-8",
        ) as file:
            file.write("\n\n".join(paragraphs))

        count += 1

    print(f"Reviews generated: {reviews_dir}/ ({count} files)")


def _create_thumbnail(thumb_dir, content_id, version, colors):
    """Create a single placeholder thumbnail."""
    image = Image.new("RGB", (320, 180), random.choice(colors))
    draw = ImageDraw.Draw(image)

    try:
        font = ImageFont.truetype("arial.ttf", 20)
    except OSError:
        font = ImageFont.load_default()

    label = f"{content_id}\n{version}"
    bbox = draw.multiline_textbbox((0, 0), label, font=font, spacing=4, align="center")
    x = (320 - (bbox[2] - bbox[0])) // 2
    y = (180 - (bbox[3] - bbox[1])) // 2
    draw.multiline_text((x, y), label, fill=(255, 255, 255), font=font, spacing=4, align="center")
    image.save(os.path.join(thumb_dir, f"{content_id}_{version}.jpg"), "JPEG", quality=85)


def _generate_thumbnails(content_ids):
    """Generate thumbnail image files."""
    thumb_dir = os.path.join(DATA_SOURCES_DIR, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    colors = [
        (220, 50, 50),
        (50, 120, 220),
        (50, 180, 50),
        (220, 180, 50),
        (150, 50, 200),
        (50, 200, 200),
        (200, 100, 50),
        (100, 100, 100),
    ]

    for content_id in content_ids:
        _create_thumbnail(thumb_dir, content_id, "v1", colors)
        if random.random() < 0.40:
            _create_thumbnail(thumb_dir, content_id, "v2", colors)

    v1_count = len(content_ids)
    v2_count = len([name for name in os.listdir(thumb_dir) if "_v2.jpg" in name])
    print(
        "Thumbnails generated: "
        f"{thumb_dir}/ ({v1_count} v1 + {v2_count} v2 = {v1_count + v2_count} files)"
    )


def generate_file_sources():
    """Generate all file-based data sources."""
    master_ids = load_master_ids()

    print("Generating CSV...")
    _generate_csv(master_ids["content_ids"])

    print("Generating JSON viewing events...")
    _generate_json_events(master_ids["user_ids"], master_ids["content_ids"])

    print("Generating Excel ad campaigns...")
    _generate_excel(master_ids["campaign_ids"])

    print("Generating text reviews...")
    _generate_reviews(master_ids["user_ids"], master_ids["content_ids"])

    print("Generating thumbnail images...")
    _generate_thumbnails(master_ids["content_ids"])

    print("All file sources generated.")


if __name__ == "__main__":
    generate_file_sources()
