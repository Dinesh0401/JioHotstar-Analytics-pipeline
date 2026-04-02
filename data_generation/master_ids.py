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
