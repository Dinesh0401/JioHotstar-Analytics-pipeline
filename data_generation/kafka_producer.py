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
from data_generation.master_ids import generate_event_id, generate_session_id, load_master_ids


random.seed(SEED)

EVENT_TYPES = ["PLAY", "PAUSE", "SEEK", "BUFFER_START", "BUFFER_END", "COMPLETE"]
REFERRERS = [
    "home_banner",
    "search",
    "recommendation",
    "deeplink",
    "push_notification",
    "social_share",
]
DEVICE_CATEGORIES = ["mobile", "desktop", "smart_tv"]
DEVICE_PROBS = [0.60, 0.25, 0.15]

MATCH_PHASES = [
    {"name": "pre_match", "duration_min": 30, "events": 500, "weights": [0.55, 0.15, 0.05, 0.10, 0.10, 0.05]},
    {"name": "innings_1", "duration_min": 90, "events": 2500, "weights": [0.35, 0.15, 0.15, 0.12, 0.12, 0.11]},
    {"name": "mid_innings_break", "duration_min": 20, "events": 500, "weights": [0.10, 0.50, 0.05, 0.05, 0.05, 0.25]},
    {"name": "innings_2", "duration_min": 90, "events": 3000, "weights": [0.30, 0.12, 0.15, 0.18, 0.15, 0.10]},
    {"name": "post_match", "duration_min": 30, "events": 1000, "weights": [0.15, 0.10, 0.05, 0.05, 0.05, 0.60]},
]

_running = True


def _signal_handler(sig, frame):
    del sig, frame
    global _running
    print("\nShutting down gracefully...")
    _running = False


signal.signal(signal.SIGINT, _signal_handler)


def _create_producer():
    """Create Kafka producer with retries."""
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8") if key else None,
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            if attempt == 9:
                raise
            print(f"Kafka not ready, retrying ({attempt + 1}/10)...")
            time.sleep(3)


def _generate_event(user_ids, content_ids, session_pool, phase_weights, phase_time_base, duration_min):
    """Generate a single streaming event."""
    event_type = random.choices(EVENT_TYPES, phase_weights)[0]
    device_category = random.choices(DEVICE_CATEGORIES, DEVICE_PROBS)[0]

    if random.random() < 0.10:
        session_id = random.choice(session_pool) if session_pool else generate_session_id()
    else:
        session_id = generate_session_id()
        session_pool.append(session_id)

    if len(session_pool) > 5000:
        session_pool.pop(0)

    event_dt = phase_time_base + timedelta(minutes=random.uniform(0, duration_min))
    event_dt += timedelta(seconds=random.uniform(-30, 30))

    if event_type == "SEEK":
        watch_duration_ms = None
        seek_position_ms = random.randint(0, 7_200_000)
        if random.random() < 0.03:
            event_type = "PLAY"
    else:
        watch_duration_ms = random.randint(1000, 7_200_000)
        seek_position_ms = None

    return {
        "event_id": generate_event_id(),
        "user_id": random.choice(user_ids),
        "content_id": random.choice(content_ids),
        "session_id": session_id,
        "device_category": device_category,
        "event_type": event_type,
        "watch_duration_ms": watch_duration_ms,
        "seek_position_ms": seek_position_ms,
        "event_ts": event_dt.isoformat(timespec="milliseconds") + "Z",
        "session_start_ts": phase_time_base.isoformat(timespec="milliseconds") + "Z",
        "referrer": random.choice(REFERRERS),
    }


def run_batch_mode(producer, user_ids, content_ids):
    """Produce one full IPL match simulation, then exit."""
    print("=== BATCH MODE: Simulating one IPL match day ===")
    session_pool = []
    total_sent = 0
    phase_time = datetime.now().replace(hour=19, minute=30, second=0, microsecond=0)

    for phase in MATCH_PHASES:
        print(f"\n  Phase: {phase['name']} ({phase['events']} events, {phase['duration_min']} min)")
        for _ in range(phase["events"]):
            if not _running:
                break

            event = _generate_event(
                user_ids,
                content_ids,
                session_pool,
                phase["weights"],
                phase_time,
                phase["duration_min"],
            )
            producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
            total_sent += 1

            if total_sent % 500 == 0:
                print(f"    Sent {total_sent} events...")

            time.sleep(random.uniform(0.01, 0.03))

        phase_time += timedelta(minutes=phase["duration_min"])
        if not _running:
            break

    producer.flush()
    print(f"\n=== BATCH COMPLETE: {total_sent} events sent ===")


def run_live_mode(producer, user_ids, content_ids):
    """Continuously produce events until interrupted."""
    print("=== LIVE MODE: Continuous IPL streaming (Ctrl+C to stop) ===")
    session_pool = []
    total_sent = 0
    phase_index = 0
    match_start = datetime.now().replace(hour=19, minute=30, second=0, microsecond=0)

    while _running:
        phase = MATCH_PHASES[phase_index % len(MATCH_PHASES)]
        phase_time = match_start + timedelta(
            minutes=sum(item["duration_min"] for item in MATCH_PHASES[: phase_index % len(MATCH_PHASES)])
        )

        print(f"\n  Phase: {phase['name']}")
        for _ in range(phase["events"]):
            if not _running:
                break

            event = _generate_event(
                user_ids,
                content_ids,
                session_pool,
                phase["weights"],
                phase_time,
                phase["duration_min"],
            )
            producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
            total_sent += 1

            if total_sent % 500 == 0:
                print(f"    Sent {total_sent} events...")

            time.sleep(random.uniform(0.1, 0.3))

        phase_index += 1

    producer.flush()
    print(f"\n=== LIVE MODE STOPPED: {total_sent} events sent ===")


def main():
    """Parse CLI arguments and run the requested producer mode."""
    parser = argparse.ArgumentParser(description="Kafka IPL streaming event producer")
    parser.add_argument(
        "--mode",
        choices=["batch", "live"],
        default="batch",
        help="batch: ~7500 events then exit | live: continuous until Ctrl+C",
    )
    args = parser.parse_args()

    master_ids = load_master_ids()
    producer = _create_producer()

    if args.mode == "batch":
        run_batch_mode(producer, master_ids["user_ids"], master_ids["content_ids"])
    else:
        run_live_mode(producer, master_ids["user_ids"], master_ids["content_ids"])

    producer.close()


if __name__ == "__main__":
    main()
