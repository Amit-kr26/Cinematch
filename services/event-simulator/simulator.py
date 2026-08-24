# services/event-simulator/simulator.py
"""
Produces synthetic user events to Kafka topic `user-events`.

Event schema (JSON, per Kafka message value):
  {
    "user_id":    int,
    "movie_id":   int,
    "event_type": "click" | "view" | "rating",
    "rating":     float,   # only present when event_type="rating"
    "timestamp":  str      # ISO-8601 UTC
  }

Kafka producer config:
  - Partition key: str(user_id) — ensures per-user ordering within a partition
  - retries=5, acks="all" (at-least-once; idempotence NOT enabled — duplicates
    are harmless because every downstream write is idempotent)

Package: kafka-python-ng (drop-in for kafka-python — imports as `from kafka import ...`)
"""
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "user-events")
DATA_DIR      = os.getenv("DATA_DIR", "/data")
EVENT_RATE    = float(os.getenv("EVENT_RATE", "5"))   # events per second

EVENT_TYPES   = ["click", "view", "rating"]
EVENT_WEIGHTS = [0.50, 0.35, 0.15]   # click most common, rating least


def load_user_movie_pairs(data_dir: str = DATA_DIR) -> list[tuple[int, int]]:
    """Load (user_id, movie_id) pairs from MovieLens 1M ratings.dat."""
    path = os.path.join(data_dir, "ml-1m", "ratings.dat")
    pairs: list[tuple[int, int]] = []
    with open(path) as f:
        for line in f:
            parts = line.strip().split("::")
            pairs.append((int(parts[0]), int(parts[1])))
    return pairs


def make_event(user_id: int, movie_id: int) -> dict:
    """Build a single event dict matching the schema above."""
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    event: dict = {
        "user_id":    user_id,
        "movie_id":   movie_id,
        "event_type": event_type,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    if event_type == "rating":
        # Sample rating from realistic distribution (skewed toward 3-5 stars)
        event["rating"] = float(
            random.choices([1, 2, 3, 4, 5], weights=[5, 10, 25, 35, 25])[0]
        )
    return event


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        retries=5,
        acks="all",
    )


def run(pairs: list[tuple[int, int]], producer: KafkaProducer,
        event_rate: float = EVENT_RATE, max_events: int = 0) -> int:
    """
    Emit events in a loop. Returns number of events produced.
    max_events=0 means run forever (until KeyboardInterrupt).
    """
    interval = 1.0 / event_rate
    count = 0
    while True:
        user_id, movie_id = random.choice(pairs)
        event = make_event(user_id, movie_id)
        producer.send(KAFKA_TOPIC, key=user_id, value=event)
        print(f"Produced event user_id={user_id} movie_id={movie_id} "
              f"type={event['event_type']}")
        count += 1
        if max_events and count >= max_events:
            break
        time.sleep(interval)
    producer.flush()
    return count


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Movie RecSys event simulator")
    parser.add_argument("--users",  type=int, default=0,
                        help="Limit to first N unique users (0=all)")
    parser.add_argument("--events", type=int, default=0,
                        help="Max events to produce (0=infinite)")
    args = parser.parse_args()

    pairs = load_user_movie_pairs()
    if args.users:
        seen: set[int] = set()
        filtered: list[tuple[int, int]] = []
        for u, m in pairs:
            seen.add(u)
            filtered.append((u, m))
            if len(seen) >= args.users:
                break
        pairs = filtered

    print(f"Loaded {len(pairs)} user-movie pairs. Rate={EVENT_RATE} events/s")
    producer = make_producer()
    try:
        run(pairs, producer, max_events=args.events)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
