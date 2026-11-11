"""Kafka producer: replays prepared historical records as simulated events.

Run from the project root:  python kafka/producer.py --delay 0.2
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "happiness-events"
PARTITIONS = 3
REPLICATION = 1
SOURCES = {
    "test": ROOT / "data" / "streaming" / "stream_test.csv",      # held-out 30% (default)
    "all": ROOT / "data" / "processed" / "happiness_unified.csv",  # every record
}
log = logging.getLogger("producer")


def build_event(row: pd.Series, index: int, width: int, prefix: str = "E") -> dict:
    """Map one unified-schema record to the contract's JSON event."""
    return {
        "event_id": f"{prefix}{index:0{width}d}",
        "country": str(row["country"]),
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now().isoformat(timespec="seconds"),  # emission time
    }


def invalid_samples(template: dict) -> list[tuple[str | None, bytes]]:
    """Deliberately broken messages to demo the validator (--inject-invalid)."""
    def enc(d): return json.dumps(d).encode()
    missing = {k: v for k, v in template.items() if k != "gdp"}
    wrong_type = {**template, "family": "high"}
    negative = {**template, "gdp": -4.2}
    bad_time = {**template, "event_time": "yesterday-ish"}
    empty_country = {**template, "country": "  "}
    return [
        (template["country"], enc({**missing, "event_id": "BAD-MISSING-FIELD"})),
        (template["country"], enc({**wrong_type, "event_id": "BAD-WRONG-TYPE"})),
        (template["country"], enc({**negative, "event_id": "BAD-NEGATIVE-GDP"})),
        (template["country"], enc({**bad_time, "event_id": "BAD-EVENT-TIME"})),
        ("blank", enc({**empty_country, "event_id": "BAD-EMPTY-COUNTRY"})),
        (None, b"this is not json {{{"),
    ]


def ensure_topic(bootstrap: str, topic: str) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=PARTITIONS, replication_factor=REPLICATION)])
        log.info("created topic %s (%d partitions, rf=%d)", topic, PARTITIONS, REPLICATION)
    except TopicAlreadyExistsError:
        log.info("topic %s already exists", topic)
    finally:
        admin.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--source", choices=SOURCES, default="test")
    ap.add_argument("--delay", type=float, default=0.2, help="seconds between events")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    ap.add_argument("--id-prefix", default="E", help="event_id prefix (change it to replay as NEW events)")
    ap.add_argument("--inject-invalid", action="store_true", help="also send a few malformed events")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from kafka import KafkaProducer

    df = pd.read_csv(SOURCES[args.source])
    if args.limit:
        df = df.head(args.limit)
    width = max(3, len(str(len(df))))

    ensure_topic(args.bootstrap, args.topic)
    # No serializers: we hand Kafka bytes so deliberately malformed payloads pass through untouched.
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", retries=5)

    bad = invalid_samples(build_event(df.iloc[0], 0, width, "X")) if args.inject_invalid else []
    bad_positions = {int(len(df) * (i + 1) / (len(bad) + 1)): b for i, b in enumerate(bad)}

    sent = 0
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        event = build_event(row, i, width, args.id_prefix)
        meta = producer.send(args.topic, key=event["country"].encode("utf-8"),
                             value=json.dumps(event).encode("utf-8")).get(timeout=30)
        sent += 1
        log.info("sent %s %s %d -> partition %d offset %d", event["event_id"], event["country"],
                 event["year"], meta.partition, meta.offset)
        if i in bad_positions:
            key, raw = bad_positions[i]
            producer.send(args.topic, key=key.encode("utf-8") if key else None, value=raw)
            log.info("sent deliberately invalid message (%d bytes)", len(raw))
        time.sleep(args.delay)
    producer.flush()
    log.info("done: %d events sent", sent)


if __name__ == "__main__":
    main()
