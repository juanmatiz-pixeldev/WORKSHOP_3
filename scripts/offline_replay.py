"""Broker-free demo: push the streaming set through the SAME consumer logic
(process_message) into a database, without Kafka.

Useful to try the dashboard without Docker, or to build the demo screenshots.
    python scripts/offline_replay.py --db-url sqlite:///demo.db --inject-invalid
"""
import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "kafka"))

import consumer  # noqa: E402
import db  # noqa: E402
import producer  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", default="sqlite:///demo.db")
    ap.add_argument("--source", choices=producer.SOURCES, default="test")
    ap.add_argument("--step-seconds", type=float, default=1.0, help="simulated spacing between event_time values")
    ap.add_argument("--inject-invalid", action="store_true")
    args = ap.parse_args()

    engine = db.get_engine(args.db_url)
    db.init_db(engine)
    pred = consumer.Predictor(ROOT / "models" / "model.pkl")
    regions = consumer.load_regions(ROOT / "data" / "processed" / "happiness_unified.csv")

    df = pd.read_csv(producer.SOURCES[args.source])
    width = max(3, len(str(len(df))))
    start = datetime.now().replace(microsecond=0) - timedelta(seconds=args.step_seconds * len(df))
    bad = producer.invalid_samples(producer.build_event(df.iloc[0], 0, width, "X")) if args.inject_invalid else []
    bad_at = {int(len(df) * (i + 1) / (len(bad) + 1)): b for i, b in enumerate(bad)}

    offsets = {0: 0, 1: 0, 2: 0}
    def next_msg(key, value):
        p = hash(key or b"") % 3
        offsets[p] += 1
        return consumer.Incoming(producer.TOPIC, p, offsets[p] - 1, key, value)

    counts: dict[str, int] = {}
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        ev = producer.build_event(row, i, width)
        ev["event_time"] = (start + timedelta(seconds=args.step_seconds * i)).isoformat(timespec="seconds")
        s = consumer.process_message(next_msg(ev["country"].encode(), json.dumps(ev).encode()), engine, pred, regions)
        counts[s] = counts.get(s, 0) + 1
        if i in bad_at:
            k, raw = bad_at[i]
            s = consumer.process_message(next_msg(k.encode() if k else None, raw), engine, pred, regions)
            counts[s] = counts.get(s, 0) + 1
    print("processed:", counts)


if __name__ == "__main__":
    main()
