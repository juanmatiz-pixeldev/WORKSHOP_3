"""Kafka consumer: raw store -> validate -> preprocess -> predict -> store.

Run from the project root:  python kafka/consumer.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # sibling modules (contract, db)
import contract  # noqa: E402
import db  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"
log = logging.getLogger("consumer")


@dataclass
class Incoming:
    """Transport-agnostic view of a Kafka record (lets us unit-test without a broker)."""
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class Predictor:
    """Loads the serialized training pipeline and applies it to one event."""

    def __init__(self, model_path: Path, metadata_path: Path | None = None):
        self.pipeline = joblib.load(model_path)
        trained_on = list(getattr(self.pipeline, "feature_names_in_", []))
        if trained_on != contract.FEATURES:  # fail fast: this is a deployment error, not a bad event
            raise RuntimeError(f"Model expects features {trained_on} but the contract provides {contract.FEATURES}")
        meta_file = metadata_path or model_path.with_name("model_metadata.json")
        self.version = json.loads(meta_file.read_text()).get("model_version") if meta_file.exists() else None

    def predict(self, event: dict) -> float:
        # Same feature ordering as training; scaling happens inside the Pipeline.
        X = pd.DataFrame([[event[f] for f in contract.FEATURES]], columns=contract.FEATURES)
        y = float(self.pipeline.predict(X)[0])
        if y != y or y in (float("inf"), float("-inf")):
            raise ValueError(f"model returned non-finite prediction: {y}")
        return y


def load_regions(path: Path) -> dict[str, str]:
    """country -> region from the unified training dataset (for dim_country)."""
    if not path.exists():
        return {}
    df = pd.read_csv(path, usecols=["country", "region"]).drop_duplicates("country")
    return dict(zip(df["country"], df["region"]))


def process_message(msg: Incoming, engine, predictor: Predictor, regions: dict[str, str]) -> str:
    """Handle ONE message and return its final processing status.

    Business problems (bad payloads, prediction failures) are recorded, never raised.
    Infrastructure problems (DB down) propagate so the offset is NOT committed.
    """
    payload_text = (msg.value or b"").decode("utf-8", errors="replace")
    key_text = msg.key.decode("utf-8", errors="replace") if msg.key else None

    # 1. Store the raw event first, exactly as received.
    with engine.begin() as conn:
        raw_id = db.insert_raw(conn, topic=msg.topic, partition=msg.partition, offset=msg.offset,
                               key=key_text, payload_text=payload_text, received_at=datetime.now())
    if raw_id is None:
        log.info("redelivery of %s[%d]@%d ignored", msg.topic, msg.partition, msg.offset)
        return contract.DUPLICATE_EVENT

    # 2. Validate against the event contract.
    result = contract.validate_payload(msg.value)
    event_id = None
    try:
        parsed = json.loads(payload_text)
        if isinstance(parsed, dict) and isinstance(parsed.get("event_id"), str):
            event_id = parsed["event_id"]
    except ValueError:
        pass
    if result.status != contract.VALID:
        with engine.begin() as conn:
            db.set_status(conn, raw_id, result.status, "; ".join(result.errors), event_id)
        log.warning("%s event_id=%s: %s", result.status, event_id, result.errors)
        return result.status

    event = result.event
    # The Kafka key must equal the country (contract). Mismatch is logged, not fatal.
    if key_text is not None and key_text != event["country"]:
        log.warning("event %s: kafka key %r != country %r", event["event_id"], key_text, event["country"])

    # 3. Idempotency on event_id.
    with engine.begin() as conn:
        if db.event_already_processed(conn, event["event_id"]):
            db.set_status(conn, raw_id, contract.DUPLICATE_EVENT,
                          f"event_id {event['event_id']} already processed", event["event_id"])
            return contract.DUPLICATE_EVENT

    # 4. Predict + 5. store (one transaction: no raw=VALID without its fact row).
    try:
        predicted = predictor.predict(event)
        actual = float(event["actual_happiness_score"])
        with engine.begin() as conn:
            country_id = db.get_or_create_country(conn, event["country"], regions.get(event["country"]))
            date_id = db.get_or_create_date(conn, event["event_time"])
            db.insert_prediction(
                conn, raw_event_id=raw_id, country_id=country_id, date_id=date_id,
                observation_year=event["year"], actual_score=actual, predicted_score=predicted,
                prediction_error=predicted - actual, abs_error=abs(predicted - actual),
                event_time=event["event_time"], prediction_timestamp=datetime.now(),
                model_version=predictor.version)
            db.set_status(conn, raw_id, contract.VALID, None, event["event_id"])
        return contract.VALID
    except Exception as exc:  # noqa: BLE001 - prediction/transform problems must not kill the stream
        log.exception("prediction failed for event %s", event["event_id"])
        with engine.begin() as conn:
            db.set_status(conn, raw_id, contract.PREDICTION_ERROR, f"{type(exc).__name__}: {exc}", event["event_id"])
        return contract.PREDICTION_ERROR


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--group-id", default=GROUP_ID)
    ap.add_argument("--db-url", default=None, help="SQLAlchemy URL (default: $DATABASE_URL or local Postgres)")
    ap.add_argument("--model", default=str(ROOT / "models" / "model.pkl"))
    ap.add_argument("--idle-timeout", type=float, default=0,
                    help="exit after N seconds without messages (0 = run forever)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from kafka import KafkaConsumer  # imported here so tests don't need a broker

    engine = db.get_engine(args.db_url)
    db.init_db(engine)
    predictor = Predictor(Path(args.model))
    regions = load_regions(ROOT / "data" / "processed" / "happiness_unified.csv")

    consumer = KafkaConsumer(
        args.topic, bootstrap_servers=args.bootstrap, group_id=args.group_id,
        auto_offset_reset="earliest", enable_auto_commit=False,   # commit only after the DB write
        consumer_timeout_ms=int(args.idle_timeout * 1000) if args.idle_timeout else float("inf"))
    log.info("consuming %s as group %s (model version %s)", args.topic, args.group_id, predictor.version)

    counts: dict[str, int] = {}
    try:
        for rec in consumer:
            status = process_message(Incoming(rec.topic, rec.partition, rec.offset, rec.key, rec.value),
                                     engine, predictor, regions)
            consumer.commit()
            counts[status] = counts.get(status, 0) + 1
            log.info("p%d@%d key=%s -> %s", rec.partition, rec.offset, rec.key and rec.key.decode(), status)
    except KeyboardInterrupt:
        log.info("interrupted")
    finally:
        consumer.close()
        log.info("summary: %s", counts)


if __name__ == "__main__":
    main()
