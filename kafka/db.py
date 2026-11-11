"""Database layer (SQLAlchemy Core). PostgreSQL by default, SQLite for local tests.

sql/create_tables.sql is the PostgreSQL DDL used by docker-compose; the Table
objects below describe the same schema so SQLite can create it too.
"""
from __future__ import annotations

import os
from datetime import date, datetime

from sqlalchemy import (BigInteger, Column, Date, DateTime, Float, ForeignKey, Integer, MetaData,
                        String, Table, Text, UniqueConstraint, create_engine, insert, select, update)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

DEFAULT_DB_URL = "postgresql+psycopg2://happiness:happiness@localhost:5432/happiness"

metadata = MetaData()

raw_happiness_events = Table(
    "raw_happiness_events", metadata,
    Column("raw_event_id", Integer().with_variant(BigInteger, "postgresql"), primary_key=True, autoincrement=True),
    Column("event_id", String(100)),          # copied from payload when parseable
    Column("kafka_topic", String(255), nullable=False),
    Column("kafka_partition", Integer, nullable=False),
    Column("kafka_offset", BigInteger, nullable=False),
    Column("kafka_key", String(255)),
    Column("raw_payload", Text, nullable=False),  # message value exactly as received
    Column("received_at", DateTime, nullable=False),
    Column("processing_status", String(30), nullable=False),
    Column("status_detail", Text),
    UniqueConstraint("kafka_topic", "kafka_partition", "kafka_offset", name="uq_raw_kafka_position"),
)

dim_country = Table(
    "dim_country", metadata,
    Column("country_id", Integer, primary_key=True, autoincrement=True),
    Column("country_name", String(150), nullable=False, unique=True),
    Column("region", String(100)),
)

dim_date = Table(
    "dim_date", metadata,
    Column("date_id", Integer, primary_key=True),  # yyyymmdd of event_time
    Column("full_date", Date, nullable=False),
    Column("year", Integer, nullable=False),
    Column("quarter", Integer, nullable=False),
    Column("month", Integer, nullable=False),
    Column("day", Integer, nullable=False),
)

fact_predictions = Table(
    "fact_predictions", metadata,
    Column("prediction_id", Integer().with_variant(BigInteger, "postgresql"), primary_key=True, autoincrement=True),
    Column("raw_event_id", BigInteger, ForeignKey("raw_happiness_events.raw_event_id"), nullable=False, unique=True),
    Column("country_id", Integer, ForeignKey("dim_country.country_id"), nullable=False),
    Column("date_id", Integer, ForeignKey("dim_date.date_id"), nullable=False),
    Column("observation_year", Integer, nullable=False),   # historical year of the record
    Column("actual_score", Float, nullable=False),
    Column("predicted_score", Float, nullable=False),
    Column("prediction_error", Float, nullable=False),     # predicted - actual (signed)
    Column("abs_error", Float, nullable=False),
    Column("event_time", DateTime, nullable=False),
    Column("prediction_timestamp", DateTime, nullable=False),
    Column("model_version", String(50)),
)


def get_engine(url: str | None = None) -> Engine:
    return create_engine(url or os.getenv("DATABASE_URL", DEFAULT_DB_URL), pool_pre_ping=True, future=True)


def init_db(engine: Engine) -> None:
    """Idempotent: creates missing tables (no-op when the Postgres init script already ran)."""
    metadata.create_all(engine)


# ---------------------------------------------------------------------------
def insert_raw(conn, *, topic, partition, offset, key, payload_text, received_at) -> int | None:
    """Insert the raw message BEFORE any validation. Returns raw_event_id, or None
    if this exact topic/partition/offset was already stored (redelivery)."""
    exists = conn.execute(select(raw_happiness_events.c.raw_event_id).where(
        raw_happiness_events.c.kafka_topic == topic,
        raw_happiness_events.c.kafka_partition == partition,
        raw_happiness_events.c.kafka_offset == offset)).first()
    if exists:
        return None
    res = conn.execute(insert(raw_happiness_events).values(
        kafka_topic=topic, kafka_partition=partition, kafka_offset=offset, kafka_key=key,
        raw_payload=payload_text, received_at=received_at, processing_status="RECEIVED"))
    return int(res.inserted_primary_key[0])


def set_status(conn, raw_event_id: int, status: str, detail: str | None = None, event_id: str | None = None) -> None:
    values = {"processing_status": status, "status_detail": detail}
    if event_id is not None:
        values["event_id"] = event_id[:100]
    conn.execute(update(raw_happiness_events)
                 .where(raw_happiness_events.c.raw_event_id == raw_event_id).values(**values))


def event_already_processed(conn, event_id: str) -> bool:
    return conn.execute(select(raw_happiness_events.c.raw_event_id).where(
        raw_happiness_events.c.event_id == event_id[:100],
        raw_happiness_events.c.processing_status == "VALID")).first() is not None


def get_or_create_country(conn, name: str, region: str | None) -> int:
    sel = select(dim_country.c.country_id).where(dim_country.c.country_name == name)
    row = conn.execute(sel).first()
    if row:
        return row[0]
    try:
        with conn.begin_nested():
            res = conn.execute(insert(dim_country).values(country_name=name, region=region))
            return int(res.inserted_primary_key[0])
    except IntegrityError:  # another consumer created it first
        return conn.execute(sel).first()[0]


def get_or_create_date(conn, ts: datetime) -> int:
    date_id = ts.year * 10000 + ts.month * 100 + ts.day
    if conn.execute(select(dim_date.c.date_id).where(dim_date.c.date_id == date_id)).first():
        return date_id
    try:
        with conn.begin_nested():
            conn.execute(insert(dim_date).values(
                date_id=date_id, full_date=date(ts.year, ts.month, ts.day), year=ts.year,
                quarter=(ts.month - 1) // 3 + 1, month=ts.month, day=ts.day))
    except IntegrityError:
        pass
    return date_id


def insert_prediction(conn, **values) -> int:
    res = conn.execute(insert(fact_predictions).values(**values))
    return int(res.inserted_primary_key[0])
