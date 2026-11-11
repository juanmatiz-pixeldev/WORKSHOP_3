-- PostgreSQL DDL for the streaming happiness pipeline.
-- Mounted into the Postgres container by docker-compose (runs once, on first start).
-- kafka/db.py describes the same schema for SQLAlchemy/SQLite; keep both in sync.

-- RAW / STAGING: the Kafka message exactly as received (traceability, auditing, reprocessing).
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      BIGSERIAL PRIMARY KEY,
    event_id          VARCHAR(100),                 -- copied from the payload when parseable
    kafka_topic       VARCHAR(255) NOT NULL,
    kafka_partition   INTEGER      NOT NULL,
    kafka_offset      BIGINT       NOT NULL,
    kafka_key         VARCHAR(255),
    raw_payload       TEXT         NOT NULL,        -- original message value, untouched
    received_at       TIMESTAMP    NOT NULL,
    processing_status VARCHAR(30)  NOT NULL
        CHECK (processing_status IN ('RECEIVED','VALID','INVALID_SCHEMA','INVALID_VALUES',
                                     'PREDICTION_ERROR','DUPLICATE_EVENT')),
    status_detail     TEXT,                         -- validation / exception messages
    CONSTRAINT uq_raw_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ix_raw_event_id ON raw_happiness_events (event_id);
CREATE INDEX IF NOT EXISTS ix_raw_status   ON raw_happiness_events (processing_status);

-- DIMENSIONS
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(150) NOT NULL UNIQUE,
    region       VARCHAR(100)
);

-- date_id = yyyymmdd of the event (emission) date.
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INTEGER PRIMARY KEY,
    full_date DATE     NOT NULL,
    year      INTEGER  NOT NULL,
    quarter   INTEGER  NOT NULL,
    month     INTEGER  NOT NULL,
    day       INTEGER  NOT NULL
);

-- FACT: one row per successfully predicted event, linked to its raw event.
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGSERIAL PRIMARY KEY,
    raw_event_id         BIGINT  NOT NULL UNIQUE REFERENCES raw_happiness_events (raw_event_id),
    country_id           INTEGER NOT NULL REFERENCES dim_country (country_id),
    date_id              INTEGER NOT NULL REFERENCES dim_date (date_id),
    observation_year     INTEGER NOT NULL,          -- historical year of the record (degenerate dimension)
    actual_score         DOUBLE PRECISION NOT NULL,
    predicted_score      DOUBLE PRECISION NOT NULL,
    prediction_error     DOUBLE PRECISION NOT NULL, -- predicted - actual (signed)
    abs_error            DOUBLE PRECISION NOT NULL,
    event_time           TIMESTAMP NOT NULL,        -- when the producer emitted the event
    prediction_timestamp TIMESTAMP NOT NULL,        -- when the consumer produced the prediction
    model_version        VARCHAR(50)
);
CREATE INDEX IF NOT EXISTS ix_fact_country ON fact_predictions (country_id);
CREATE INDEX IF NOT EXISTS ix_fact_year    ON fact_predictions (observation_year);
