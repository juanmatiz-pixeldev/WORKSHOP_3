"""Event contract for topic `happiness-events` and its validator.

Kafka only transports bytes; the application owns the meaning of the fields.
validate_payload() never raises: every problem becomes a status + error list so
that a bad record can be stored, skipped and never crash the pipeline.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime

# Processing statuses stored in raw_happiness_events.processing_status
RECEIVED = "RECEIVED"                  # stored, not processed yet
VALID = "VALID"                        # passed the contract and was predicted
INVALID_SCHEMA = "INVALID_SCHEMA"      # bad JSON, missing field, wrong type/format
INVALID_VALUES = "INVALID_VALUES"      # right type, impossible value (NaN, negative, out of range)
PREDICTION_ERROR = "PREDICTION_ERROR"  # contract OK, model/prediction step failed
DUPLICATE_EVENT = "DUPLICATE_EVENT"    # event_id already processed (replay / redelivery)

# Model input features, IN TRAINING ORDER. Checked against the model at startup.
FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
STRING_FIELDS = ["event_id", "country"]
INT_FIELDS = ["year"]
NUMERIC_FIELDS = [*FEATURES, "actual_happiness_score"]
TIME_FIELDS = ["event_time"]
REQUIRED_FIELDS = [*STRING_FIELDS, *INT_FIELDS, *NUMERIC_FIELDS, *TIME_FIELDS]

# Deliberately loose sanity bounds: they catch garbage, not unusual-but-real data
# (observed max in training: gdp 2.10, family 1.64, health 1.14).
FEATURE_BOUNDS = (0.0, 3.0)
SCORE_BOUNDS = (0.0, 10.0)
YEAR_BOUNDS = (2000, 2100)


@dataclass
class ValidationResult:
    status: str
    errors: list[str] = field(default_factory=list)
    event: dict | None = None


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _parse_time(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:  # normalise to naive UTC so the DB column is uniform
        from datetime import timezone
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def validate_payload(payload: bytes | str | None) -> ValidationResult:
    """Validate one raw Kafka message value against the contract."""
    # --- 1. Decode + parse -------------------------------------------------
    if payload is None:
        return ValidationResult(INVALID_SCHEMA, ["empty message (null value)"])
    try:
        text = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else payload
        data = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return ValidationResult(INVALID_SCHEMA, [f"payload is not valid UTF-8 JSON: {exc}"])
    if not isinstance(data, dict):
        return ValidationResult(INVALID_SCHEMA, [f"payload must be a JSON object, got {type(data).__name__}"])

    # --- 2. Schema: required fields, types, formats -----------------------
    schema_errors: list[str] = []
    missing = [f for f in REQUIRED_FIELDS if f not in data]
    if missing:
        schema_errors.append(f"missing required fields: {missing}")
    for f in STRING_FIELDS:
        if f in data and not isinstance(data[f], str):
            schema_errors.append(f"{f} must be a string, got {type(data[f]).__name__}")
    for f in INT_FIELDS:
        if f in data and (not isinstance(data[f], int) or isinstance(data[f], bool)):
            schema_errors.append(f"{f} must be an integer, got {type(data[f]).__name__}")
    for f in NUMERIC_FIELDS:
        if f in data and not _is_number(data[f]):
            schema_errors.append(f"{f} must be numeric, got {type(data[f]).__name__}")
    event_time = None
    if "event_time" in data:
        if not isinstance(data["event_time"], str):
            schema_errors.append("event_time must be an ISO-8601 string")
        else:
            try:
                event_time = _parse_time(data["event_time"])
            except ValueError:
                schema_errors.append(f"event_time is not a valid ISO-8601 timestamp: {data['event_time']!r}")
    if schema_errors:
        return ValidationResult(INVALID_SCHEMA, schema_errors)

    # --- 3. Values ---------------------------------------------------------
    value_errors: list[str] = []
    for f in STRING_FIELDS:
        if not data[f].strip():
            value_errors.append(f"{f} must be a non-empty string")
    lo, hi = YEAR_BOUNDS
    if not lo <= data["year"] <= hi:
        value_errors.append(f"year {data['year']} outside [{lo}, {hi}]")
    for f in NUMERIC_FIELDS:
        v = data[f]
        if not math.isfinite(v):
            value_errors.append(f"{f} must be finite, got {v}")
            continue
        lo, hi = SCORE_BOUNDS if f == "actual_happiness_score" else FEATURE_BOUNDS
        if not lo <= v <= hi:
            value_errors.append(f"{f}={v} outside [{lo}, {hi}]")
    if value_errors:
        return ValidationResult(INVALID_VALUES, value_errors)

    event = dict(data)
    event["country"] = data["country"].strip()
    event["event_time"] = event_time
    return ValidationResult(VALID, [], event)
