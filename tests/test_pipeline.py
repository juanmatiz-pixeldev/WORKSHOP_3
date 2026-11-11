"""Broker-free tests: contract validation + consumer logic against SQLite."""
import json
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import func, select

import consumer
import contract
import db
import producer

ROOT = Path(__file__).resolve().parents[1]


def good_event(**over):
    ev = {"event_id": "E001", "country": "Colombia", "year": 2019, "gdp": 0.99, "family": 1.1,
          "health": 0.6, "freedom": 0.4, "generosity": 0.1, "corruption": 0.05,
          "actual_happiness_score": 6.2, "event_time": "2026-10-30T15:30:00"}
    ev.update(over)
    return ev


def msg(value, offset=0, key=b"Colombia", partition=0):
    if not isinstance(value, (bytes, type(None))):
        value = json.dumps(value).encode()
    return consumer.Incoming("happiness-events", partition, offset, key, value)


@pytest.fixture()
def env(tmp_path):
    engine = db.get_engine(f"sqlite:///{tmp_path/'t.db'}")
    db.init_db(engine)
    pred = consumer.Predictor(ROOT / "models" / "model.pkl")
    return engine, pred


# ---- contract ---------------------------------------------------------------
def test_valid_event():
    r = contract.validate_payload(json.dumps(good_event()).encode())
    assert r.status == contract.VALID and r.event["event_time"].year == 2026


@pytest.mark.parametrize("payload,status", [
    (b"not json", contract.INVALID_SCHEMA),
    (None, contract.INVALID_SCHEMA),
    (b"[1,2]", contract.INVALID_SCHEMA),
    ({k: v for k, v in good_event().items() if k != "gdp"}, contract.INVALID_SCHEMA),
    (good_event(family="high"), contract.INVALID_SCHEMA),
    (good_event(gdp=None), contract.INVALID_SCHEMA),
    (good_event(gdp=True), contract.INVALID_SCHEMA),
    (good_event(year=2019.5), contract.INVALID_SCHEMA),
    (good_event(year="2019"), contract.INVALID_SCHEMA),
    (good_event(event_time="soon"), contract.INVALID_SCHEMA),
    (good_event(event_id=7), contract.INVALID_SCHEMA),
    (good_event(gdp=-1.0), contract.INVALID_VALUES),
    (good_event(freedom=99), contract.INVALID_VALUES),
    (good_event(actual_happiness_score=11), contract.INVALID_VALUES),
    (good_event(country="   "), contract.INVALID_VALUES),
    (good_event(year=1066), contract.INVALID_VALUES),
    (b'{"event_id":"E1","country":"X","year":2019,"gdp":NaN,"family":1,"health":1,"freedom":1,'
     b'"generosity":1,"corruption":1,"actual_happiness_score":5,"event_time":"2026-01-01T00:00:00"}',
     contract.INVALID_VALUES),
])
def test_invalid_events(payload, status):
    if not isinstance(payload, (bytes, type(None))):
        payload = json.dumps(payload).encode()
    assert contract.validate_payload(payload).status == status


def test_model_features_match_contract():
    import joblib
    assert list(joblib.load(ROOT / "models" / "model.pkl").feature_names_in_) == contract.FEATURES
    assert "actual_happiness_score" not in contract.FEATURES  # no target leakage


# ---- consumer ---------------------------------------------------------------
def counts(engine):
    with engine.connect() as c:
        raw = dict(c.execute(select(db.raw_happiness_events.c.processing_status,
                                    func.count()).group_by(db.raw_happiness_events.c.processing_status)).all())
        facts = c.execute(select(func.count()).select_from(db.fact_predictions)).scalar()
    return raw, facts


def test_valid_event_is_predicted_and_traceable(env):
    engine, pred = env
    assert consumer.process_message(msg(good_event()), engine, pred, {"Colombia": "Latin America and Caribbean"}) == "VALID"
    with engine.connect() as c:
        f = c.execute(select(db.fact_predictions)).mappings().one()
        raw = c.execute(select(db.raw_happiness_events)).mappings().one()
        country = c.execute(select(db.dim_country)).mappings().one()
        date = c.execute(select(db.dim_date)).mappings().one()
    assert f["raw_event_id"] == raw["raw_event_id"] and raw["processing_status"] == "VALID"
    assert f["prediction_error"] == pytest.approx(f["predicted_score"] - f["actual_score"])
    assert f["abs_error"] == pytest.approx(abs(f["prediction_error"]))
    assert 2 < f["predicted_score"] < 9
    assert country["region"] == "Latin America and Caribbean" and date["date_id"] == 20261030
    assert json.loads(raw["raw_payload"]) == good_event()  # stored exactly as received


def test_invalid_events_are_stored_skipped_and_do_not_crash(env):
    engine, pred = env
    bad = [b"garbage{", {**good_event(event_id="B1"), "gdp": -3}, {k: v for k, v in good_event(event_id="B2").items() if k != "year"}]
    for i, b in enumerate(bad):
        consumer.process_message(msg(b if isinstance(b, bytes) else json.dumps(b).encode(), offset=i), engine, pred, {})
    assert consumer.process_message(msg(good_event(event_id="OK"), offset=10), engine, pred, {}) == "VALID"
    raw, facts = counts(engine)
    assert raw == {"INVALID_SCHEMA": 2, "INVALID_VALUES": 1, "VALID": 1} and facts == 1


def test_same_kafka_offset_redelivery_is_ignored(env):
    engine, pred = env
    consumer.process_message(msg(good_event()), engine, pred, {})
    assert consumer.process_message(msg(good_event()), engine, pred, {}) == "DUPLICATE_EVENT"
    raw, facts = counts(engine)
    assert raw == {"VALID": 1} and facts == 1


def test_same_event_id_new_offset_is_duplicate(env):
    engine, pred = env
    consumer.process_message(msg(good_event(), offset=0), engine, pred, {})
    assert consumer.process_message(msg(good_event(), offset=1), engine, pred, {}) == "DUPLICATE_EVENT"
    raw, facts = counts(engine)
    assert raw == {"VALID": 1, "DUPLICATE_EVENT": 1} and facts == 1


def test_prediction_failure_is_recorded_not_raised(env):
    engine, pred = env

    class Boom:
        version = "x"
        def predict(self, e): raise RuntimeError("model exploded")

    assert consumer.process_message(msg(good_event()), engine, Boom(), {}) == "PREDICTION_ERROR"
    with engine.connect() as c:
        row = c.execute(select(db.raw_happiness_events)).mappings().one()
    assert row["processing_status"] == "PREDICTION_ERROR" and "model exploded" in row["status_detail"]
    assert counts(engine)[1] == 0


def test_producer_events_satisfy_contract():
    df = pd.read_csv(ROOT / "data" / "streaming" / "stream_test.csv")
    for i, (_, row) in enumerate(df.head(25).iterrows(), 1):
        ev = producer.build_event(row, i, 3)
        assert contract.validate_payload(json.dumps(ev).encode()).status == "VALID"
    assert set(ev) == set(contract.REQUIRED_FIELDS)


def test_invalid_samples_are_all_rejected():
    df = pd.read_csv(ROOT / "data" / "streaming" / "stream_test.csv")
    for key, raw in producer.invalid_samples(producer.build_event(df.iloc[0], 0, 3, "X")):
        assert contract.validate_payload(raw).status in (contract.INVALID_SCHEMA, contract.INVALID_VALUES)


def test_full_replay_end_to_end(env):
    """Replay the whole held-out stream (+ invalid samples) through the consumer."""
    engine, pred = env
    df = pd.read_csv(ROOT / "data" / "streaming" / "stream_test.csv")
    off = 0
    for i, (_, row) in enumerate(df.iterrows(), 1):
        ev = producer.build_event(row, i, 3)
        consumer.process_message(msg(ev, offset=off, key=ev["country"].encode()), engine, pred, {}); off += 1
    for key, raw in producer.invalid_samples(producer.build_event(df.iloc[0], 0, 3, "X")):
        consumer.process_message(msg(raw, offset=off, key=key and key.encode()), engine, pred, {}); off += 1
    raw, facts = counts(engine)
    assert raw["VALID"] == len(df) == facts and sum(v for k, v in raw.items() if k != "VALID") == 6
