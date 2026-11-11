# Streaming ETL with Apache Kafka and Machine Learning — World Happiness

Workshop 3 · ETL (G01) · Universidad Autónoma de Occidente

## 1. Project description

An end-to-end pipeline that goes from five yearly World Happiness CSV files to **real-time happiness-score predictions**:

* **Offline (batch):** profile and harmonize the 2015–2019 files → feature engineering → train a regression `Pipeline` → `models/model.pkl`.
* **Streaming:** a Kafka producer replays held-out historical records as events; a consumer stores each raw event, validates it against an event contract, predicts with the serialized pipeline and stores the result in PostgreSQL.
* **Analytics:** a dashboard that queries the database (never CSVs) for the four required KPIs plus pipeline-health views.

The emphasis is pipeline integration, reliability and reproducibility, not model accuracy.

## 2. Architecture

```
OFFLINE
 data/raw/2015..2019.csv ─► etl/harmonize.py ─► data/processed/happiness_unified.csv
                                                      │
                          etl/modeling.py (70/30 split, Pipeline: StandardScaler + LinearRegression)
                                   │                                   │
                           models/model.pkl                 data/streaming/stream_test.csv (30 % held-out rows)

STREAMING
 stream_test.csv ─► kafka/producer.py ─► topic happiness-events (3 partitions, rf=1, key = country)
                                                  │
                                       kafka/consumer.py  (group happiness-ml-team)
        1 store raw ─► 2 validate contract ─► 3 dedupe ─► 4 same features/order ─► 5 model.pkl ─► 6 predict ─► 7 store
                │                                                                                              │
     raw_happiness_events  (always, with status)                       fact_predictions + dim_country + dim_date
                                                                                       │
                                                              sql/kpis.sql ─► dashboards/app.py
```

Folder layout follows the workshop recommendation, plus `etl/` (reusable batch code shared by notebooks and CLIs), `scripts/` and `tests/`:

```
data/{raw,processed,streaming}  notebooks/{eda,model_training}.ipynb  etl/  kafka/{producer,consumer,contract,db}.py
models/model.pkl  sql/{create_tables,kpis}.sql  dashboards/  docker-compose.yml  requirements.txt  tests/  scripts/
```

## 3. Data cleaning decisions

Implemented in `etl/harmonize.py`, explained step by step (with evidence) in `notebooks/eda.ipynb`.

| Problem found | Decision | Why |
|---|---|---|
| Same indicator, different names (`Economy (GDP per Capita)`, `Economy..GDP.per.Capita.`, `GDP per capita`; `Family` vs `Social support`; `Trust (Government Corruption)` vs `Perceptions of corruption`; `Country` vs `Country or region`) | Normalized alias table → `country, region, year, happiness_score, gdp, family, health, freedom, generosity, corruption`. An unknown column raises an error | One schema for batch and streaming; no silent column loss |
| Columns only in some years or derived from the target: rank, standard error, confidence interval, whiskers, dystopia residual | Dropped | Leakage (`score = Σ factors + residual`; rank is a function of the score) and not available at inference time |
| `Region` only in 2015/2016 | Country→region lookup from those files; Gambia (the only missing one) set manually | Used for analytics in `dim_country` only |
| Country spelling differs between years (6 names, e.g. `Hong Kong S.A.R., China` vs `Hong Kong`, `Trinidad & Tobago` vs `Trinidad and Tobago`) | Standardized | Country is the business key and the Kafka key |
| 1 missing value: `corruption`, United Arab Emirates 2018 | Imputed with that country's mean from the other years; flagged in `corruption_imputed` | Slow-moving indicator; 0.13 % of rows; keeps a valid target observation and stays auditable |
| Exact duplicates | None found; `(country, year)` is enforced as unique | Idempotent re-runs |
| Outliers (IQR: corruption 68, generosity 17, family 10; UAE-2018 gdp is the dataset maximum; 30 rows with a 0 in one factor) | Kept and reported | Published observations; removing them would bias the replayed stream |
| Different country sets per year (158/157/155/156/156 rows) | Kept | Real survey coverage change, not an error |

Result: **782 rows × 11 columns**, 164 countries.
Caveat: Gallup's definitions of "family/social support" and "corruption" drifted slightly between report editions; pooling assumes they are comparable.

## 4. Feature engineering decisions

* **Target:** `happiness_score`. **Features (6, in this order):** `gdp, family, health, freedom, generosity, corruption`.
* Chosen because they are present in every year, numeric, and exactly what the event contract carries.
* Correlation with the score: gdp 0.79, health 0.74, family 0.65, freedom 0.55, corruption 0.40, generosity 0.14.
* **Excluded:** rank/dystopia residual/SE/CI (leakage); `actual_happiness_score` (evaluation only); `country` (164 levels, ≤5 rows each → memorization); `year` (cannot extrapolate); `region` (not in the event).
* No categorical encoding is needed. `StandardScaler` lives **inside** the `Pipeline`, is fitted on the training split only and runs identically in the consumer.
* **Split:** 70/30 random, `random_state=42`. Compared Linear Regression, Decision Tree and Random Forest; shipped **Linear Regression** (simple, reproducible, interpretable).

| Model (test split) | MAE | RMSE | R² |
|---|---|---|---|
| **Linear Regression (shipped)** | 0.429 | 0.554 | 0.760 |
| Decision Tree (depth 5) | 0.470 | 0.605 | 0.714 |
| Random Forest (200 trees) | 0.394 | 0.502 | 0.802 |

The notebook also checks a country-grouped holdout (R² ≈ 0.78) to confirm that having the same country in train and test is not inflating the score.

## 5. Kafka pipeline and event contract

**Topic** `happiness-events`, 3 partitions, replication factor 1 (created by `kafka-init` in Docker Compose and, idempotently, by the producer). Events are keyed by `country`, so all events of a country land in one partition and keep their order.

**Producer** (`kafka/producer.py`): reads the prepared data record by record, JSON-serializes, sends with `acks=all` and a configurable `--delay`. By default it streams the **30 % held-out test rows (235 events)**, so the dashboard's error KPIs are measured on data the model never saw (`--source all` streams all 782). `event_id` is `E001…` (deterministic), `event_time` is the emission time. `--inject-invalid` adds six deliberately malformed messages to demonstrate validation.

```json
{"event_id": "E001", "country": "Colombia", "year": 2019, "gdp": 1.2, "family": 0.8, "health": 0.9,
 "freedom": 0.6, "generosity": 0.3, "corruption": 0.1, "actual_happiness_score": 6.2,
 "event_time": "2026-10-30T15:30:00"}
```

**Contract** (`kafka/contract.py`):

| Field | Rule | Violation → status |
|---|---|---|
| whole payload | UTF-8 JSON object, all 11 fields present | `INVALID_SCHEMA` |
| `event_id`, `country` | string, non-blank after trim | wrong type → `INVALID_SCHEMA`; blank → `INVALID_VALUES` |
| `year` | integer (not float/bool/string), 2000–2100 | type → `INVALID_SCHEMA`; range → `INVALID_VALUES` |
| `gdp, family, health, freedom, generosity, corruption` | JSON number (not string/bool/null), finite, 0–3 | type → `INVALID_SCHEMA`; NaN/inf/negative/out of range → `INVALID_VALUES` |
| `actual_happiness_score` | number, finite, 0–10; **never a model input** | same as above |
| `event_time` | valid ISO-8601 timestamp | `INVALID_SCHEMA` |

Bounds are deliberately loose: they reject garbage, not unusual-but-real data. Missing model features are caught by the "all fields present" rule, and at startup the consumer refuses to run if `model.feature_names_in_` differs from the contract's feature list.

**Consumer** (`kafka/consumer.py`, group `happiness-ml-team`, `auto_offset_reset=earliest`, manual commit after the DB write):

1. Insert the original message (value, key, topic/partition/offset) into `raw_happiness_events` with status `RECEIVED` — before any parsing.
2. Validate → status `VALID` / `INVALID_SCHEMA` / `INVALID_VALUES`. Invalid events are kept in the raw table, skipped and never crash the consumer.
3. `event_id` already predicted → `DUPLICATE_EVENT` (idempotency on replays).
4. Build a one-row DataFrame in training feature order, run the loaded `Pipeline`, store the fact row and mark the raw row `VALID` **in one transaction**. Any prediction exception → `PREDICTION_ERROR` with the message in `status_detail`.
5. Commit the Kafka offset. A redelivered offset is detected by a unique `(topic, partition, offset)` constraint (at-least-once delivery, effectively-once results). Infrastructure errors (e.g. database down) are *not* swallowed: the consumer stops without committing, so nothing is lost on restart.

Statuses: `RECEIVED, VALID, INVALID_SCHEMA, INVALID_VALUES, PREDICTION_ERROR, DUPLICATE_EVENT`.

## 6. Database schema (PostgreSQL; `sql/create_tables.sql`)

```
raw_happiness_events (staging)                dim_country            dim_date
  raw_event_id PK                               country_id PK          date_id PK (yyyymmdd of event_time)
  event_id, kafka_topic/partition/offset (UQ)   country_name UQ        full_date, year, quarter, month, day
  kafka_key, raw_payload (as received)          region
  received_at, processing_status, status_detail        ▲                    ▲
        ▲                                              │                    │
        └──────────── fact_predictions ────────────────┴────────────────────┘
            prediction_id PK, raw_event_id FK UQ, country_id FK, date_id FK, observation_year,
            actual_score, predicted_score, prediction_error (predicted − actual), abs_error,
            event_time, prediction_timestamp, model_version
```

* Every prediction links to the exact Kafka event through `raw_event_id`.
* `date_id` is the event-emission date; the historical year of the observation is stored in the fact as `observation_year` (degenerate dimension), because a streaming demo emits all events within minutes, so the emission date alone would give a one-point "trend".
* `kafka/db.py` defines the same schema for SQLAlchemy so SQLite can be used locally (`--db-url sqlite:///x.db`).

## 7. Dashboard

`dashboards/app.py` (Streamlit) runs the queries in `sql/kpis.sql` against the database. The same queries can be pasted as custom SQL in Power BI, Tableau or Looker Studio against the Postgres on `localhost:5432`.

| KPI | Definition |
|---|---|
| 1 · Average prediction error | **MAE** = mean(abs_error); also RMSE and bias (mean of predicted − actual; sign shows over/under-prediction) |
| 2 · Predictions by country | Count of predictions per country, coloured by that country's MAE |
| 3 · Predicted vs actual | Scatter of every prediction against the dashed perfect-prediction line |
| 4 · Trends over time | (a) average actual vs predicted by observation year; (b) rolling MAE in arrival order (`event_time`) |
| Pipeline health | Raw events by processing status and the list of rejected events with the reason |

![dashboard](dashboards/screenshots/dashboard.png)

> The screenshot was generated from `scripts/offline_replay.py` (the same `process_message` logic as the consumer, without a broker, `event_time` spaced 1 s apart, six invalid events injected) into SQLite and rendered by `dashboards/export_screenshot.py`. A live run against Kafka/Postgres gives the same numbers, since it uses the same model and held-out rows; only the timestamps differ.

## 8. Execution instructions

Requirements: Python 3.10+, Docker with Compose.

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

docker compose up -d                  # Kafka (KRaft) + PostgreSQL; creates topic + tables
docker compose ps                     # wait until kafka and postgres are healthy

# Optional: rebuild the offline artifacts (already committed)
python etl/build_dataset.py           # data/processed/happiness_unified.csv
python etl/train_model.py             # models/model.pkl + data/streaming/stream_test.csv
#   or run notebooks/eda.ipynb and notebooks/model_training.ipynb

# Terminal 1 – consumer (start first or later: it reads from the earliest offset)
python kafka/consumer.py
# Terminal 2 – producer (≈50 s at 0.2 s/event)
python kafka/producer.py --delay 0.2 --inject-invalid

# Terminal 3 – dashboard
streamlit run dashboards/app.py       # http://localhost:8501
```

Run the scripts from the project root with `python kafka/<script>.py` (the local `kafka/` folder name would shadow the Kafka client library under `python -m`).

Defaults match `docker-compose.yml`: Kafka `localhost:9092`, database `postgresql+psycopg2://happiness:happiness@localhost:5432/happiness`. Override with `--bootstrap`, `KAFKA_BOOTSTRAP`, `--db-url` or `DATABASE_URL`.

**Re-running:** event ids are deterministic, so replaying the same stream marks events `DUPLICATE_EVENT`. To start clean, run `docker compose down -v` and `up -d` again, or use `--id-prefix R2-` to replay as new events.

**Without Docker:** `python scripts/offline_replay.py --db-url sqlite:///demo.db --inject-invalid`, then `DATABASE_URL=sqlite:///demo.db streamlit run dashboards/app.py`.

**Tests** (no broker needed): `python -m pytest tests -q`. They cover the contract (valid/invalid cases), the consumer against SQLite (raw storage, status marking, no crash on bad input, duplicates, prediction failure, traceability) and a full replay of the held-out stream.

## 9. Known limitations

* The Docker/Kafka/PostgreSQL path was written to the documented settings and the DDL was syntax-checked against the PostgreSQL grammar, but **it was not executed against a live broker or database** while preparing this repo; the consumer logic itself is tested through SQLite.
* Random 70/30 split on panel data (same country, different years); the country-grouped check shows it does not materially inflate results.
* Timestamps are naive local time (as in the workshop's example); timezone-aware input is normalized to UTC.
* `event_time` values are within seconds of each other in a live run (they are emission times), so the time-trend view is most meaningful with a larger `--delay` or `--source all`.
