-- Dashboard KPI queries. Standard SQL: runs unchanged on PostgreSQL and SQLite.
-- Each block starts with "-- name: <id>" so dashboards/app.py can load it.
-- In Power BI / Tableau / Looker Studio paste a block as a custom SQL query.

-- name: kpi1_average_prediction_error
-- MAE (mean absolute error), signed bias, and MSE (RMSE = sqrt(mse), computed in the BI layer).
SELECT COUNT(*)                                  AS n_predictions,
       AVG(abs_error)                            AS mae,
       AVG(prediction_error)                     AS mean_signed_error,
       AVG(prediction_error * prediction_error)  AS mse
FROM fact_predictions;

-- name: kpi2_predictions_by_country
SELECT c.country_name,
       c.region,
       COUNT(*)            AS n_predictions,
       AVG(f.predicted_score) AS avg_predicted,
       AVG(f.actual_score)    AS avg_actual,
       AVG(f.abs_error)       AS mae
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name, c.region
ORDER BY n_predictions DESC, c.country_name;

-- name: kpi2b_error_by_region
SELECT c.region,
       COUNT(*)         AS n_predictions,
       AVG(f.abs_error) AS mae,
       AVG(f.prediction_error) AS mean_signed_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.region
ORDER BY mae DESC;

-- name: kpi3_predicted_vs_actual
SELECT f.raw_event_id,
       c.country_name,
       f.observation_year,
       f.actual_score,
       f.predicted_score,
       f.prediction_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id;

-- name: kpi4_trend_by_observation_year
SELECT f.observation_year,
       COUNT(*)               AS n_predictions,
       AVG(f.actual_score)    AS avg_actual,
       AVG(f.predicted_score) AS avg_predicted,
       AVG(f.abs_error)       AS mae
FROM fact_predictions f
GROUP BY f.observation_year
ORDER BY f.observation_year;

-- name: kpi4b_trend_by_event_time
-- One row per prediction in arrival order; the BI layer buckets/rolls it (e.g. rolling MAE).
SELECT f.event_time,
       f.prediction_timestamp,
       f.abs_error,
       f.prediction_error
FROM fact_predictions f
ORDER BY f.event_time, f.prediction_id;

-- name: pipeline_health_by_status
SELECT processing_status, COUNT(*) AS n_events
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY n_events DESC;

-- name: pipeline_rejected_events
SELECT raw_event_id, event_id, kafka_partition, kafka_offset, processing_status, status_detail
FROM raw_happiness_events
WHERE processing_status NOT IN ('VALID', 'RECEIVED')
ORDER BY raw_event_id;
