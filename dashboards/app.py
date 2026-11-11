"""Streamlit dashboard. Every number comes from SQL queries in sql/kpis.sql
executed against the prediction database (never from CSV files).

    DATABASE_URL=postgresql+psycopg2://happiness:happiness@localhost:5432/happiness \
        streamlit run dashboards/app.py
"""
import os
import sys
from pathlib import Path

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
import kpi_queries as kq  # noqa: E402

st.set_page_config(page_title="Happiness streaming predictions", layout="wide")
st.title("World Happiness – real-time prediction monitor")

db_url = st.sidebar.text_input("Database URL", os.getenv("DATABASE_URL", kq.DEFAULT_DB_URL))
top_n = st.sidebar.slider("Countries shown", 5, 40, 15)
window = st.sidebar.slider("Rolling window (events)", 5, 100, 25)
st.sidebar.button("Refresh")

queries = kq.load_queries()
engine = kq.get_engine(db_url)


def q(name):
    return kq.run(engine, name, queries)


try:
    k1 = q("kpi1_average_prediction_error").iloc[0]
except Exception as exc:  # noqa: BLE001
    st.error(f"Cannot query the database: {exc}")
    st.stop()

if not k1["n_predictions"]:
    st.info("No predictions yet. Start the producer and consumer, then refresh.")
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Predictions", int(k1["n_predictions"]))
c2.metric("KPI 1 · Mean absolute error", f"{k1['mae']:.3f}")
c3.metric("RMSE", f"{np.sqrt(k1['mse']):.3f}")
c4.metric("Bias (pred − actual)", f"{k1['mean_signed_error']:+.3f}")

left, right = st.columns(2)
with left:
    st.subheader("KPI 2 · Predictions by country")
    by_country = q("kpi2_predictions_by_country").head(top_n)
    st.plotly_chart(px.bar(by_country, x="n_predictions", y="country_name", orientation="h",
                           color="mae", color_continuous_scale="Reds",
                           labels={"n_predictions": "Predictions", "country_name": "", "mae": "MAE"})
                    .update_yaxes(autorange="reversed"), width="stretch")
with right:
    st.subheader("KPI 3 · Predicted vs actual score")
    pva = q("kpi3_predicted_vs_actual")
    fig = px.scatter(pva, x="actual_score", y="predicted_score", color="observation_year",
                     hover_data=["country_name"], labels={"actual_score": "Actual", "predicted_score": "Predicted"})
    lo, hi = pva[["actual_score", "predicted_score"]].min().min(), pva[["actual_score", "predicted_score"]].max().max()
    fig.add_trace(go.Scatter(x=[lo, hi], y=[lo, hi], mode="lines", name="perfect", line=dict(dash="dash", color="grey")))
    st.plotly_chart(fig, width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("KPI 4a · Trend by observation year")
    t = q("kpi4_trend_by_observation_year")
    fig = go.Figure()
    fig.add_scatter(x=t["observation_year"], y=t["avg_actual"], name="Avg actual", mode="lines+markers")
    fig.add_scatter(x=t["observation_year"], y=t["avg_predicted"], name="Avg predicted", mode="lines+markers")
    fig.update_xaxes(dtick=1)
    st.plotly_chart(fig, width="stretch")
with right:
    st.subheader("KPI 4b · Rolling MAE as events arrive")
    s = q("kpi4b_trend_by_event_time")
    s["rolling_mae"] = s["abs_error"].rolling(window, min_periods=1).mean()
    st.plotly_chart(px.line(s, x="event_time", y="rolling_mae", labels={"rolling_mae": f"MAE (last {window})"}),
                    width="stretch")

st.subheader("Pipeline health")
h1, h2 = st.columns([1, 2])
h1.dataframe(q("pipeline_health_by_status"), hide_index=True)
rej = q("pipeline_rejected_events")
h2.dataframe(rej if len(rej) else rej.head(0), hide_index=True)
