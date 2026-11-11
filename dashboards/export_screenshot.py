"""Renders the dashboard KPIs (same SQL as the live dashboard) to a PNG.

    python dashboards/export_screenshot.py --db-url sqlite:///demo.db
"""
import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import kpi_queries as kq  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", default=None)
    ap.add_argument("--out", default=str(kq.ROOT / "dashboards" / "screenshots" / "dashboard.png"))
    args = ap.parse_args()
    eng, qs = kq.get_engine(args.db_url), kq.load_queries()
    run = lambda n: kq.run(eng, n, qs)  # noqa: E731

    k1 = run("kpi1_average_prediction_error").iloc[0]
    by_c = run("kpi2_predictions_by_country").head(12)
    pva = run("kpi3_predicted_vs_actual")
    trend = run("kpi4_trend_by_observation_year")
    ev = run("kpi4b_trend_by_event_time")
    health = run("pipeline_health_by_status")

    fig = plt.figure(figsize=(16, 10), constrained_layout=True)
    gs = fig.add_gridspec(3, 2, height_ratios=[0.25, 1, 1])
    ax = fig.add_subplot(gs[0, :]); ax.axis("off")
    tiles = [("Predictions", f"{int(k1.n_predictions)}"), ("KPI 1 · MAE", f"{k1.mae:.3f}"),
             ("RMSE", f"{np.sqrt(k1.mse):.3f}"), ("Bias (pred − actual)", f"{k1.mean_signed_error:+.3f}"),
             ("Rejected events", f"{int(health[~health.processing_status.eq('VALID')].n_events.sum())}")]
    for i, (lab, val) in enumerate(tiles):
        ax.text(0.1 + i * 0.2, 0.6, val, ha="center", fontsize=26, weight="bold")
        ax.text(0.1 + i * 0.2, 0.15, lab, ha="center", fontsize=12, color="#555")
    ax.set_title("World Happiness – streaming prediction dashboard", fontsize=16, weight="bold", loc="left")

    a = fig.add_subplot(gs[1, 0])
    a.barh(by_c.country_name[::-1], by_c.n_predictions[::-1], color="#c0392b")
    a.set_title("KPI 2 · Predictions by country (top 12)"); a.set_xlabel("Predictions")

    b = fig.add_subplot(gs[1, 1])
    sc = b.scatter(pva.actual_score, pva.predicted_score, c=pva.observation_year, cmap="viridis", s=18, alpha=.8)
    lo, hi = pva.actual_score.min(), pva.actual_score.max()
    b.plot([lo, hi], [lo, hi], "--", color="grey"); b.set_title("KPI 3 · Predicted vs actual score")
    b.set_xlabel("Actual"); b.set_ylabel("Predicted"); fig.colorbar(sc, ax=b, label="observation year", ticks=[2015, 2016, 2017, 2018, 2019])

    c = fig.add_subplot(gs[2, 0])
    c.plot(trend.observation_year, trend.avg_actual, "o-", label="Avg actual")
    c.plot(trend.observation_year, trend.avg_predicted, "s-", label="Avg predicted")
    c.set_xticks(trend.observation_year); c.legend(); c.set_title("KPI 4a · Trend by observation year")

    d = fig.add_subplot(gs[2, 1])
    d.plot(range(len(ev)), ev.abs_error.rolling(25, min_periods=1).mean(), color="#2c3e50")
    d.set_title("KPI 4b · Rolling MAE (25 events) in arrival order"); d.set_xlabel("event #")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=110)
    print("saved", args.out)


if __name__ == "__main__":
    main()
