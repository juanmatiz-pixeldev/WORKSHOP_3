"""Model pipeline: split, build, evaluate, save."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeRegressor

from etl.harmonize import FEATURES, TARGET

RANDOM_STATE = 42
TEST_SIZE = 0.30


def split(df: pd.DataFrame):
    """70/30 split. Returns X_train, X_test, y_train, y_test and the test frame
    (with country/year/target) that is later replayed through Kafka."""
    train, test = train_test_split(df, test_size=TEST_SIZE, random_state=RANDOM_STATE)
    return train[FEATURES], test[FEATURES], train[TARGET], test[TARGET], test


def candidates() -> dict[str, Pipeline]:
    """Every candidate is a full Pipeline so scaling is learned on train only."""
    return {
        "linear_regression": Pipeline([("scaler", StandardScaler()), ("model", LinearRegression())]),
        "decision_tree": Pipeline([("scaler", StandardScaler()),
                                   ("model", DecisionTreeRegressor(max_depth=5, random_state=RANDOM_STATE))]),
        "random_forest": Pipeline([("scaler", StandardScaler()),
                                   ("model", RandomForestRegressor(n_estimators=200, random_state=RANDOM_STATE))]),
    }


def evaluate(pipe: Pipeline, X: pd.DataFrame, y: pd.Series) -> dict[str, float]:
    pred = pipe.predict(X)
    return {
        "MAE": float(mean_absolute_error(y, pred)),
        "RMSE": float(np.sqrt(mean_squared_error(y, pred))),
        "R2": float(r2_score(y, pred)),
    }


def save_model(pipe: Pipeline, name: str, metrics: dict, model_dir: Path | str = "models") -> Path:
    """Persist the fitted Pipeline (preprocessing + estimator) plus metadata."""
    model_dir = Path(model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    path = model_dir / "model.pkl"
    joblib.dump(pipe, path)
    meta = {
        "model_name": name,
        "model_version": datetime.now().strftime("%Y%m%d%H%M%S"),
        "features": FEATURES,
        "target": TARGET,
        "test_size": TEST_SIZE,
        "random_state": RANDOM_STATE,
        "metrics_test": metrics,
    }
    (model_dir / "model_metadata.json").write_text(json.dumps(meta, indent=2))
    return path
