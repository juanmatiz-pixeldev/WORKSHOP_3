"""CLI: unified dataset -> trained pipeline (models/model.pkl) + streaming set."""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl.harmonize import FEATURES, TARGET  # noqa: E402
from etl.modeling import candidates, evaluate, save_model, split  # noqa: E402


def main() -> None:
    df = pd.read_csv(ROOT / "data" / "processed" / "happiness_unified.csv")
    X_tr, X_te, y_tr, y_te, test_df = split(df)

    results = {}
    fitted = {}
    for name, pipe in candidates().items():
        pipe.fit(X_tr, y_tr)
        fitted[name] = pipe
        results[name] = {"train": evaluate(pipe, X_tr, y_tr), "test": evaluate(pipe, X_te, y_te)}
        print(f"{name:18s} test {results[name]['test']}")

    # Workshop focus is pipeline integration, not optimisation: ship the simple,
    # interpretable linear regression.
    chosen = "linear_regression"
    save_model(fitted[chosen], chosen, results[chosen]["test"], ROOT / "models")
    (ROOT / "models" / "evaluation_all_models.json").write_text(json.dumps(results, indent=2))

    # Held-out rows are what the producer replays (never seen during training).
    stream = ROOT / "data" / "streaming"
    stream.mkdir(parents=True, exist_ok=True)
    test_df.sort_values(["year", "country"]).to_csv(stream / "stream_test.csv", index=False)
    print(f"Saved models/model.pkl; {len(test_df)} held-out rows -> data/streaming/stream_test.csv")


if __name__ == "__main__":
    main()
