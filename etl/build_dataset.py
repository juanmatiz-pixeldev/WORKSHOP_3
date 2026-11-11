"""CLI: raw CSVs -> data/processed/happiness_unified.csv + quality report."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl.harmonize import build_unified, load_raw, quality_report  # noqa: E402


def main() -> None:
    raw = load_raw(ROOT / "data" / "raw")
    unified = build_unified(raw)
    out = ROOT / "data" / "processed"
    out.mkdir(parents=True, exist_ok=True)
    unified.to_csv(out / "happiness_unified.csv", index=False)
    (out / "quality_report.json").write_text(json.dumps(quality_report(raw, unified), indent=2))
    print(f"Wrote {len(unified)} rows -> {out / 'happiness_unified.csv'}")


if __name__ == "__main__":
    main()
