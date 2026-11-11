"""Batch ETL: load the five yearly World Happiness CSVs and harmonize them
into one unified analytical dataset.

Every cleaning decision is implemented here (and justified in the README and
the EDA notebook) so that notebooks and CLI scripts share a single code path.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

YEARS = [2015, 2016, 2017, 2018, 2019]

# --- Unified schema ---------------------------------------------------------
FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
TARGET = "happiness_score"
UNIFIED_COLUMNS = ["country", "region", "year", TARGET, *FEATURES, "corruption_imputed"]

UNIFIED_DTYPES = {
    "country": "string",
    "region": "string",
    "year": "int64",
    TARGET: "float64",
    **{f: "float64" for f in FEATURES},
    "corruption_imputed": "bool",
}

# Column aliases, keyed by a normalized name (lowercase, alphanumerics only).
ALIASES = {
    "country": "country",
    "countryorregion": "country",
    "region": "region",
    "happinessscore": TARGET,
    "score": TARGET,
    "economygdppercapita": "gdp",
    "gdppercapita": "gdp",
    "family": "family",
    "socialsupport": "family",
    "healthlifeexpectancy": "health",
    "healthylifeexpectancy": "health",
    "freedom": "freedom",
    "freedomtomakelifechoices": "freedom",
    "generosity": "generosity",
    "trustgovernmentcorruption": "corruption",
    "perceptionsofcorruption": "corruption",
}

# Columns present in some years that are deliberately NOT carried over:
# rank (function of the target), dystopia residual (score = sum(factors) +
# residual), standard error / CI / whiskers (uncertainty of the target itself,
# published only in some years).
DROPPED_PATTERNS = ("rank", "dystopiaresidual", "standarderror",
                    "confidenceinterval", "whisker")

COUNTRY_RENAMES = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Taiwan Province of China": "Taiwan",
    "Macedonia": "North Macedonia",
    "North Cyprus": "Northern Cyprus",
    "Somaliland Region": "Somaliland region",
    "Trinidad & Tobago": "Trinidad and Tobago",
}

# Region exists only in the 2015/2016 files. After the country-name standardization
# every country appears there except Gambia (first listed in 2019), assigned manually
# using the region names of those files.
MANUAL_REGIONS = {
    "Gambia": "Sub-Saharan Africa",
}


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def load_raw(raw_dir: Path | str) -> dict[int, pd.DataFrame]:
    raw_dir = Path(raw_dir)
    return {y: pd.read_csv(raw_dir / f"{y}.csv") for y in YEARS}


def column_mapping(df: pd.DataFrame) -> dict[str, str | None]:
    """raw column -> unified column (None = intentionally dropped)."""
    out: dict[str, str | None] = {}
    for col in df.columns:
        key = _norm(col)
        if key in ALIASES:
            out[col] = ALIASES[key]
        elif any(p in key for p in DROPPED_PATTERNS):
            out[col] = None
        else:
            raise ValueError(f"Unmapped column {col!r}: extend ALIASES or DROPPED_PATTERNS")
    return out


def harmonize_one(df: pd.DataFrame, year: int) -> pd.DataFrame:
    mapping = column_mapping(df)
    keep = {raw: new for raw, new in mapping.items() if new is not None}
    out = df[list(keep)].rename(columns=keep).copy()
    if "region" not in out:
        out["region"] = pd.NA
    out["year"] = year
    out["country"] = out["country"].astype("string").str.strip().replace(COUNTRY_RENAMES)
    for col in [TARGET, *FEATURES]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    return out[["country", "region", "year", TARGET, *FEATURES]]


def build_unified(raw: dict[int, pd.DataFrame]) -> pd.DataFrame:
    frames = [harmonize_one(df, y) for y, df in raw.items()]
    uni = pd.concat(frames, ignore_index=True)

    # 1. Duplicates on the business key (country, year).
    uni = uni.drop_duplicates(subset=["country", "year"], keep="first")

    # 2. Region: only 2015/2016 provide it -> country->region lookup.
    lookup = (uni.dropna(subset=["region"]).drop_duplicates("country")
              .set_index("country")["region"].to_dict())
    lookup.update({k: v for k, v in MANUAL_REGIONS.items() if k not in lookup})
    uni["region"] = uni["country"].map(lookup).astype("string")

    # 3. Missing values: only 'corruption' has a gap (UAE 2018). Impute with the
    #    country's mean from the other years and keep an audit flag.
    uni["corruption_imputed"] = uni["corruption"].isna()
    country_mean = uni.groupby("country")["corruption"].transform("mean")
    uni["corruption"] = uni["corruption"].fillna(country_mean)
    uni = uni.dropna(subset=[TARGET, *FEATURES])

    uni = uni.sort_values(["year", "country"]).reset_index(drop=True)
    return uni[UNIFIED_COLUMNS].astype(UNIFIED_DTYPES)


def outlier_flags(df: pd.DataFrame, cols=(TARGET, *FEATURES), k: float = 1.5) -> pd.DataFrame:
    """Boolean frame of IQR outliers per column (reported, NOT removed)."""
    flags = {}
    for c in cols:
        q1, q3 = df[c].quantile([0.25, 0.75])
        iqr = q3 - q1
        flags[c] = (df[c] < q1 - k * iqr) | (df[c] > q3 + k * iqr)
    return pd.DataFrame(flags)


def quality_report(raw: dict[int, pd.DataFrame], unified: pd.DataFrame) -> dict:
    per_year = {}
    for y, df in raw.items():
        per_year[y] = {
            "rows": int(len(df)),
            "columns": list(df.columns),
            "missing_cells": {c: int(n) for c, n in df.isna().sum().items() if n},
            "exact_duplicates": int(df.duplicated().sum()),
            "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        }
    return {
        "per_year": per_year,
        "unified_rows": int(len(unified)),
        "unified_countries": int(unified["country"].nunique()),
        "imputed_corruption_rows": int(unified["corruption_imputed"].sum()),
        "outlier_counts": {c: int(n) for c, n in outlier_flags(unified).sum().items()},
    }
