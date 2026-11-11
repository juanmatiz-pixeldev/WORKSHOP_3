"""Loads the named queries from sql/kpis.sql and runs them against the DB."""
import os
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_URL = "postgresql+psycopg2://happiness:happiness@localhost:5432/happiness"


def load_queries(path: Path = ROOT / "sql" / "kpis.sql") -> dict[str, str]:
    blocks = re.split(r"^-- name:\s*(\w+)\s*$", path.read_text(), flags=re.M)
    return {name: sql.strip() for name, sql in zip(blocks[1::2], blocks[2::2])}


def get_engine(url: str | None = None):
    return create_engine(url or os.getenv("DATABASE_URL", DEFAULT_DB_URL))


def run(engine, name: str, queries: dict[str, str] | None = None) -> pd.DataFrame:
    queries = queries or load_queries()
    with engine.connect() as conn:
        return pd.read_sql(text(queries[name]), conn)
