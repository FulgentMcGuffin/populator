"""Populate yield-curve studio data into DuckDB and SQLite."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)

SRC_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from backends import DuckDBSource, SQLiteSource
from ycs.cli import parse_args
from ycs.pipeline import run_populate_pipeline

DATABASE_TARGETS = (
    (SQLiteSource, "sqlite"),
    # (DuckDBSource, "duckdb"),
)


def populate_ycs(
    source_class: type,
    backend: str,
    args,
    *,
    db_path: str | None = None,
) -> None:
    """Run the YCS populate pipeline for one backend."""
    run_populate_pipeline(source_class, backend, args, db_path=db_path)


if __name__ == "__main__":
    args = parse_args([backend for _, backend in DATABASE_TARGETS])
    for source_class, backend in tqdm(
        DATABASE_TARGETS,
        desc="Database targets",
        unit="db",
    ):
        db_path = source_class.get_full_db_path()
        tqdm.write(f"Populating YCS -> {db_path} ({backend})")
        populate_ycs(source_class, backend, args, db_path=db_path)
