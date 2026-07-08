"""Populate yield-curve studio data into SQLite."""

import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)

SRC_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from backends import SQLiteSource
from ycs.cli import parse_args
from ycs.pipeline import run_populate_pipeline

if __name__ == "__main__":
    db_path = SQLiteSource.get_full_db_path()
    run_populate_pipeline(
        SQLiteSource,
        "sqlite",
        parse_args("sqlite"),
        db_path=db_path,
    )
