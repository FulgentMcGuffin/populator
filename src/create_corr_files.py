"""Create melted correlation pickle files from database rate tables.

Reads zero_rates and par_rates from the configured database backend and writes
Pearson and distance-correlation pickle files to the derived data directory.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from backends import DuckDBSource, SQLiteSource
from ycs.config import DEFAULT_START_DAY, DEFAULT_START_MONTH, DEFAULT_START_YEAR
from ycs.pipeline import run_create_corr_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create melted correlation pickle files from zero_rates and par_rates "
            "stored in the database."
        ),
    )
    parser.add_argument(
        "--starting-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"First year of the correlation sample (default: {DEFAULT_START_YEAR}).",
    )
    parser.add_argument(
        "--starting-month",
        type=int,
        default=DEFAULT_START_MONTH,
        help=f"First month of the correlation sample (default: {DEFAULT_START_MONTH}).",
    )
    parser.add_argument(
        "--starting-day",
        type=int,
        default=DEFAULT_START_DAY,
        help=f"First day of the correlation sample (default: {DEFAULT_START_DAY}).",
    )
    parser.add_argument(
        "--run-async",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Process tenors in parallel (default: True). Use --no-run-async to disable.",
    )
    parser.add_argument(
        "--backend",
        choices=("sqlite", "duckdb"),
        default="duckdb",
        help="Database backend to read rate tables from (default: duckdb).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    source_class = SQLiteSource if args.backend == "sqlite" else DuckDBSource
    run_create_corr_files(
        source_class,
        starting_year=args.starting_year,
        starting_month=args.starting_month,
        starting_day=args.starting_day,
        run_async=args.run_async,
    )
