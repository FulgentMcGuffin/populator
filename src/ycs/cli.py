"""CLI argument parsing for backend-specific populate scripts."""

from __future__ import annotations

import argparse
from typing import Literal

BackendName = Literal["sqlite", "duckdb"]

_BACKEND_LABELS = {
    "sqlite": "SQLite",
    "duckdb": "DuckDB",
}


def backend_label(backend: BackendName) -> str:
    return _BACKEND_LABELS[backend]


def parse_args(backend: BackendName) -> argparse.Namespace:
    """Parse populate script arguments for the given backend."""
    label = _BACKEND_LABELS[backend]
    populate_flag = f"populate_{backend}_corr_from_files"

    parser = argparse.ArgumentParser(
        description=(
            f"Load studio data into {label} and build or ingest correlation tables."
        ),
    )
    parser.add_argument(
        "--load-from-files",
        action="store_true",
        default=False,
        help=f"Populate {label} from parquet directories (zero rates, par, spot FX).",
    )
    parser.add_argument(
        "--create-corr-files",
        action="store_true",
        default=False,
        help="Compute correlation matrices and write melted pickle files to DATA_DIR.",
    )
    parser.add_argument(
        f"--{populate_flag.replace('_', '-')}",
        action="store_true",
        default=False,
        dest=populate_flag,
        help=f"Load melted correlation pickles and write the window_corr {label} table.",
    )
    return parser.parse_args()
