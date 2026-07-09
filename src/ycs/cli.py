"""CLI argument parsing for backend-specific populate scripts."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Literal

BackendName = Literal["sqlite", "duckdb"]

_BACKEND_LABELS = {
    "sqlite": "SQLite",
    "duckdb": "DuckDB",
}


def backend_label(backend: BackendName) -> str:
    return _BACKEND_LABELS[backend]


def parse_args(
    backends: BackendName | Sequence[BackendName] = "sqlite",
) -> argparse.Namespace:
    """Parse populate script arguments for one or more backends."""
    if isinstance(backends, str):
        backend_list: list[BackendName] = [backends]
    else:
        backend_list = list(backends)

    labels = [_BACKEND_LABELS[backend] for backend in backend_list]
    if len(labels) == 1:
        description = (
            f"Load studio data into {labels[0]} and build or ingest correlation tables."
        )
    else:
        description = (
            "Load studio data into "
            + " and ".join(labels)
            + " and build or ingest correlation tables."
        )

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--load-from-files",
        action="store_true",
        default=False,
        help="Populate from parquet directories (zero rates, par, spot FX).",
    )
    parser.add_argument(
        "--create-corr-files",
        action="store_true",
        default=False,
        help="Compute correlation matrices and write melted pickle files to DATA_DIR.",
    )
    for backend in backend_list:
        label = _BACKEND_LABELS[backend]
        populate_flag = f"populate_{backend}_corr_from_files"
        parser.add_argument(
            f"--{populate_flag.replace('_', '-')}",
            action="store_true",
            default=False,
            dest=populate_flag,
            help=f"Load melted correlation pickles and write the window_corr {label} table.",
        )
    return parser.parse_args()
