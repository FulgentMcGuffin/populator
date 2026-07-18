"""Apache Hamilton DAG nodes for the yield-curve populate pipeline.

Each function is a node; parameters define upstream dependencies. Runtime
configuration and CLI flags are passed as graph inputs via the Driver.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .config import (
    DEFAULT_CORRELATION_WINDOW_SIZES,
    DEFAULT_TENORS,
)
from .workflow import (
    build_window_corr_frames,
    corr_dir_path,
    empty_rate_tables,
    load_rate_tables,
    save_window_corr,
)


def backend() -> str:
    """Backend name: ``sqlite`` or ``duckdb``."""


def create_corr_files() -> bool:
    """Whether to compute and write correlation pickle files."""


def populate_db_corr_from_files() -> bool:
    """Whether to load correlation pickles into the window_corr table."""


def starting_year() -> int:
    """First year of the correlation sample."""


def starting_month() -> int:
    """First month of the correlation sample."""


def starting_day() -> int:
    """First day of the correlation sample."""


def run_async() -> bool:
    """Whether tenor correlation jobs run in parallel."""


def overwrite_existing() -> bool:
    """Whether to overwrite existing correlation pickle files."""


def rate_tables(
    source_class: type[Any],
    db_path: str | None,
    directories_loaded: dict[str, bool],
    create_corr_files: bool,
    populate_db_corr_from_files: bool,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Read rate tables after optional directory load completes."""
    if not create_corr_files and not populate_db_corr_from_files:
        return empty_rate_tables()

    if directories_loaded:
        failed = [
            table_name
            for table_name, loaded in directories_loaded.items()
            if not loaded
        ]
        if failed:
            raise ValueError(
                f"Directory load did not create tables: {', '.join(failed)}"
            )

    return load_rate_tables(source_class, db_path)


def zero_rates(rate_tables: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]) -> pl.DataFrame:
    return rate_tables[0]


def par_rates(rate_tables: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]) -> pl.DataFrame:
    return rate_tables[1]


def spotfx_rates(
    rate_tables: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame],
) -> pl.DataFrame:
    return rate_tables[2]


def tenors() -> list[str]:
    return DEFAULT_TENORS


def correlation_window_sizes() -> list[int]:
    return DEFAULT_CORRELATION_WINDOW_SIZES


def corr_dir() -> str:
    return corr_dir_path()


def window_corr_frame_list(
    zero_rates: pl.DataFrame,
    par_rates: pl.DataFrame,
    corr_dir: str,
    create_corr_files: bool,
    populate_db_corr_from_files: bool,
    tenors: list[str],
    correlation_window_sizes: list[int],
    starting_year: int,
    starting_month: int,
    starting_day: int,
    run_async: bool,
    overwrite_existing: bool,
) -> list[pl.DataFrame]:
    """Compute or load correlation matrices and assemble window_corr rows."""
    if not create_corr_files and not populate_db_corr_from_files:
        return []

    return build_window_corr_frames(
        rate_frames=[(zero_rates, "zero_rates"), (par_rates, "par_rates")],
        corr_dir=corr_dir,
        populate_db_corr_from_files=populate_db_corr_from_files,
        create_corr_files=create_corr_files,
        tenors=tenors,
        correlation_window_sizes=correlation_window_sizes,
        start_year=starting_year,
        start_month=starting_month,
        start_day=starting_day,
        run_async=run_async,
        overwrite_existing=overwrite_existing,
    )


def window_corr_dataframe(
    window_corr_frame_list: list[pl.DataFrame],
) -> pl.DataFrame | None:
    if not window_corr_frame_list:
        return None
    return pl.concat(window_corr_frame_list)


def saved_window_corr(
    source_class: type[Any],
    db_path: str | None,
    backend: str,
    populate_db_corr_from_files: bool,
    window_corr_dataframe: pl.DataFrame | None,
) -> None:
    """Persist assembled correlation rows to the database when requested."""
    if populate_db_corr_from_files and window_corr_dataframe is not None:
        save_window_corr(source_class, window_corr_dataframe, backend, db_path)


def corr_files_created(
    zero_rates: pl.DataFrame,
    par_rates: pl.DataFrame,
    corr_dir: str,
    tenors: list[str],
    correlation_window_sizes: list[int],
    starting_year: int,
    starting_month: int,
    starting_day: int,
    run_async: bool,
    overwrite_existing: bool,
) -> None:
    """Terminal node for the create-corr-files-only workflow."""
    build_window_corr_frames(
        rate_frames=[(zero_rates, "zero_rates"), (par_rates, "par_rates")],
        corr_dir=corr_dir,
        populate_db_corr_from_files=False,
        create_corr_files=True,
        tenors=tenors,
        correlation_window_sizes=correlation_window_sizes,
        start_year=starting_year,
        start_month=starting_month,
        start_day=starting_day,
        run_async=run_async,
        overwrite_existing=overwrite_existing,
    )


def pipeline_summary(
    zero_rates: pl.DataFrame,
    saved_window_corr: None,
    directories_loaded: dict[str, bool],
    create_corr_files: bool,
    populate_db_corr_from_files: bool,
) -> dict[str, int | dict[str, bool]]:
    """Terminal node for the full populate workflow."""
    if create_corr_files or populate_db_corr_from_files:
        print(zero_rates.shape)
        print(zero_rates[-10:])
    return {
        "zero_rates_rows": zero_rates.height,
        "directories_loaded": directories_loaded,
    }
