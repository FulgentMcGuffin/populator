"""Imperative workflow steps used by the Hamilton DAG and legacy callers."""

from __future__ import annotations

import os
import pickle
from typing import Any

import polars as pl

from backends.base import QueryError

from .cli import backend_label
from .config import (
    DEFAULT_CORRELATION_WINDOW_SIZES,
    DEFAULT_TENORS,
    WINDOW_CORR_DEDUP_COLUMNS,
    WINDOW_CORR_TABLE,
)
from .correlations import get_corr_matrix

__all__ = [
    "build_window_corr_frames",
    "corr_dir_path",
    "empty_rate_tables",
    "load_rate_tables",
    "save_window_corr",
]


def load_rate_tables(
    source_class: type[Any],
    db_path: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Read zero rates, par rates, and spot FX from the database."""
    required_tables = ("zero_rates", "par_rates", "spotfx")
    with source_class(db_path, read_only=False) as db:
        existing = set(db.list_tables())
        missing = [name for name in required_tables if name not in existing]
        if missing:
            raise QueryError(
                f"Missing rate tables: {', '.join(missing)}. "
                "Run with --load-from-files first or populate the database."
            )

        zero_rates = pl.DataFrame(db.execute("SELECT * FROM zero_rates")).with_columns(
            pl.col("date").str.to_date()
        )
        par_rates = pl.DataFrame(db.execute("SELECT * FROM par_rates")).with_columns(
            pl.col("date").str.to_date()
        )
        spotfx_rates = pl.DataFrame(db.execute("SELECT * FROM spotfx")).with_columns(
            pl.col("date").str.to_date()
        )
    return zero_rates, par_rates, spotfx_rates


def empty_rate_tables() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return empty frames when correlation steps are not requested."""
    empty = pl.DataFrame()
    return empty, empty, empty


def corr_dir_path() -> str:
    return (
        f"{os.getenv('DERIVED_LOCALDATA_PATH')}/{os.getenv('DERIVED_CORR_FOLDER')}"
    )


def _melted_corr_path(
    corr_dir: str,
    prefix: str,
    input_df_name: str,
    start_year: int,
    start_month: int,
    start_day: int,
    max_date_str: str,
    window_size: int,
) -> str:
    return (
        f"{corr_dir}/{prefix}_{input_df_name}_"
        f"{start_year:04d}{start_month:02d}{start_day:02d}_"
        f"{max_date_str}_W{window_size}.pkl"
    )


def _load_or_create_corr_matrices(
    *,
    melted_file: str,
    populate_db_corr_from_files: bool,
    create_corr_files: bool,
    input_df: pl.DataFrame,
    correlation_window_size: int,
    tenors: list[str],
    use_dcor: bool,
    run_async: bool,
    overwrite_existing: bool = False,
) -> dict | None:
    if os.path.exists(melted_file) and not overwrite_existing:
        print(f"Loading existing {melted_file}")
        if populate_db_corr_from_files:
            with open(melted_file, "rb") as handle:
                return pickle.load(handle)
        return None

    if not create_corr_files:
        return None

    print(f"Creating {melted_file}")
    corr_matrices = get_corr_matrix(
        input_df,
        window=correlation_window_size,
        tenors=tenors,
        use_dcor=use_dcor,
        is_polars=True,
        run_async=run_async,
    )
    with open(melted_file, "wb") as handle:
        pickle.dump(corr_matrices, handle)
    return corr_matrices


def _append_corr_frames(
    df_pl_all: list[pl.DataFrame],
    corr_matrices: dict | None,
    correlation_window_size: int,
    corr_type: str,
) -> None:
    if corr_matrices is None:
        return

    dfs_pl = [
        df_pl.with_columns(
            pl.lit(term).alias("observable"),
            pl.col("Var1").alias("source1"),
            pl.col("Var2").alias("source2"),
            pl.lit(correlation_window_size).alias("window_size"),
            pl.lit(corr_type).alias("corr_type"),
        ).drop(["Var1", "Var2"])
        for term, df_pl in corr_matrices.items()
    ]
    if dfs_pl:
        df_pl_all.append(pl.concat(dfs_pl))


def build_window_corr_frames(
    *,
    rate_frames: list[tuple[pl.DataFrame, str]],
    corr_dir: str,
    populate_db_corr_from_files: bool,
    create_corr_files: bool,
    tenors: list[str],
    correlation_window_sizes: list[int],
    start_year: int,
    start_month: int,
    start_day: int,
    run_async: bool,
    overwrite_existing: bool = False,
) -> list[pl.DataFrame]:
    """Compute or load correlation pickles and assemble rows for window_corr."""
    df_pl_all: list[pl.DataFrame] = []

    for input_df, input_df_name in rate_frames:
        filtered_df = input_df.filter(
            pl.col("date") >= pl.date(start_year, start_month, start_day)
        )
        max_date_str = filtered_df["date"].max().strftime("%Y%m%d")

        for correlation_window_size in correlation_window_sizes:
            pearson_file = _melted_corr_path(
                corr_dir,
                "corr_dfs_melted",
                input_df_name,
                start_year,
                start_month,
                start_day,
                max_date_str,
                correlation_window_size,
            )
            pearson_matrices = _load_or_create_corr_matrices(
                melted_file=pearson_file,
                populate_db_corr_from_files=populate_db_corr_from_files,
                create_corr_files=create_corr_files,
                input_df=filtered_df,
                correlation_window_size=correlation_window_size,
                tenors=tenors,
                use_dcor=False,
                run_async=run_async,
                overwrite_existing=overwrite_existing,
            )
            _append_corr_frames(
                df_pl_all,
                pearson_matrices,
                correlation_window_size,
                "pearson",
            )

            dcorr_file = _melted_corr_path(
                corr_dir,
                "dcorr_dfs_melted",
                input_df_name,
                start_year,
                start_month,
                start_day,
                max_date_str,
                correlation_window_size,
            )
            dcorr_matrices = _load_or_create_corr_matrices(
                melted_file=dcorr_file,
                populate_db_corr_from_files=populate_db_corr_from_files,
                create_corr_files=create_corr_files,
                input_df=filtered_df,
                correlation_window_size=correlation_window_size,
                tenors=tenors,
                use_dcor=True,
                run_async=run_async,
                overwrite_existing=overwrite_existing,
            )
            _append_corr_frames(
                df_pl_all,
                dcorr_matrices,
                correlation_window_size,
                "dcorr",
            )

    return df_pl_all


def save_window_corr(
    source_class: type[Any],
    df_pl_all: pl.DataFrame,
    backend_name: str,
    db_path: str | None = None,
) -> None:
    """Append or create the window_corr table and remove duplicate rows."""
    df_pl_all = df_pl_all.rechunk()
    label = backend_label(backend_name)
    with source_class(db_path, read_only=False) as db:
        if WINDOW_CORR_TABLE in db.list_tables():
            db.append_to_table(WINDOW_CORR_TABLE, df_pl_all)
        else:
            db.create_table_from_polars(WINDOW_CORR_TABLE, df_pl_all, True)
        db.remove_duplicates(
            WINDOW_CORR_TABLE,
            duplicate_columns=WINDOW_CORR_DEDUP_COLUMNS,
            keep="last",
        )
    print(
        f"Saved {df_pl_all.shape[0]} rows to {label} in table {WINDOW_CORR_TABLE}"
    )
