"""Shared populate pipeline orchestration."""

from __future__ import annotations

import os
import pickle
from typing import Any

import polars as pl

from .cli import backend_label
from .config import (
    DEFAULT_CORRELATION_WINDOW_SIZES,
    DEFAULT_START_DAY,
    DEFAULT_START_MONTH,
    DEFAULT_START_YEAR,
    DEFAULT_TENORS,
    WINDOW_CORR_DEDUP_COLUMNS,
    WINDOW_CORR_TABLE,
)
from .correlations import get_corr_matrix
from .data_loading import populate_from_files

__all__ = [
    "load_rate_tables",
    "run_create_corr_files",
    "run_populate_pipeline",
    "save_window_corr",
]


def load_rate_tables(source_class: type[Any]) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Read zero rates, par rates, and spot FX from the database."""
    with source_class(read_only=False) as db:
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
    backend_label: str,
) -> None:
    """Append or create the window_corr table and remove duplicate rows."""
    df_pl_all = df_pl_all.rechunk()
    with source_class(read_only=False) as db:
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
        f"Saved {df_pl_all.shape[0]} rows to {backend_label} in table {WINDOW_CORR_TABLE}"
    )


def run_create_corr_files(
    source_class: type[Any],
    *,
    starting_year: int = DEFAULT_START_YEAR,
    starting_month: int = DEFAULT_START_MONTH,
    starting_day: int = DEFAULT_START_DAY,
    run_async: bool = True,
    overwrite_existing: bool = True,
) -> None:
    """Compute melted correlation pickles from rate tables in the database."""
    zero_rates, par_rates, _ = load_rate_tables(source_class)
    corr_dir = (
        f"{os.getenv('DERIVED_LOCALDATA_PATH')}/{os.getenv('DERIVED_CORR_FOLDER')}"
    )
    build_window_corr_frames(
        rate_frames=[(zero_rates, "zero_rates"), (par_rates, "par_rates")],
        corr_dir=corr_dir,
        populate_db_corr_from_files=False,
        create_corr_files=True,
        tenors=DEFAULT_TENORS,
        correlation_window_sizes=DEFAULT_CORRELATION_WINDOW_SIZES,
        start_year=starting_year,
        start_month=starting_month,
        start_day=starting_day,
        run_async=run_async,
        overwrite_existing=overwrite_existing,
    )


def run_populate_pipeline(
    source_class: type[Any],
    backend: str,
    args,
) -> None:
    """Run the full populate workflow for a database backend."""
    backend_label_name = backend_label(backend)
    populate_db_corr_from_files = getattr(
        args, f"populate_{backend}_corr_from_files", False
    )

    if args.load_from_files:
        populate_from_files(source_class)

    zero_rates, par_rates, spotfx_rates = load_rate_tables(source_class)

    corr_dir = (
        f"{os.getenv('DERIVED_LOCALDATA_PATH')}/{os.getenv('DERIVED_CORR_FOLDER')}"
    )
    df_pl_all = build_window_corr_frames(
        rate_frames=[(zero_rates, "zero_rates"), (par_rates, "par_rates")],
        corr_dir=corr_dir,
        populate_db_corr_from_files=populate_db_corr_from_files,
        create_corr_files=args.create_corr_files,
        tenors=DEFAULT_TENORS,
        correlation_window_sizes=DEFAULT_CORRELATION_WINDOW_SIZES,
        start_year=DEFAULT_START_YEAR,
        start_month=DEFAULT_START_MONTH,
        start_day=DEFAULT_START_DAY,
        run_async=True,
    )

    if df_pl_all:
        save_window_corr(source_class, pl.concat(df_pl_all), backend_label_name)

    print(zero_rates.shape)
    print(zero_rates[-10:])
