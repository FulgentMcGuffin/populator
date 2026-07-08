"""Hamilton-driven populate pipeline orchestration."""

from __future__ import annotations

from typing import Any

from ingestion.pipeline import build_driver as build_ingestion_driver

from .config import DEFAULT_START_DAY, DEFAULT_START_MONTH, DEFAULT_START_YEAR
from . import hamilton_nodes
from .data_loading import ycs_ingestion_overrides

__all__ = [
    "build_driver",
    "load_rate_tables",
    "run_create_corr_files",
    "run_populate_pipeline",
    "save_window_corr",
]

# Re-export workflow helpers for backwards compatibility.
from .workflow import build_window_corr_frames, load_rate_tables, save_window_corr


def build_driver():
    """Construct a Hamilton driver for the yield-curve populate DAG."""
    return build_ingestion_driver(hamilton_nodes)


def run_create_corr_files(
    source_class: type[Any],
    *,
    db_path: str | None = None,
    starting_year: int = DEFAULT_START_YEAR,
    starting_month: int = DEFAULT_START_MONTH,
    starting_day: int = DEFAULT_START_DAY,
    run_async: bool = True,
    overwrite_existing: bool = True,
) -> None:
    """Compute melted correlation pickles from rate tables in the database."""
    dr = build_driver()
    dr.execute(
        ["corr_files_created"],
        overrides={
            **ycs_ingestion_overrides(
                source_class,
                should_load=False,
                db_path=db_path,
            ),
            "starting_year": starting_year,
            "starting_month": starting_month,
            "starting_day": starting_day,
            "run_async": run_async,
            "overwrite_existing": overwrite_existing,
        },
    )


def run_populate_pipeline(
    source_class: type[Any],
    backend: str,
    args,
    *,
    db_path: str | None = None,
) -> None:
    """Run the full populate workflow for a database backend."""
    populate_db_corr_from_files = getattr(
        args, f"populate_{backend}_corr_from_files", False
    )
    dr = build_driver()
    dr.execute(
        ["pipeline_summary"],
        overrides={
            **ycs_ingestion_overrides(
                source_class,
                should_load=args.load_from_files,
                db_path=db_path,
            ),
            "backend": backend,
            "create_corr_files": args.create_corr_files,
            "populate_db_corr_from_files": populate_db_corr_from_files,
            "starting_year": DEFAULT_START_YEAR,
            "starting_month": DEFAULT_START_MONTH,
            "starting_day": DEFAULT_START_DAY,
            "run_async": True,
            "overwrite_existing": False,
        },
    )
