"""Tests for the YCS Hamilton populate pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from backends import SQLiteSource
from ycs.pipeline import run_populate_pipeline


def test_run_populate_pipeline_without_flags_skips_rate_tables(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ycs_data.sqlite"
    args = argparse.Namespace(
        load_from_files=False,
        create_corr_files=False,
        populate_sqlite_corr_from_files=False,
        populate_duckdb_corr_from_files=False,
    )

    run_populate_pipeline(
        SQLiteSource,
        "sqlite",
        args,
        db_path=str(db_path),
    )

    assert not db_path.exists()


def test_run_populate_pipeline_corr_flags_require_rate_tables(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ycs_data.sqlite"
    args = argparse.Namespace(
        load_from_files=False,
        create_corr_files=True,
        populate_sqlite_corr_from_files=False,
        populate_duckdb_corr_from_files=False,
    )

    with pytest.raises(Exception, match="Missing rate tables"):
        run_populate_pipeline(
            SQLiteSource,
            "sqlite",
            args,
            db_path=str(db_path),
        )
