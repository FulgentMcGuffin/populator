"""Tests for local file ingestion into database backends."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from backends import DuckDBSource, SQLiteSource
from ingestion import (
    load_directories_into_tables,
    load_directory_into_table,
    load_files_from_dir,
    read_local_file,
)
from ingestion.load import source_from_filename_transform


@pytest.fixture
def sample_frames() -> dict[str, pl.DataFrame]:
    return {
        "eur": pl.DataFrame({"date": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]}),
        "usd": pl.DataFrame({"date": ["2024-01-01", "2024-01-02"], "value": [3.0, 4.0]}),
    }


@pytest.fixture
def data_dir(tmp_path: Path, sample_frames: dict[str, pl.DataFrame]) -> Path:
    """Directory with one file per supported format."""
    sample_frames["eur"].write_parquet(tmp_path / "eur.parquet")
    sample_frames["usd"].write_csv(tmp_path / "usd.csv")
    pl.DataFrame({"date": ["2024-01-03"], "value": [5.0]}).write_ipc(
        tmp_path / "gbp.feather"
    )
    return tmp_path


def test_read_local_file_parquet_csv_feather(data_dir: Path) -> None:
    parquet_df = read_local_file(data_dir / "eur.parquet")
    csv_df = read_local_file(data_dir / "usd.csv")
    feather_df = read_local_file(data_dir / "gbp.feather")

    assert parquet_df.height == 2
    assert csv_df.height == 2
    assert feather_df.height == 1


def test_load_files_from_dir_concatenates_with_source_column(data_dir: Path) -> None:
    df = load_files_from_dir(
        data_dir,
        transform=source_from_filename_transform,
    )

    assert df.height == 5
    assert set(df["source"].to_list()) == {"eur", "usd", "gbp"}


@pytest.mark.parametrize("source_class", [SQLiteSource, DuckDBSource])
def test_create_table_from_polars_with_reserved_column_name(
    source_class: type,
) -> None:
    from datetime import date

    df = pl.DataFrame(
        {
            "Index": [date(2024, 1, 1), date(2024, 1, 2)],
            "Stock": ["AC.PA", "AI.PA"],
            "Volume": [400000.0, 1000.0],
        }
    )

    with source_class(":memory:", read_only=False) as db:
        created = db.create_table_from_polars("equity_eod", df, overwrite_if_exists=True)
        rows = db.execute('SELECT "Index", Stock, Volume FROM equity_eod ORDER BY Stock')

    assert created is True
    assert len(rows) == 2
    assert rows[0]["Stock"] == "AC.PA"
    assert rows[0]["Volume"] == 400000.0


@pytest.mark.parametrize("source_class", [SQLiteSource, DuckDBSource])
def test_load_directory_into_table_in_memory(
    source_class: type,
    data_dir: Path,
) -> None:
    with source_class(":memory:", read_only=False) as db:
        created = load_directory_into_table(
            db,
            "rates",
            data_dir,
            transform=source_from_filename_transform,
        )
        rows = db.execute("SELECT COUNT(*) AS n FROM rates")

    assert created is True
    assert rows[0]["n"] == 5


@pytest.mark.parametrize("source_class", [SQLiteSource, DuckDBSource])
def test_load_multiple_tables_in_memory_single_connection(
    source_class: type,
    tmp_path: Path,
    sample_frames: dict[str, pl.DataFrame],
) -> None:
    zero_dir = tmp_path / "zero_coupon"
    par_dir = tmp_path / "par"
    zero_dir.mkdir()
    par_dir.mkdir()
    sample_frames["eur"].write_parquet(zero_dir / "eur.parquet")
    sample_frames["usd"].write_parquet(par_dir / "usd.parquet")

    with source_class(":memory:", read_only=False) as db:
        load_directory_into_table(
            db,
            "zero_rates",
            zero_dir,
            extensions=frozenset({".parquet"}),
            transform=source_from_filename_transform,
        )
        load_directory_into_table(
            db,
            "par_rates",
            par_dir,
            extensions=frozenset({".parquet"}),
            transform=source_from_filename_transform,
        )
        zero_count = db.execute("SELECT COUNT(*) AS n FROM zero_rates")[0]["n"]
        par_count = db.execute("SELECT COUNT(*) AS n FROM par_rates")[0]["n"]

    assert zero_count == 2
    assert par_count == 2


@pytest.mark.parametrize("source_class", [SQLiteSource, DuckDBSource])
def test_hamilton_run_load_directories_into_tables_in_memory(
    source_class: type,
    tmp_path: Path,
    sample_frames: dict[str, pl.DataFrame],
) -> None:
    zero_dir = tmp_path / "zero_coupon"
    par_dir = tmp_path / "par"
    zero_dir.mkdir()
    par_dir.mkdir()
    sample_frames["eur"].write_parquet(zero_dir / "eur.parquet")
    sample_frames["usd"].write_parquet(par_dir / "usd.parquet")

    from ingestion import run_load_directories_into_tables

    results = run_load_directories_into_tables(
        source_class,
        {
            "zero_rates": str(zero_dir),
            "par_rates": str(par_dir),
        },
        db_path=":memory:",
        extensions=frozenset({".parquet"}),
        file_transform=source_from_filename_transform,
    )

    assert results == {"zero_rates": True, "par_rates": True}


@pytest.mark.parametrize("source_class", [SQLiteSource, DuckDBSource])
def test_hamilton_skips_load_when_disabled(
    source_class: type,
    tmp_path: Path,
) -> None:
    from ingestion import run_load_directories_into_tables

    results = run_load_directories_into_tables(
        source_class,
        {"rates": str(tmp_path)},
        should_load=False,
    )

    assert results == {}


def test_hamilton_build_driver_lists_ingestion_nodes() -> None:
    from ingestion import build_driver

    dr = build_driver()
    available = {node.name for node in dr.list_available_variables()}
    assert "directories_loaded" in available
    assert "load_summary" in available


@pytest.mark.parametrize(
    ("source_class", "db_suffix"),
    [
        (SQLiteSource, ".db"),
        (DuckDBSource, ".duckdb"),
    ],
)
def test_load_directories_into_tables(
    source_class: type,
    db_suffix: str,
    tmp_path: Path,
    sample_frames: dict[str, pl.DataFrame],
) -> None:
    zero_dir = tmp_path / "zero_coupon"
    par_dir = tmp_path / "par"
    zero_dir.mkdir()
    par_dir.mkdir()
    sample_frames["eur"].write_parquet(zero_dir / "eur.parquet")
    sample_frames["usd"].write_parquet(par_dir / "usd.parquet")

    db_path = tmp_path / f"test{db_suffix}"
    results = load_directories_into_tables(
        source_class,
        {
            "zero_rates": zero_dir,
            "par_rates": par_dir,
        },
        db_path=db_path,
        extensions=frozenset({".parquet"}),
        transform=source_from_filename_transform,
    )

    assert results == {"zero_rates": True, "par_rates": True}

    with source_class(db_path, read_only=True) as db:
        zero_count = db.execute("SELECT COUNT(*) AS n FROM zero_rates")[0]["n"]
        par_count = db.execute("SELECT COUNT(*) AS n FROM par_rates")[0]["n"]

    assert zero_count == 2
    assert par_count == 2
