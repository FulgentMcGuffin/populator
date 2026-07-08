"""Tests for ingestion transforms."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from backends import DuckDBSource, SQLiteSource
from ingestion import (
    CastDateColumnTransform,
    CastNumericStringColumnsTransform,
    FileSourceTransform,
    FilenamePartTransform,
    LitColumnTransform,
    MapColumnTransform,
    MeltTransform,
    PrefixedMeltTransform,
    apply_transforms,
    load_directory_into_table,
    load_files_from_dir,
)


@pytest.fixture
def wide_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "source": ["eur", "eur"],
            "1.0": [1.1, 1.2],
            "2.0": [2.1, 2.2],
        }
    )


@pytest.fixture
def equity_wide_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Date": ["2024-01-01", "2024-01-02"],
            "Index": [100.0, 101.0],
            "Index.2": [200.0, 201.0],
            "AC.PA.Open": [10.0, 10.5],
            "AC.PA.High": [11.0, 11.5],
            "AC.PA.Low": [9.0, 9.5],
            "AC.PA.Close": [10.5, 11.0],
            "AC.PA.Volume": [1000, 1100],
            "AI.PA.Open": [20.0, 20.5],
            "AI.PA.High": [21.0, 21.5],
            "AI.PA.Low": [19.0, 19.5],
            "AI.PA.Close": [20.5, 21.0],
            "AI.PA.Volume": [2000, 2100],
            "AIR.PA.Open": [30.0, 30.5],
            "AIR.PA.High": [31.0, 31.5],
            "AIR.PA.Low": [29.0, 29.5],
            "AIR.PA.Close": [30.5, 31.0],
            "AIR.PA.Volume": [3000, 3100],
        }
    )


def test_cast_numeric_string_columns_transform_parses_scientific_notation() -> None:
    df = pl.DataFrame(
        {
            "Index": ["2024-01-01", "2024-01-02"],
            "LR.PA.Volume": ["4e+05", "1.2e+06"],
            "LR.PA.Close": ["10.5", "11.0"],
        }
    )

    result = CastNumericStringColumnsTransform(exclude=["Index"]).apply(
        "equity.csv",
        df,
    )

    assert result.schema["Index"] == pl.String
    assert result["LR.PA.Volume"].to_list() == [400000.0, 1200000.0]
    assert result["LR.PA.Close"].to_list() == [10.5, 11.0]


def test_load_csv_with_scientific_notation_via_infer_schema_length_zero(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "CAC40_daily.csv"
    csv_path.write_text(
        "Index,AC.PA.Volume\n"
        "2024-01-01,4e+05\n"
        "2024-01-02,1000\n",
        encoding="utf-8",
    )

    result = load_files_from_dir(
        tmp_path,
        extensions=frozenset({".csv"}),
        csv_infer_schema_length=0,
        transforms=[
            CastNumericStringColumnsTransform(exclude=["Index"]),
            CastDateColumnTransform(column="Index", format="YYYY-mm-dd HH:MM:SS"),
        ],
    )

    assert result["AC.PA.Volume"].to_list() == [400000.0, 1000.0]


def test_prefixed_melt_transform_equity_columns(equity_wide_frame: pl.DataFrame) -> None:
    result = PrefixedMeltTransform(
        separator=".",
        group_column="Stock",
        exclude=["Date"],
        ignore=["Index"],
    ).apply("equity.csv", equity_wide_frame)

    assert result.columns == [
        "Date",
        "Index",
        "Index.2",
        "Stock",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ]
    assert result.height == 6
    assert set(result["Stock"].to_list()) == {"AC.PA", "AI.PA", "AIR.PA"}

    ac = result.filter(pl.col("Stock") == "AC.PA").sort("Date")
    assert ac["Open"].to_list() == [10.0, 10.5]
    assert ac["Volume"].to_list() == [1000, 1100]
    assert ac["Date"].to_list() == ["2024-01-01", "2024-01-02"]
    assert ac["Index"].to_list() == [100.0, 101.0]
    assert ac["Index.2"].to_list() == [200.0, 201.0]


def test_filename_part_transform(tmp_path: Path, wide_frame: pl.DataFrame) -> None:
    file_path = tmp_path / "CAC40_daily.csv"
    file_path.touch()

    result = FilenamePartTransform(
        column="EqIndex",
        separator="_",
        part_index=0,
    ).apply(str(file_path), wide_frame)

    assert result["EqIndex"].to_list() == ["CAC40", "CAC40"]


def test_cast_date_column_transform() -> None:
    from datetime import date

    df = pl.DataFrame({"Index": ["2024-01-01", "2024-01-02"]})

    result = CastDateColumnTransform(column="Index").apply("equity.csv", df)

    assert result.schema["Index"] == pl.Date
    assert result["Index"].to_list() == [date(2024, 1, 1), date(2024, 1, 2)]


def test_cast_date_column_transform_with_datetime_format() -> None:
    from datetime import date

    df = pl.DataFrame({"Index": ["2024-01-01 16:00:00", "2024-01-02 00:00:00"]})

    result = CastDateColumnTransform(
        column="Index",
        format="YYYY-mm-dd HH:MM:SS",
    ).apply("equity.csv", df)

    assert result.schema["Index"] == pl.Date
    assert result["Index"].to_list() == [date(2024, 1, 1), date(2024, 1, 2)]


def test_melt_transform(wide_frame: pl.DataFrame) -> None:
    melted = MeltTransform(index=["date", "source"]).apply("rates.parquet", wide_frame)

    assert melted.columns == ["date", "source", "variable", "value"]
    assert melted.height == 4
    assert set(melted["variable"].to_list()) == {"1.0", "2.0"}


def test_lit_column_transform(wide_frame: pl.DataFrame) -> None:
    result = LitColumnTransform("region", "EMEA").apply("rates.parquet", wide_frame)

    assert result["region"].to_list() == ["EMEA", "EMEA"]


def test_file_source_transform_uses_absolute_path(tmp_path: Path, wide_frame: pl.DataFrame) -> None:
    file_path = tmp_path / "nested" / "rates.parquet"
    file_path.parent.mkdir()
    file_path.write_bytes(b"")

    result = FileSourceTransform().apply(str(file_path), wide_frame)

    assert result["file_source"].to_list() == [str(file_path.resolve())] * 2


def test_map_column_transform_with_lambda(wide_frame: pl.DataFrame) -> None:
    result = MapColumnTransform(
        source_column="source",
        target_column="source_upper",
        mapper=lambda value: str(value).upper(),
        return_dtype=pl.Utf8,
    ).apply("rates.parquet", wide_frame)

    assert result["source_upper"].to_list() == ["EUR", "EUR"]


def test_apply_transforms_runs_in_order(wide_frame: pl.DataFrame) -> None:
    path = "/tmp/rates.parquet"
    result = apply_transforms(
        path,
        wide_frame,
        [
            LitColumnTransform("kind", "zero"),
            MapColumnTransform(
                "kind",
                "kind_label",
                mapper=lambda value: f"{value}_rate",
                return_dtype=pl.Utf8,
            ),
        ],
    )

    assert result["kind"].to_list() == ["zero", "zero"]
    assert result["kind_label"].to_list() == ["zero_rate", "zero_rate"]


def test_compose_transforms_not_applied_when_unspecified(
    tmp_path: Path,
    wide_frame: pl.DataFrame,
) -> None:
    wide_frame.write_parquet(tmp_path / "eur.parquet")

    without = load_files_from_dir(tmp_path, extensions=frozenset({".parquet"}))
    with_steps = load_files_from_dir(
        tmp_path,
        extensions=frozenset({".parquet"}),
        transforms=[LitColumnTransform("dataset", "ycs")],
    )

    assert "dataset" not in without.columns
    assert with_steps["dataset"].to_list() == ["ycs", "ycs"]


@pytest.mark.parametrize("source_class", [SQLiteSource, DuckDBSource])
def test_load_directory_with_transform_pipeline(
    source_class: type,
    tmp_path: Path,
    wide_frame: pl.DataFrame,
) -> None:
    file_path = tmp_path / "eur.parquet"
    wide_frame.write_parquet(file_path)

    transforms = [
        MeltTransform(index=["date", "source"]),
        FileSourceTransform(),
        LitColumnTransform("dataset", "ycs"),
    ]

    with source_class(":memory:", read_only=False) as db:
        load_directory_into_table(
            db,
            "rates",
            tmp_path,
            extensions=frozenset({".parquet"}),
            transforms=transforms,
        )
        rows = db.execute("SELECT * FROM rates")

    assert len(rows) == 4
    assert rows[0]["dataset"] == "ycs"
    assert rows[0]["file_source"] == str(file_path.resolve())
