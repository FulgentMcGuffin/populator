"""Tests for YCS swap-par and repo/RFR ingestion."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from backends import SQLiteSource
from ingestion import load_directories_into_tables
from ycs.config import SWAP_TERM_TENORS
from ycs.data_loading import rename_tenor_columns, ycs_parquet_transform


def _quote_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": ["2024-01-01"],
            "0.25": [0.11],
            "0.5": [0.22],
            "1.0": [0.33],
        }
    )


def test_rename_tenor_columns_keeps_halves_and_quarters() -> None:
    renamed = rename_tenor_columns(_quote_frame())

    assert "Y000p25" in renamed.columns
    assert "Y000p5" in renamed.columns
    assert "Y001p0" in renamed.columns


def test_swap_par_transform_sets_currency_coupons_and_index() -> None:
    result = ycs_parquet_transform("USD_L.parquet", _quote_frame())

    assert result.columns[:5] == [
        "date",
        "ccy",
        "coupon_period",
        "coupon_period_fixed",
        "index",
    ]
    assert result.columns[5:] == SWAP_TERM_TENORS
    assert result["ccy"].to_list() == ["USD"]
    assert result["coupon_period"].to_list() == ["3M"]
    assert result["coupon_period_fixed"].to_list() == ["6M"]
    assert result["index"].to_list() == ["UNSPECIFIED"]
    assert result["Y000p25"].to_list() == [0.11]
    assert result["Y000p5"].to_list() == [0.22]
    assert result["Y006p0"].to_list() == [None]


def test_repo_rfr_transform_uses_floating_coupon_only() -> None:
    result = ycs_parquet_transform("EUR_R.parquet", _quote_frame())

    assert "coupon_period_fixed" not in result.columns
    assert result["ccy"].to_list() == ["EUR"]
    assert result["coupon_period"].to_list() == ["6M"]
    assert result["index"].to_list() == ["UNSPECIFIED"]


def test_term_structure_transform_rejects_unknown_currency() -> None:
    with pytest.raises(ValueError, match="Unknown currency"):
        ycs_parquet_transform("XXX_L.parquet", _quote_frame())


def test_plain_ycs_file_still_uses_source_column() -> None:
    result = ycs_parquet_transform("ITA.parquet", _quote_frame())

    assert result["source"].to_list() == ["ITA"]
    assert "ccy" not in result.columns


def test_directory_load_splits_swap_and_repo_by_stem_suffix(tmp_path: Path) -> None:
    frame = _quote_frame()
    frame.write_parquet(tmp_path / "USD_L.parquet")
    frame.write_parquet(tmp_path / "EUR_R.parquet")
    db_path = tmp_path / "ycs.db"

    results = load_directories_into_tables(
        SQLiteSource,
        {
            "swap_par_rates": tmp_path,
            "repo_rfr_rates": tmp_path,
        },
        db_path=db_path,
        extensions=frozenset({".parquet"}),
        transform=ycs_parquet_transform,
        stem_suffixes={
            "swap_par_rates": "_L",
            "repo_rfr_rates": "_R",
        },
    )

    with SQLiteSource(db_path, read_only=True) as db:
        swap_rows = db.execute("SELECT ccy, coupon_period_fixed FROM swap_par_rates")
        repo_rows = db.execute("SELECT ccy, coupon_period FROM repo_rfr_rates")

    assert results == {"swap_par_rates": True, "repo_rfr_rates": True}
    assert swap_rows == [{"ccy": "USD", "coupon_period_fixed": "6M"}]
    assert repo_rows == [{"ccy": "EUR", "coupon_period": "6M"}]
