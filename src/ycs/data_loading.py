"""YCS-specific paths and transforms for the generic :mod:`ingestion` pipeline."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import polars as pl

from ingestion.pipeline import ingestion_overrides
from ingestion.transforms import LitColumnTransform

from .config import (
    REPO_RFR_STEM_SUFFIX,
    REPO_RFR_TABLE,
    SWAP_COUPON_PERIODS,
    SWAP_PAR_STEM_SUFFIX,
    SWAP_PAR_TABLE,
    SWAP_TERM_TENORS,
    UNSPECIFIED_INDEX,
)

__all__ = [
    "YCS_PARQUET_EXTENSIONS",
    "rename_tenor_columns",
    "ycs_ingestion_overrides",
    "ycs_parquet_transform",
    "ycs_table_directories",
]

YCS_PARQUET_EXTENSIONS = frozenset({".parquet"})

_NUMERIC_COLUMN = re.compile(r"^\d+(\.\d+)?$")


def _tenor_label(column: str) -> str:
    """Map a numeric tenor name to ``Y000p5`` or ``Y000p25`` style."""
    if _NUMERIC_COLUMN.match(str(column)) is None:
        return column
    value = float(column)
    quarters = value * 4
    halves = value * 2
    is_quarter = abs(quarters - round(quarters)) < 1e-8
    is_half = abs(halves - round(halves)) < 1e-8
    if is_quarter and not is_half:
        return f"Y{value:06.2f}".replace(".", "p")
    return f"Y{value:05.1f}".replace(".", "p")


def rename_tenor_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize numeric tenor column names to ``Y001p0``-style labels."""
    return df.rename(_tenor_label)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} environment variable is not set")
    return value


def _currency_from_stem(stem: str, suffix: str, path: str) -> str:
    if not stem.endswith(suffix) or len(stem) <= len(suffix):
        raise ValueError(
            f"Filename stem {stem!r} in {path} does not end with suffix {suffix!r}"
        )
    currency = stem[: -len(suffix)]
    if currency not in SWAP_COUPON_PERIODS:
        known = ", ".join(sorted(SWAP_COUPON_PERIODS))
        raise ValueError(
            f"Unknown currency {currency!r} in {path}. Known: {known}"
        )
    return currency


def _select_term_structure(
    df: pl.DataFrame,
    path: str,
    id_columns: list[str],
) -> pl.DataFrame:
    missing = [column for column in id_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Term structure columns missing from {path}: {missing}")

    expressions: list[pl.Expr] = [pl.col(column) for column in id_columns]
    for tenor in SWAP_TERM_TENORS:
        if tenor in df.columns:
            expressions.append(pl.col(tenor))
        else:
            expressions.append(pl.lit(None).cast(pl.Float64).alias(tenor))
    return df.select(expressions)


def _term_structure_frame(
    path: str,
    df: pl.DataFrame,
    *,
    suffix: str,
    include_fixed: bool,
) -> pl.DataFrame:
    currency = _currency_from_stem(Path(path).stem, suffix, path)
    floating, fixed = SWAP_COUPON_PERIODS[currency]
    df = LitColumnTransform("ccy", currency).apply(path, df)
    df = LitColumnTransform("coupon_period", floating).apply(path, df)
    id_columns = ["date", "ccy", "coupon_period"]
    if include_fixed:
        df = LitColumnTransform("coupon_period_fixed", fixed).apply(path, df)
        id_columns.append("coupon_period_fixed")
    df = LitColumnTransform("index", UNSPECIFIED_INDEX).apply(path, df)
    id_columns.append("index")
    return _select_term_structure(rename_tenor_columns(df), path, id_columns)


def ycs_parquet_transform(path: str, df: pl.DataFrame) -> pl.DataFrame:
    """Normalize YCS parquet, including swap-par and repo/RFR term structures."""
    stem = Path(path).stem
    if stem.endswith(SWAP_PAR_STEM_SUFFIX):
        return _term_structure_frame(
            path,
            df,
            suffix=SWAP_PAR_STEM_SUFFIX,
            include_fixed=True,
        )
    if stem.endswith(REPO_RFR_STEM_SUFFIX):
        return _term_structure_frame(
            path,
            df,
            suffix=REPO_RFR_STEM_SUFFIX,
            include_fixed=False,
        )

    source = os.path.basename(path).rsplit(".", 1)[0]
    return rename_tenor_columns(df.with_columns(pl.lit(source).alias("source")))


def ycs_table_directories() -> dict[str, str]:
    """Return YCS parquet directory paths keyed by target table name."""
    localdata_path = _require_env("LOCALDATA_PATH")
    return {
        "zero_rates": f"{localdata_path}/zero_coupon",
        "par_rates": f"{localdata_path}/par",
        "spotfx": f"{localdata_path}/spot_fx_rates",
        SWAP_PAR_TABLE: _require_env("LOCALDATA_SWAP_PAR_FOLDER"),
        REPO_RFR_TABLE: _require_env("LOCALDATA_REPO_RFR_FOLDER"),
    }


def ycs_ingestion_overrides(
    source_class: type[Any],
    *,
    should_load: bool,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Build ingestion Hamilton overrides with YCS paths and parquet transform."""
    return ingestion_overrides(
        source_class=source_class,
        should_load=should_load,
        table_directories=ycs_table_directories() if should_load else {},
        db_path=db_path,
        extensions=YCS_PARQUET_EXTENSIONS,
        file_transform=ycs_parquet_transform,
        stem_suffixes=(
            {
                SWAP_PAR_TABLE: SWAP_PAR_STEM_SUFFIX,
                REPO_RFR_TABLE: REPO_RFR_STEM_SUFFIX,
            }
            if should_load
            else {}
        ),
    )
