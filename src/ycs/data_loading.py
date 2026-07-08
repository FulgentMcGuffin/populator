"""YCS-specific paths and transforms for the generic :mod:`ingestion` pipeline."""

from __future__ import annotations

import os
import re
from typing import Any

import polars as pl

from ingestion.pipeline import ingestion_overrides

__all__ = [
    "YCS_PARQUET_EXTENSIONS",
    "rename_tenor_columns",
    "ycs_ingestion_overrides",
    "ycs_parquet_transform",
    "ycs_table_directories",
]

YCS_PARQUET_EXTENSIONS = frozenset({".parquet"})


def rename_tenor_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize numeric tenor column names to ``Y001p0``-style labels."""
    return df.rename(
        lambda col: (
            f"Y{float(col):05.1f}".replace(".", "p")
            if re.match(r"^\d+(\.\d+)?$", str(col))
            else col
        )
    )


def ycs_parquet_transform(path: str, df: pl.DataFrame) -> pl.DataFrame:
    source = os.path.basename(path).rsplit(".", 1)[0]
    return rename_tenor_columns(df.with_columns(pl.lit(source).alias("source")))


def ycs_table_directories() -> dict[str, str]:
    """Return YCS parquet directory paths keyed by target table name."""
    localdata_path = os.getenv("LOCALDATA_PATH")
    if localdata_path is None:
        raise ValueError("LOCALDATA_PATH environment variable is not set")

    return {
        "zero_rates": f"{localdata_path}/zero_coupon",
        "par_rates": f"{localdata_path}/par",
        "spotfx": f"{localdata_path}/spot_fx_rates",
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
    )
