"""Parquet ingestion helpers shared by SQLite and DuckDB pipelines."""

from __future__ import annotations

import asyncio
import os
import re
from typing import Any

import polars as pl

__all__ = ["load_parquets_from_dir", "populate_from_files"]


def load_parquets_from_dir(directory: str) -> pl.DataFrame:
    """Concurrently read all parquet files in *directory* and return them concatenated."""
    paths = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".parquet")
    ]

    async def _load_all() -> list[pl.DataFrame]:
        async def _read_parquet(path: str) -> pl.DataFrame:
            def _construct_polars(path: str) -> pl.DataFrame:
                source = os.path.basename(path).replace(".parquet", "")
                df = (
                    pl.read_parquet(path)
                    .with_columns(pl.lit(source).alias("source"))
                    .rename(
                        lambda col: (
                            f"Y{float(col):05.1f}".replace(".", "p")
                            if re.match(r"^\d+(\.\d+)?$", str(col))
                            else col
                        )
                    )
                )
                return df

            return await asyncio.to_thread(_construct_polars, path)

        return list(await asyncio.gather(*[_read_parquet(p) for p in paths]))

    return pl.concat(asyncio.run(_load_all()))


def populate_from_files(
    source_class: type[Any],
    db_path: str | None = None,
) -> None:
    """Load zero rates, par rates, and spot FX parquet directories into the database."""
    localdata_path = os.getenv("LOCALDATA_PATH")
    if localdata_path is None:
        raise ValueError("LOCALDATA_PATH environment variable is not set")

    table_dirs = {
        "zero_rates": f"{localdata_path}/zero_coupon",
        "par_rates": f"{localdata_path}/par",
        "spotfx": f"{localdata_path}/spot_fx_rates",
    }

    with source_class(db_path, read_only=False) as db:
        for table_name, directory in table_dirs.items():
            if os.path.exists(directory):
                db.create_table_from_polars(
                    table_name,
                    load_parquets_from_dir(directory),
                    True,
                )
            else:
                print(f"Skipping {table_name}: directory {directory} does not exist")
