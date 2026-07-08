"""Upload local file directories into SQLite or DuckDB tables."""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import polars as pl

from .files import SUPPORTED_EXTENSIONS, load_files_from_dir
from .transforms import FileTransform, IngestionTransform

__all__ = [
    "load_directories_into_tables",
    "load_directory_into_table",
    "source_from_filename_transform",
]


def source_from_filename_transform(path: str, df: pl.DataFrame) -> pl.DataFrame:
    """Add a ``source`` column from the file stem (no extension)."""
    source = Path(path).stem
    return df.with_columns(pl.lit(source).alias("source"))


def load_directory_into_table(
    db: Any,
    table_name: str,
    directory: str | Path,
    *,
    extensions: frozenset[str] | set[str] | None = None,
    transforms: Sequence[IngestionTransform] | None = None,
    transform: FileTransform | None = None,
    overwrite_if_exists: bool = True,
    progress: bool = False,
    csv_infer_schema_length: int | None = None,
) -> bool:
    """Load all matching files from *directory* into a single *table_name*."""
    df = load_files_from_dir(
        directory,
        extensions=extensions,
        transforms=transforms,
        transform=transform,
        progress=progress,
        csv_infer_schema_length=csv_infer_schema_length,
    )
    if progress:
        from tqdm import tqdm

        tqdm.write(
            f"Writing {df.height:,} rows x {df.width} columns to table {table_name!r}"
        )
    return db.create_table_from_polars(table_name, df, overwrite_if_exists)


def load_directories_into_tables(
    source_class: type[Any],
    table_directories: dict[str, str | Path],
    *,
    db_path: str | Path | None = None,
    extensions: frozenset[str] | set[str] | None = None,
    transforms: Sequence[IngestionTransform] | None = None,
    transform: FileTransform | None = None,
    overwrite_if_exists: bool = True,
    skip_missing: bool = True,
    progress: bool = False,
    csv_infer_schema_length: int | None = None,
) -> dict[str, bool]:
    """Load each directory into its named table using a writable backend."""
    results: dict[str, bool] = {}
    if progress:
        from tqdm import tqdm
    else:
        tqdm = None  # type: ignore[assignment]

    with source_class(db_path, read_only=False) as db:
        for table_name, directory in table_directories.items():
            directory = str(directory)
            if not os.path.exists(directory):
                if skip_missing:
                    message = (
                        f"Skipping {table_name}: directory {directory} does not exist"
                    )
                    if progress:
                        tqdm.write(message)
                    else:
                        print(message)
                    results[table_name] = False
                    continue
                raise FileNotFoundError(
                    f"Directory for table {table_name} does not exist: {directory}"
                )

            if progress:
                tqdm.write(f"Loading table {table_name!r} from {directory}")

            results[table_name] = load_directory_into_table(
                db,
                table_name,
                directory,
                extensions=extensions,
                transforms=transforms,
                transform=transform,
                overwrite_if_exists=overwrite_if_exists,
                progress=progress,
                csv_infer_schema_length=csv_infer_schema_length,
            )
            if progress:
                status = "created" if results[table_name] else "unchanged"
                tqdm.write(f"Finished {table_name!r} ({status})")

    return results
