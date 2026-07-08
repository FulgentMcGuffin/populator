"""Populate equity end-of-day CSV files into DuckDB and SQLite."""

from __future__ import annotations

import sys
from pathlib import Path

from tqdm import tqdm

SRC_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from backends import DuckDBSource, SQLiteSource
from ingestion import (
    CastDateColumnTransform,
    CastNumericStringColumnsTransform,
    FileSourceTransform,
    FilenamePartTransform,
    PrefixedMeltTransform,
    load_directories_into_tables,
)

EQUITY_CSV_DIR = Path(r"D:\data\equity\eod")
TABLE_NAME = "equity_eod"
DUCKDB_PATH = Path(r"D:\data\duckdb\equity_eod_data.duckdb")
SQLITE_PATH = Path(r"D:\data\sqlite\equity_eod_data.sqlite")

CSV_EXTENSIONS = frozenset({".csv"})
MELT_SEPARATOR = "."
FILENAME_SEPARATOR = "_"
STOCK_COLUMN = "Stock"
INDEX_COLUMN = "Index"
EQ_INDEX_COLUMN = "EqIndex"
DATE_FORMAT = "YYYY-mm-dd HH:MM:SS"

EQUITY_TRANSFORMS = [
    CastNumericStringColumnsTransform(exclude=[INDEX_COLUMN]),
    PrefixedMeltTransform(
        separator=MELT_SEPARATOR,
        group_column=STOCK_COLUMN,
        exclude=[INDEX_COLUMN],
    ),
    FilenamePartTransform(
        column=EQ_INDEX_COLUMN,
        separator=FILENAME_SEPARATOR,
        part_index=0,
    ),
    FileSourceTransform(),
    CastDateColumnTransform(column=INDEX_COLUMN, format=DATE_FORMAT),
]

DATABASE_TARGETS = (
    (DuckDBSource, DUCKDB_PATH),
    (SQLiteSource, SQLITE_PATH),
)


def populate_equity_eod(
    source_class: type,
    db_path: Path,
    *,
    csv_dir: Path = EQUITY_CSV_DIR,
    table_name: str = TABLE_NAME,
    transforms: list = EQUITY_TRANSFORMS,
    progress: bool = True,
) -> dict[str, bool]:
    """Load equity CSV files from *csv_dir* into *table_name* at *db_path*."""
    backend_name = getattr(source_class, "name", source_class.__name__)
    if progress:
        tqdm.write(
            f"Populating {table_name!r} -> {db_path} ({backend_name}) from {csv_dir}"
        )
        tqdm.write(
            f"Transforms: parse numeric strings, melt (sep={MELT_SEPARATOR!r}), "
            f"filename part -> {EQ_INDEX_COLUMN!r}, file_source, cast {INDEX_COLUMN!r} to date"
        )

    return load_directories_into_tables(
        source_class,
        {table_name: str(csv_dir)},
        db_path=str(db_path),
        extensions=CSV_EXTENSIONS,
        transforms=transforms,
        progress=progress,
        csv_infer_schema_length=0,
    )


if __name__ == "__main__":
    for backend_class, database_path in tqdm(
        DATABASE_TARGETS,
        desc="Database targets",
        unit="db",
    ):
        results = populate_equity_eod(backend_class, database_path)
        tqdm.write(f"{database_path}: {results}")
