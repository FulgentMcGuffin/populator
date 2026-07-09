"""Populate World Bank indicator CSV files into DuckDB and SQLite."""

from __future__ import annotations

import sys
from pathlib import Path

from tqdm import tqdm

SRC_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from backends import DuckDBSource, SQLiteSource
from ingestion import load_files_into_tables

WORLD_BANK_DATA_DIR = Path(r"D:\data\other\kaggle\world_bank_indicators")
DUCKDB_PATH = Path(r"D:\data\duckdb\world_bank.duckdb")
SQLITE_PATH = Path(r"D:\data\sqlite\world_bank.sqlite")

TABLE_FILES = {
    "topic_mapping": WORLD_BANK_DATA_DIR / "indicator_topic_mapping.csv",
    "indicators": WORLD_BANK_DATA_DIR / "world_bank_indicators_long.csv",
}

DATABASE_TARGETS = (
    (DuckDBSource, DUCKDB_PATH),
    (SQLiteSource, SQLITE_PATH),
)


def populate_world_bank(
    source_class: type,
    db_path: Path,
    *,
    table_files: dict[str, Path] = TABLE_FILES,
    progress: bool = True,
) -> dict[str, bool]:
    """Load World Bank CSV files into *db_path*."""
    backend_name = getattr(source_class, "name", source_class.__name__)
    if progress:
        tqdm.write(f"Populating World Bank -> {db_path} ({backend_name})")
        for table_name, file_path in table_files.items():
            tqdm.write(f"  {table_name!r} <- {file_path}")

    return load_files_into_tables(
        source_class,
        {table_name: str(file_path) for table_name, file_path in table_files.items()},
        db_path=str(db_path),
        progress=progress,
    )


if __name__ == "__main__":
    for backend_class, database_path in tqdm(
        DATABASE_TARGETS,
        desc="Database targets",
        unit="db",
    ):
        results = populate_world_bank(backend_class, database_path)
        tqdm.write(f"{database_path}: {results}")
