"""Apache Hamilton DAG nodes for local file ingestion into databases.

Each function is a node; parameters define upstream dependencies. Runtime
configuration is passed as graph overrides at ``Driver.execute()`` time.
"""

from __future__ import annotations

from typing import Any

from .files import FileTransform, SUPPORTED_EXTENSIONS
from .load import load_directories_into_tables

__all__ = [
    "directories_loaded",
    "load_summary",
]


def source_class() -> type[Any]:
    """Database backend class (SQLiteSource or DuckDBSource)."""


def should_load_directories() -> bool:
    """Whether to load local file directories into the database."""


def table_directories() -> dict[str, str]:
    """Mapping of table name to directory path."""


def db_path() -> str | None:
    """Optional database path (e.g. ``:memory:`` or a file path)."""


def extensions() -> frozenset[str] | None:
    """File extensions to load; ``None`` uses all supported types."""


def file_transform() -> FileTransform | None:
    """Optional per-file transform applied before concatenation."""


def overwrite_if_exists() -> bool:
    """Replace existing tables when loading."""


def skip_missing() -> bool:
    """Skip directories that do not exist instead of raising."""


def directories_loaded(
    source_class: type[Any],
    should_load_directories: bool,
    table_directories: dict[str, str],
    db_path: str | None,
    extensions: frozenset[str] | None,
    file_transform: FileTransform | None,
    overwrite_if_exists: bool,
    skip_missing: bool,
) -> dict[str, bool]:
    """Load each configured directory into its table when requested."""
    if not should_load_directories:
        return {}

    return load_directories_into_tables(
        source_class,
        table_directories,
        db_path=db_path,
        extensions=extensions,
        transform=file_transform,
        overwrite_if_exists=overwrite_if_exists,
        skip_missing=skip_missing,
    )


def load_summary(directories_loaded: dict[str, bool]) -> dict[str, bool]:
    """Terminal node for the ingestion-only workflow."""
    return directories_loaded
