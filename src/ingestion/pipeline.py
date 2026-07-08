"""Hamilton-driven orchestration for local file ingestion."""

from __future__ import annotations

from typing import Any

from hamilton import driver

from . import hamilton_nodes
from .files import FileTransform, SUPPORTED_EXTENSIONS

__all__ = [
    "build_driver",
    "ingestion_overrides",
    "run_load_directories_into_tables",
]


def build_driver(*extra_modules: Any) -> driver.Driver:
    """Construct a Hamilton driver for the ingestion DAG."""
    return (
        driver.Builder()
        .with_modules(hamilton_nodes, *extra_modules)
        .build()
    )


def ingestion_overrides(
    *,
    source_class: type[Any],
    should_load: bool,
    table_directories: dict[str, str] | None = None,
    db_path: str | None = None,
    extensions: frozenset[str] | set[str] | None = None,
    file_transform: FileTransform | None = None,
    overwrite_if_exists: bool = True,
    skip_missing: bool = True,
) -> dict[str, Any]:
    """Build Hamilton override inputs for the ingestion DAG nodes."""
    return {
        "source_class": source_class,
        "should_load_directories": should_load,
        "table_directories": table_directories or {},
        "db_path": db_path,
        "extensions": (
            frozenset(extensions) if extensions is not None else SUPPORTED_EXTENSIONS
        ),
        "file_transform": file_transform,
        "overwrite_if_exists": overwrite_if_exists,
        "skip_missing": skip_missing,
    }


def run_load_directories_into_tables(
    source_class: type[Any],
    table_directories: dict[str, str],
    *,
    db_path: str | None = None,
    extensions: frozenset[str] | set[str] | None = None,
    file_transform: FileTransform | None = None,
    overwrite_if_exists: bool = True,
    skip_missing: bool = True,
    should_load: bool = True,
) -> dict[str, bool]:
    """Run the ingestion Hamilton DAG to load directories into database tables."""
    dr = build_driver()
    result = dr.execute(
        ["load_summary"],
        overrides=ingestion_overrides(
            source_class=source_class,
            should_load=should_load,
            table_directories=table_directories,
            db_path=db_path,
            extensions=extensions,
            file_transform=file_transform,
            overwrite_if_exists=overwrite_if_exists,
            skip_missing=skip_missing,
        ),
    )
    return result["load_summary"]
