"""MCP server for populator ingestion (files, transforms, SQLite/DuckDB load)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from pydantic import Field

from backends import DuckDBSource, SQLiteSource
from ingestion import (
    apply_transforms,
    load_directories_into_tables,
    load_files_into_tables,
    read_local_file,
)
from ingestion.transform_registry import (
    TRANSFORM_CATALOG,
    TRANSFORM_PRESETS,
    build_transforms,
    describe_transforms,
)

BackendName = Literal["sqlite", "duckdb"]

BACKENDS = {
    "sqlite": SQLiteSource,
    "duckdb": DuckDBSource,
}

mcp = FastMCP(
    name="populator",
    instructions=(
        "Load local CSV, parquet, and feather files into SQLite or DuckDB. "
        "Use list_transforms to discover transform types and presets. "
        "Preview with preview_file before populate_from_files or "
        "populate_from_directories. MapColumnTransform is not exposed via MCP."
    ),
)


def _resolve_transforms(
    transforms: list[dict[str, Any]] | None,
    preset: str | None,
) -> list:
    if transforms and preset:
        raise ValueError("Pass either 'transforms' or 'preset', not both")
    return build_transforms(transforms, preset=preset)


def _frame_preview(df, rows: int) -> dict[str, Any]:
    sample = df.head(rows)
    return {
        "row_count": df.height,
        "column_count": df.width,
        "columns": sample.columns,
        "dtypes": {column: str(dtype) for column, dtype in df.schema.items()},
        "sample_rows": sample.to_dicts(),
    }


@mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": False})
def list_transforms() -> dict[str, Any]:
    """List available transform types, parameters, and named presets."""
    return {
        "transforms": TRANSFORM_CATALOG,
        "presets": {
            name: steps for name, steps in TRANSFORM_PRESETS.items()
        },
        "notes": [
            "Pass 'transforms' as an ordered list of step objects to populate/preview tools.",
            "Alternatively pass 'preset' (e.g. 'equity') instead of 'transforms'.",
            "Use csv_infer_schema_length=0 with cast_numeric_string_columns for CSV scientific notation.",
            "MapColumnTransform is not available via MCP because it requires a Python callable.",
        ],
    }


@mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": False})
def preview_file(
    path: Annotated[str, Field(description="Absolute path to a local data file")],
    rows: Annotated[int, Field(ge=1, le=100, description="Sample row count")] = 5,
    transforms: Annotated[
        list[dict[str, Any]] | None,
        Field(description="Ordered transform steps (see list_transforms)"),
    ] = None,
    preset: Annotated[
        str | None,
        Field(description="Named transform preset instead of transforms"),
    ] = None,
    csv_infer_schema_length: Annotated[
        int | None,
        Field(
            description=(
                "CSV-only: set to 0 to read all columns as strings before transforms"
            )
        ),
    ] = None,
) -> dict[str, Any]:
    """Preview a file optionally applying transforms before load."""
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = read_local_file(file_path, csv_infer_schema_length=csv_infer_schema_length)
    before = _frame_preview(df, rows)

    transform_steps = describe_transforms(transforms, preset=preset)
    if transform_steps:
        built = build_transforms(transform_steps)
        df = apply_transforms(str(file_path), df, built)

    return {
        "path": str(file_path),
        "transform_steps": transform_steps,
        "before": before,
        "after": _frame_preview(df, rows),
    }


@mcp.tool(annotations={"destructiveHint": True, "openWorldHint": False})
def populate_from_files(
    backend: Annotated[BackendName, Field(description="Target database engine")],
    db_path: Annotated[str, Field(description="Absolute path to .sqlite/.db or .duckdb")],
    table_files: Annotated[
        dict[str, str],
        Field(description="Map of table_name -> absolute file path"),
    ],
    overwrite: Annotated[bool, Field(description="Replace existing tables")] = True,
    transforms: Annotated[
        list[dict[str, Any]] | None,
        Field(description="Ordered transform steps applied to every file"),
    ] = None,
    preset: Annotated[
        str | None,
        Field(description="Named transform preset instead of transforms"),
    ] = None,
    csv_infer_schema_length: Annotated[
        int | None,
        Field(description="CSV-only infer_schema_length (0 reads all columns as strings)"),
    ] = None,
) -> dict[str, Any]:
    """Load specific files into named tables."""
    source_class = BACKENDS[backend]
    built_transforms = _resolve_transforms(transforms, preset)
    results = load_files_into_tables(
        source_class,
        table_files,
        db_path=db_path,
        transforms=built_transforms or None,
        overwrite_if_exists=overwrite,
        progress=False,
        csv_infer_schema_length=csv_infer_schema_length,
    )
    return {
        "backend": backend,
        "db_path": db_path,
        "transform_steps": describe_transforms(transforms, preset=preset),
        "results": results,
    }


@mcp.tool(annotations={"destructiveHint": True, "openWorldHint": False})
def populate_from_directories(
    backend: Annotated[BackendName, Field(description="Target database engine")],
    db_path: Annotated[str, Field(description="Absolute path to .sqlite/.db or .duckdb")],
    table_directories: Annotated[
        dict[str, str],
        Field(description="Map of table_name -> directory path"),
    ],
    extensions: Annotated[
        list[str] | None,
        Field(description="File extensions to load, e.g. ['.csv', '.parquet']"),
    ] = None,
    overwrite: Annotated[bool, Field(description="Replace existing tables")] = True,
    transforms: Annotated[
        list[dict[str, Any]] | None,
        Field(description="Ordered transform steps applied to every file"),
    ] = None,
    preset: Annotated[
        str | None,
        Field(description="Named transform preset instead of transforms"),
    ] = None,
    csv_infer_schema_length: Annotated[
        int | None,
        Field(description="CSV-only infer_schema_length (0 reads all columns as strings)"),
    ] = None,
) -> dict[str, Any]:
    """Load all matching files from each directory into named tables."""
    source_class = BACKENDS[backend]
    built_transforms = _resolve_transforms(transforms, preset)
    extension_set = frozenset(extensions) if extensions else None
    results = load_directories_into_tables(
        source_class,
        table_directories,
        db_path=db_path,
        extensions=extension_set,
        transforms=built_transforms or None,
        overwrite_if_exists=overwrite,
        progress=False,
        csv_infer_schema_length=csv_infer_schema_length,
    )
    return {
        "backend": backend,
        "db_path": db_path,
        "extensions": sorted(extension_set) if extension_set else None,
        "transform_steps": describe_transforms(transforms, preset=preset),
        "results": results,
    }


@mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": False})
def list_db_tables(
    backend: Annotated[BackendName, Field(description="Database engine")],
    db_path: Annotated[str, Field(description="Absolute path to the database file")],
) -> dict[str, Any]:
    """List tables in a SQLite or DuckDB database."""
    source_class = BACKENDS[backend]
    with source_class(db_path, read_only=True) as db:
        tables = db.list_tables()
    return {"backend": backend, "db_path": db_path, "tables": tables}


@mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": False})
def describe_db_table(
    backend: Annotated[BackendName, Field(description="Database engine")],
    db_path: Annotated[str, Field(description="Absolute path to the database file")],
    table: Annotated[str, Field(description="Table name")],
) -> dict[str, Any]:
    """Describe columns and types for one table."""
    source_class = BACKENDS[backend]
    with source_class(db_path, read_only=True) as db:
        schema = db.get_schema(table)
    return {
        "backend": backend,
        "db_path": db_path,
        "table": table,
        "columns": [column.to_dict() for column in schema.columns],
    }


@mcp.resource("populator://transforms")
def transforms_resource() -> str:
    """JSON catalog of transform types and presets."""
    return json.dumps(list_transforms(), indent=2)


def main() -> None:
    """Run the MCP server (stdio by default; HTTP when PORT is set)."""
    import os

    port = os.environ.get("PORT")
    if port:
        mcp.run(transport="streamable-http", host="0.0.0.0", port=int(port))
    else:
        mcp.run()


if __name__ == "__main__":
    main()
