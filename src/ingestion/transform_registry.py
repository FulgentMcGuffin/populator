"""Serializable transform definitions for CLI and MCP integrations."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

import polars as pl

from .transforms import (
    CastDateColumnTransform,
    CastNumericStringColumnsTransform,
    FileSourceTransform,
    FilenamePartTransform,
    IngestionTransform,
    LitColumnTransform,
    MeltTransform,
    PrefixedMeltTransform,
)

__all__ = [
    "TRANSFORM_CATALOG",
    "TRANSFORM_PRESETS",
    "build_transform",
    "build_transforms",
    "describe_transforms",
    "serialize_transforms",
]

_POLARS_DTYPES = {
    "float64": pl.Float64,
    "float32": pl.Float32,
    "int64": pl.Int64,
    "int32": pl.Int32,
}

TRANSFORM_CATALOG: list[dict[str, Any]] = [
    {
        "type": "cast_numeric_string_columns",
        "description": (
            "Cast string columns to numeric values (handles scientific notation "
            "like 4e+05). Use with csv_infer_schema_length=0."
        ),
        "parameters": {
            "exclude": {
                "type": "array",
                "items": "string",
                "default": [],
                "description": "Column names to leave as strings.",
            },
            "dtype": {
                "type": "string",
                "enum": list(_POLARS_DTYPES),
                "default": "float64",
                "description": "Target numeric dtype for string columns.",
            },
        },
    },
    {
        "type": "prefixed_melt",
        "description": (
            "Reshape wide columns like 'AC.PA.Open' into rows with a group "
            "column and metric columns (Open, High, ...)."
        ),
        "parameters": {
            "separator": {
                "type": "string",
                "required": True,
                "description": "Delimiter between prefix and metric name.",
            },
            "group_column": {
                "type": "string",
                "required": True,
                "description": "Output column holding the shared prefix.",
            },
            "exclude": {
                "type": "array",
                "items": "string",
                "default": [],
                "description": "Exact column names kept as row identifiers.",
            },
            "ignore": {
                "type": "array",
                "items": "string",
                "default": [],
                "description": "Substring matches kept as row identifiers.",
            },
        },
    },
    {
        "type": "melt",
        "description": "Unpivot wide data: index columns stay fixed, others become variable/value.",
        "parameters": {
            "index": {
                "type": "array",
                "items": "string",
                "required": True,
                "description": "Identifier columns to keep fixed.",
            },
            "variable_name": {
                "type": "string",
                "default": "variable",
            },
            "value_name": {
                "type": "string",
                "default": "value",
            },
        },
    },
    {
        "type": "filename_part",
        "description": "Add a column from a token of the file stem split on a separator.",
        "parameters": {
            "column": {"type": "string", "required": True},
            "separator": {"type": "string", "required": True},
            "part_index": {"type": "integer", "default": 0},
        },
    },
    {
        "type": "file_source",
        "description": "Add a column with the absolute path of the source file.",
        "parameters": {
            "column": {"type": "string", "default": "file_source"},
        },
    },
    {
        "type": "lit_column",
        "description": "Add a column with a fixed literal value.",
        "parameters": {
            "column": {"type": "string", "required": True},
            "value": {
                "type": "string",
                "required": True,
                "description": "Literal value (stored as string).",
            },
        },
    },
    {
        "type": "cast_date_column",
        "description": (
            "Cast a column to Polars Date. Optional format uses tokens like "
            "YYYY-mm-dd HH:MM:SS."
        ),
        "parameters": {
            "column": {"type": "string", "required": True},
            "format": {
                "type": "string",
                "default": None,
                "description": "Optional date/datetime format string.",
            },
        },
    },
]

TRANSFORM_PRESETS: dict[str, list[dict[str, Any]]] = {
    "none": [],
    "equity": [
        {
            "type": "cast_numeric_string_columns",
            "exclude": ["Index"],
        },
        {
            "type": "prefixed_melt",
            "separator": ".",
            "group_column": "Stock",
            "exclude": ["Index"],
        },
        {
            "type": "filename_part",
            "column": "EqIndex",
            "separator": "_",
            "part_index": 0,
        },
        {"type": "file_source"},
        {
            "type": "cast_date_column",
            "column": "Index",
            "format": "YYYY-mm-dd HH:MM:SS",
        },
    ],
}


def _require_fields(step: dict[str, Any], required: list[str], transform_type: str) -> None:
    missing = [field for field in required if field not in step]
    if missing:
        raise ValueError(
            f"Transform {transform_type!r} missing required fields: {missing}"
        )


def build_transform(step: dict[str, Any]) -> IngestionTransform:
    """Build one ingestion transform from a JSON-like step definition."""
    transform_type = step.get("type")
    if not transform_type:
        raise ValueError("Transform step must include a 'type' field")

    if transform_type == "cast_numeric_string_columns":
        dtype_name = step.get("dtype", "float64")
        dtype = _POLARS_DTYPES.get(dtype_name)
        if dtype is None:
            supported = ", ".join(sorted(_POLARS_DTYPES))
            raise ValueError(
                f"Unsupported dtype {dtype_name!r} for cast_numeric_string_columns. "
                f"Supported: {supported}"
            )
        return CastNumericStringColumnsTransform(
            exclude=step.get("exclude", []),
            dtype=dtype,
        )

    if transform_type == "prefixed_melt":
        _require_fields(step, ["separator", "group_column"], transform_type)
        return PrefixedMeltTransform(
            separator=step["separator"],
            group_column=step["group_column"],
            exclude=step.get("exclude", []),
            ignore=step.get("ignore", []),
        )

    if transform_type == "melt":
        _require_fields(step, ["index"], transform_type)
        return MeltTransform(
            index=step["index"],
            variable_name=step.get("variable_name", "variable"),
            value_name=step.get("value_name", "value"),
        )

    if transform_type == "filename_part":
        _require_fields(step, ["column", "separator"], transform_type)
        return FilenamePartTransform(
            column=step["column"],
            separator=step["separator"],
            part_index=step.get("part_index", 0),
        )

    if transform_type == "file_source":
        return FileSourceTransform(column=step.get("column", "file_source"))

    if transform_type == "lit_column":
        _require_fields(step, ["column", "value"], transform_type)
        return LitColumnTransform(column=step["column"], value=step["value"])

    if transform_type == "cast_date_column":
        _require_fields(step, ["column"], transform_type)
        return CastDateColumnTransform(
            column=step["column"],
            format=step.get("format"),
        )

    supported = ", ".join(entry["type"] for entry in TRANSFORM_CATALOG)
    raise ValueError(f"Unknown transform type {transform_type!r}. Supported: {supported}")


def build_transforms(
    steps: list[dict[str, Any]] | None = None,
    *,
    preset: str | None = None,
) -> list[IngestionTransform]:
    """Build transforms from explicit steps and/or a named preset."""
    if steps is not None and preset is not None:
        raise ValueError("Pass either 'steps' or 'preset', not both")

    if preset is not None:
        if preset not in TRANSFORM_PRESETS:
            supported = ", ".join(sorted(TRANSFORM_PRESETS))
            raise ValueError(f"Unknown preset {preset!r}. Supported: {supported}")
        steps = TRANSFORM_PRESETS[preset]
    elif steps is None:
        steps = []

    return [build_transform(step) for step in steps]


def serialize_transforms(transforms: list[IngestionTransform]) -> list[dict[str, Any]]:
    """Serialize known transform instances back to step dictionaries."""
    serialized: list[dict[str, Any]] = []
    for transform in transforms:
        if isinstance(transform, CastNumericStringColumnsTransform):
            dtype_name = next(
                (name for name, dtype in _POLARS_DTYPES.items() if dtype == transform.dtype),
                "float64",
            )
            step: dict[str, Any] = {
                "type": "cast_numeric_string_columns",
                "exclude": list(transform.exclude),
            }
            if dtype_name != "float64":
                step["dtype"] = dtype_name
            serialized.append(step)
        elif isinstance(transform, PrefixedMeltTransform):
            step = {
                "type": "prefixed_melt",
                "separator": transform.separator,
                "group_column": transform.group_column,
            }
            if transform.exclude:
                step["exclude"] = list(transform.exclude)
            if transform.ignore:
                step["ignore"] = list(transform.ignore)
            serialized.append(step)
        elif isinstance(transform, MeltTransform):
            serialized.append(
                {
                    "type": "melt",
                    "index": list(transform.index),
                    "variable_name": transform.variable_name,
                    "value_name": transform.value_name,
                }
            )
        elif isinstance(transform, FilenamePartTransform):
            serialized.append(
                {
                    "type": "filename_part",
                    "column": transform.column,
                    "separator": transform.separator,
                    "part_index": transform.part_index,
                }
            )
        elif isinstance(transform, FileSourceTransform):
            if transform.column != "file_source":
                serialized.append(
                    {"type": "file_source", "column": transform.column}
                )
            else:
                serialized.append({"type": "file_source"})
        elif isinstance(transform, LitColumnTransform):
            serialized.append(
                {
                    "type": "lit_column",
                    "column": transform.column,
                    "value": transform.value,
                }
            )
        elif isinstance(transform, CastDateColumnTransform):
            serialized.append(
                {
                    "type": "cast_date_column",
                    "column": transform.column,
                    "format": transform.format,
                }
            )
        elif is_dataclass(transform):
            serialized.append({"type": type(transform).__name__, **asdict(transform)})
        else:
            serialized.append({"type": type(transform).__name__})
    return serialized


def describe_transforms(
    steps: list[dict[str, Any]] | None = None,
    *,
    preset: str | None = None,
) -> list[dict[str, Any]]:
    """Return the step definitions that would be applied."""
    if preset is not None:
        if preset not in TRANSFORM_PRESETS:
            supported = ", ".join(sorted(TRANSFORM_PRESETS))
            raise ValueError(f"Unknown preset {preset!r}. Supported: {supported}")
        return list(TRANSFORM_PRESETS[preset])
    return list(steps or [])
