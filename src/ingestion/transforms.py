"""Composable Polars transformations applied per file before database load."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import polars as pl

FileTransform = Callable[[str, pl.DataFrame], pl.DataFrame]

__all__ = [
    "FileSourceTransform",
    "IngestionTransform",
    "LitColumnTransform",
    "MapColumnTransform",
    "MeltTransform",
    "PrefixedMeltTransform",
    "apply_transforms",
    "build_file_transform",
    "compose_transforms",
]


class IngestionTransform(Protocol):
    """Transform applied to each loaded file before concatenation."""

    def apply(self, path: str, df: pl.DataFrame) -> pl.DataFrame:
        """Return a transformed DataFrame."""


@dataclass(frozen=True)
class MeltTransform:
    """Unpivot wide data: *index* columns stay fixed, all others become values."""

    index: tuple[str, ...] | list[str]
    variable_name: str = "variable"
    value_name: str = "value"

    def apply(self, path: str, df: pl.DataFrame) -> pl.DataFrame:
        index = list(self.index)
        missing = [column for column in index if column not in df.columns]
        if missing:
            raise ValueError(
                f"Melt index columns missing from {path}: {missing}"
            )

        value_columns = [column for column in df.columns if column not in index]
        if not value_columns:
            return df

        return df.unpivot(
            on=value_columns,
            index=index,
            variable_name=self.variable_name,
            value_name=self.value_name,
        )


def _is_index_column(
    column: str,
    *,
    exclude: tuple[str, ...],
    ignore: tuple[str, ...],
) -> bool:
    if column in exclude:
        return True
    return any(token in column for token in ignore)


def _metric_column_order(value_columns: list[str], separator: str) -> list[str]:
    metrics: list[str] = []
    seen: set[str] = set()
    for column in value_columns:
        parts = column.split(separator)
        if len(parts) < 2:
            continue
        metric = parts[-1]
        if metric not in seen:
            seen.add(metric)
            metrics.append(metric)
    return metrics


@dataclass(frozen=True)
class PrefixedMeltTransform:
    """Reshape ``{prefix}{separator}{metric}`` columns into one row per prefix.

    Columns listed in *exclude* (exact match) or whose names contain any
    *ignore* string are kept as row identifiers. Remaining columns are split on
    *separator*; all segments except the last form the shared prefix stored in
    *group_column*, and the last segment becomes a wide metric column name.

    Example: ``AC.PA.Open`` with separator ``.`` yields ``Stock=AC.PA`` and
    column ``Open``.
    """

    separator: str
    group_column: str
    exclude: tuple[str, ...] | list[str] = ()
    ignore: tuple[str, ...] | list[str] = ()

    def apply(self, path: str, df: pl.DataFrame) -> pl.DataFrame:
        exclude = tuple(self.exclude)
        ignore = tuple(self.ignore)

        index_columns = [
            column
            for column in df.columns
            if _is_index_column(column, exclude=exclude, ignore=ignore)
        ]
        value_columns = [
            column for column in df.columns if column not in index_columns
        ]
        if not value_columns:
            return df

        groups: dict[str, dict[str, str]] = {}
        for column in value_columns:
            parts = column.split(self.separator)
            if len(parts) < 2:
                raise ValueError(
                    f"Column {column!r} in {path} does not contain separator "
                    f"{self.separator!r} and cannot be melted"
                )
            metric = parts[-1]
            group_key = self.separator.join(parts[:-1])
            groups.setdefault(group_key, {})[metric] = column

        metric_columns = _metric_column_order(value_columns, self.separator)
        frames: list[pl.DataFrame] = []
        for group_key, metric_map in sorted(groups.items()):
            select_exprs = [pl.col(column) for column in index_columns]
            select_exprs.append(pl.lit(group_key).alias(self.group_column))
            for metric in metric_columns:
                source_column = metric_map.get(metric)
                if source_column is None:
                    select_exprs.append(pl.lit(None).alias(metric))
                else:
                    select_exprs.append(pl.col(source_column).alias(metric))
            frames.append(df.select(select_exprs))

        return pl.concat(frames)


@dataclass(frozen=True)
class LitColumnTransform:
    """Add a column with a fixed literal value."""

    column: str
    value: Any

    def apply(self, path: str, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(self.value).alias(self.column))


@dataclass(frozen=True)
class FileSourceTransform:
    """Add a ``file_source`` column with the file's absolute path."""

    column: str = "file_source"

    def apply(self, path: str, df: pl.DataFrame) -> pl.DataFrame:
        absolute_path = os.path.abspath(path)
        return LitColumnTransform(self.column, absolute_path).apply(path, df)


@dataclass(frozen=True)
class MapColumnTransform:
    """Map values from *source_column* into a new *target_column*."""

    source_column: str
    target_column: str
    mapper: Callable[[Any], Any]
    return_dtype: pl.DataType | None = None

    def apply(self, path: str, df: pl.DataFrame) -> pl.DataFrame:
        if self.source_column not in df.columns:
            raise ValueError(
                f"Map source column {self.source_column!r} missing from {path}"
            )

        expr = pl.col(self.source_column).map_elements(
            self.mapper,
            return_dtype=self.return_dtype,
        )
        return df.with_columns(expr.alias(self.target_column))


def apply_transforms(
    path: str,
    df: pl.DataFrame,
    transforms: Sequence[IngestionTransform],
) -> pl.DataFrame:
    """Apply each transform in order."""
    for transform in transforms:
        df = transform.apply(path, df)
    return df


def compose_transforms(
    transforms: Sequence[IngestionTransform] | None,
    *,
    file_transform: FileTransform | None = None,
) -> FileTransform | None:
    """Build a single per-file transform from composable steps."""
    steps = list(transforms or [])
    if not steps and file_transform is None:
        return None

    def composed(path: str, df: pl.DataFrame) -> pl.DataFrame:
        if steps:
            df = apply_transforms(path, df, steps)
        if file_transform is not None:
            df = file_transform(path, df)
        return df

    return composed


def build_file_transform(
    transforms: Sequence[IngestionTransform] | None = None,
    *,
    file_transform: FileTransform | None = None,
) -> FileTransform | None:
    """Alias for :func:`compose_transforms`."""
    return compose_transforms(transforms, file_transform=file_transform)
