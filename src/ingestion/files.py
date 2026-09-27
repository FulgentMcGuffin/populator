"""Read parquet, CSV, and feather files from local directories."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Sequence
from pathlib import Path

import polars as pl

from .transforms import FileTransform, IngestionTransform, compose_transforms

SUPPORTED_EXTENSIONS = frozenset({".parquet", ".csv", ".feather", ".ipc"})

_READERS: dict[str, Callable[[str], pl.DataFrame]] = {
    ".parquet": pl.read_parquet,
    ".csv": pl.read_csv,
    ".feather": pl.read_ipc,
    ".ipc": pl.read_ipc,
}


def read_local_file(
    path: str | Path,
    *,
    csv_infer_schema_length: int | None = None,
) -> pl.DataFrame:
    """Read a single parquet, CSV, or feather file into a Polars DataFrame."""
    suffix = Path(path).suffix.lower()
    if suffix == ".csv" and csv_infer_schema_length is not None:
        return pl.read_csv(str(path), infer_schema_length=csv_infer_schema_length)
    reader = _READERS.get(suffix)
    if reader is None:
        supported = ", ".join(sorted(SUPPORTED_EXTENSIONS))
        raise ValueError(
            f"Unsupported file type {suffix!r} for {path}. Supported: {supported}"
        )
    return reader(str(path))


def _list_data_files(
    directory: str | Path,
    extensions: frozenset[str] | set[str] | None = None,
    *,
    stem_suffix: str | None = None,
) -> list[str]:
    allowed = frozenset(extensions) if extensions is not None else SUPPORTED_EXTENSIONS
    return sorted(
        os.path.join(directory, name)
        for name in os.listdir(directory)
        if Path(name).suffix.lower() in allowed
        and (stem_suffix is None or Path(name).stem.endswith(stem_suffix))
    )


def _drop_all_null_columns(frames: list[pl.DataFrame]) -> list[pl.DataFrame]:
    """Drop columns that are null in every row, and align the rest for concat.

    A column is removed when no frame contains a non-null value. A column that
    is entirely null in some frames and populated in others is kept, with the
    empty frames cast to the populated dtype so concatenation does not mix
    ``Null`` with a concrete type.
    """
    if not frames:
        return frames

    all_null: list[set[str]] = []
    populated: dict[str, pl.DataType] = {}
    for df in frames:
        if df.height == 0:
            all_null.append(set(df.columns))
            continue
        flags = df.select(pl.all().is_not_null().any()).row(0)
        null_columns: set[str] = set()
        for name, has_value in zip(df.columns, flags):
            if has_value:
                populated.setdefault(name, df.schema[name])
            else:
                null_columns.add(name)
        all_null.append(null_columns)

    drop = {
        name
        for null_columns in all_null
        for name in null_columns
        if name not in populated
    }
    needs_cast = any(
        name in populated and df.schema[name] != populated[name]
        for df, null_columns in zip(frames, all_null)
        for name in null_columns
    )
    if not drop and not needs_cast:
        return frames

    aligned: list[pl.DataFrame] = []
    for df, null_columns in zip(frames, all_null):
        expressions: list[pl.Expr] = []
        for name in df.columns:
            if name in drop:
                continue
            if name in null_columns and df.schema[name] != populated[name]:
                expressions.append(pl.col(name).cast(populated[name]))
            else:
                expressions.append(pl.col(name))
        aligned.append(df.select(expressions))
    return aligned


async def _load_all_files(
    paths: list[str],
    transform: FileTransform | None,
    *,
    progress: bool = False,
    csv_infer_schema_length: int | None = None,
) -> list[pl.DataFrame]:
    async def _read_one(path: str) -> pl.DataFrame:
        def _read() -> pl.DataFrame:
            df = read_local_file(
                path,
                csv_infer_schema_length=csv_infer_schema_length,
            )
            if transform is not None:
                df = transform(path, df)
            return df

        return await asyncio.to_thread(_read)

    if not progress:
        frames = list(await asyncio.gather(*[_read_one(path) for path in paths]))
    else:
        from tqdm import tqdm

        loaded: list[pl.DataFrame | None] = [None] * len(paths)
        bar = tqdm(total=len(paths), desc="Reading files", unit="file")

        async def _read_indexed(index: int, path: str) -> None:
            loaded[index] = await _read_one(path)
            bar.set_postfix(file=Path(path).name, refresh=False)
            bar.update(1)

        try:
            await asyncio.gather(
                *[_read_indexed(index, path) for index, path in enumerate(paths)]
            )
        finally:
            bar.close()

        frames = [frame for frame in loaded if frame is not None]

    return _drop_all_null_columns(frames)


def load_files_from_dir(
    directory: str | Path,
    *,
    extensions: frozenset[str] | set[str] | None = None,
    transforms: Sequence[IngestionTransform] | None = None,
    transform: FileTransform | None = None,
    progress: bool = False,
    csv_infer_schema_length: int | None = None,
    stem_suffix: str | None = None,
) -> pl.DataFrame:
    """Read all matching files in *directory* concurrently and concatenate them."""
    paths = _list_data_files(directory, extensions, stem_suffix=stem_suffix)
    if not paths:
        supported = ", ".join(sorted(extensions or SUPPORTED_EXTENSIONS))
        suffix_note = f" with stem suffix {stem_suffix!r}" if stem_suffix else ""
        raise FileNotFoundError(
            f"No supported files ({supported}){suffix_note} found in {directory}"
        )
    file_transform = compose_transforms(transforms, file_transform=transform)
    if progress:
        from tqdm import tqdm

        tqdm.write(
            f"Found {len(paths)} file(s) in {directory}; applying transforms and concatenating"
        )
    frames = asyncio.run(
        _load_all_files(
            paths,
            file_transform,
            progress=progress,
            csv_infer_schema_length=csv_infer_schema_length,
        )
    )
    return pl.concat(frames)
